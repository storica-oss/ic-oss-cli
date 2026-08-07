mod journal;
mod local;
mod persistence;
mod planner;
mod pull;
mod remote;
mod types;

use ic_oss::bucket::Client;
use ic_oss_types::{
    batch::{
        BatchCreateSmallFilesInput, BatchEnsureFoldersInput, MAX_BATCH_FILE_BYTES,
        MAX_BATCH_INLINE_BYTES, MAX_BATCH_ITEMS,
    },
    entry::{DeleteEntryIfInput, EnsureFolderInput, EntryKind as ApiEntryKind, SyncError},
    file::{CreateFileInput, CHUNK_SIZE},
    folder::{CreateFolderInput, CreateFolderOutput},
    sha3_256, to_cbor_bytes,
    upload::{
        BeginUploadInput, CommitUploadInput, GetUploadStatusInput, RenewUploadInput,
        ReplaceFileInput, UploadChunkInput, UploadStatusOutput,
    },
};
use ring::rand::{SecureRandom, SystemRandom};
use serde_bytes::ByteBuf;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::SeekFrom,
    path::PathBuf,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub use planner::{plan_sync, Plan, PlanAction, PlanOptions};
pub use pull::{execute_pull, prepare_pull, print_pull_plan, PullOptions, MAX_DOWNLOAD_RETRIES};

#[derive(Clone, Debug)]
pub struct SyncOptions {
    pub bucket: String,
    pub local_root: PathBuf,
    pub remote_parent: u32,
    pub delete: bool,
    pub overwrite: bool,
    pub excludes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct PreparedSync {
    pub plan: Plan,
    options: SyncOptions,
    local: types::LocalManifest,
    remote: types::RemoteManifest,
    enable_hash_index: bool,
    local_manifest_hash: [u8; 32],
    supports_ensure_folder: bool,
    supports_atomic_upload: bool,
    supports_incremental_delete: bool,
    supports_batch_operations: bool,
}

pub async fn prepare_sync(
    client: &Client,
    mut options: SyncOptions,
) -> Result<PreparedSync, String> {
    options.local_root = options.local_root.canonicalize().map_err(|err| {
        format!(
            "failed to resolve local root {:?}: {}",
            options.local_root, err
        )
    })?;
    let bucket = client.get_bucket_info().await?;
    let (capabilities, capability_error) = match client.get_capabilities().await {
        Ok(capabilities) => (Some(capabilities), None),
        Err(err) => (None, Some(err)),
    };
    let target = client.get_folder_info(options.remote_parent).await?;
    let parent_depth = if options.remote_parent == 0 {
        0
    } else {
        client
            .get_folder_ancestors(options.remote_parent)
            .await?
            .len()
            + 1
    };

    let local = local::scan_local(
        &options.local_root,
        &options.excludes,
        bucket.max_file_size,
        bucket.max_folder_depth as usize,
        parent_depth,
        bucket.max_children as usize,
    )?;
    println!(
        "local scan: {} cached hashes, {} files hashed",
        local.cache_hits, local.hashed_files
    );
    let supports_manifest = capabilities.as_ref().is_some_and(|value| {
        value.manifest && value.migration_state == ic_oss_types::entry::MigrationState::Ready
    });
    let remote = remote::scan_remote(
        client,
        options.remote_parent,
        target.status,
        supports_manifest,
    )
    .await?;
    println!(
        "remote scan: {} path",
        if supports_manifest {
            "revision-guarded subtree manifest"
        } else {
            "legacy recursive listing"
        }
    );

    let local_manifest_hash = manifest_hash(&local);
    let mut plan = plan_sync(
        &local,
        &remote,
        PlanOptions {
            delete: options.delete,
            overwrite: options.overwrite,
        },
    );
    if let Some(warning) = journal::recovery_warning(
        &options.bucket,
        options.remote_parent,
        &options.local_root,
        &local_manifest_hash,
    ) {
        plan.warnings.push(warning);
    }
    if let Some(err) = capability_error {
        plan.warnings.push(format!(
            "capability discovery is unavailable ({err}); safe optimizations are disabled"
        ));
    }
    Ok(PreparedSync {
        plan,
        options,
        local,
        remote,
        enable_hash_index: bucket.enable_hash_index,
        local_manifest_hash,
        supports_ensure_folder: capabilities
            .as_ref()
            .is_some_and(|value| value.ensure_folder),
        supports_atomic_upload: capabilities.as_ref().is_some_and(|value| {
            value.upload_sessions
                && value.atomic_commit
                && value.migration_state == ic_oss_types::entry::MigrationState::Ready
        }),
        supports_incremental_delete: capabilities.as_ref().is_some_and(|value| {
            value.conditional_delete
                && value.incremental_gc
                && value.migration_state == ic_oss_types::entry::MigrationState::Ready
        }),
        supports_batch_operations: capabilities
            .as_ref()
            .is_some_and(|value| value.batch_operations == Some(true)),
    })
}

pub async fn execute_sync(
    client: &Client,
    prepared: &PreparedSync,
    retry: u8,
) -> Result<(), String> {
    if prepared.plan.has_conflicts() {
        return Err("refusing to execute a sync plan containing conflicts".to_string());
    }
    if !prepared.supports_incremental_delete
        && prepared.plan.actions.iter().any(|action| {
            matches!(
                action,
                PlanAction::DeleteFile { .. } | PlanAction::DeleteDirectory { .. }
            )
        })
    {
        return Err(
            "refusing to execute delete actions because the bucket is not Ready with conditional deletion plus incremental garbage collection"
                .to_string(),
        );
    }
    if prepared.plan.replace_files > 0 && !prepared.supports_atomic_upload {
        return Err(
            "refusing to replace files because the bucket is not Ready with atomic upload support"
                .to_string(),
        );
    }

    let mut journal = journal::RecoveryJournal::start(
        &prepared.options.bucket,
        prepared.options.remote_parent,
        &prepared.options.local_root,
        &prepared.local_manifest_hash,
        &prepared.plan.actions,
    )?;
    let mut folder_ids = BTreeMap::from([(String::new(), prepared.options.remote_parent)]);
    for (path, entry) in &prepared.remote.entries {
        if entry.kind == types::EntryKind::Directory {
            folder_ids.insert(path.clone(), entry.id);
        }
    }

    if prepared.supports_batch_operations {
        batch_create_directories(client, prepared, retry, &mut journal, &mut folder_ids).await?;
        batch_upload_small_files(client, prepared, retry, &mut journal, &folder_ids).await?;
    }

    for action in &prepared.plan.actions {
        if prepared.supports_batch_operations
            && (matches!(action, PlanAction::CreateDirectory { .. })
                || matches!(action, PlanAction::UploadFile { size, .. } if *size <= MAX_BATCH_FILE_BYTES as u64))
        {
            continue;
        }
        journal.mark_started(action)?;
        match action {
            PlanAction::CreateDirectory { path } => {
                let parent_path = types::parent_path(path);
                let parent = folder_ids
                    .get(parent_path)
                    .copied()
                    .ok_or_else(|| format!("missing remote parent id for directory {}", path))?;
                let name = entry_name(path)?;
                let folder = create_directory(client, prepared, parent, name).await?;
                folder_ids.insert(path.clone(), folder.id);
                journal.mark_completed(action, Some(folder.id))?;
                println!("created directory {} (id {})", path, folder.id);
            }
            PlanAction::UploadFile { path, .. } => {
                let parent_path = types::parent_path(path);
                let parent = folder_ids
                    .get(parent_path)
                    .copied()
                    .ok_or_else(|| format!("missing remote parent id for file {}", path))?;
                let local_path = prepared.options.local_root.join(path);
                let local_entry = prepared
                    .local
                    .entries
                    .get(path)
                    .ok_or_else(|| format!("missing local metadata for {path}"))?;
                let file_id = if prepared.supports_atomic_upload {
                    upload_file_atomically(
                        client,
                        AtomicUpload {
                            parent,
                            local_path: &local_path,
                            hash: local_entry.hash,
                            remote_id: None,
                            action,
                        },
                        retry,
                        &mut journal,
                    )
                    .await?
                } else {
                    let local_path = local_path.to_str().ok_or_else(|| {
                        format!("local path is not valid UTF-8: {:?}", local_path)
                    })?;
                    crate::file::upload_file(
                        client,
                        prepared.enable_hash_index,
                        parent,
                        local_path,
                        retry,
                    )
                    .await?
                };
                journal.mark_completed(action, Some(file_id))?;
            }
            PlanAction::ReplaceFile {
                path, remote_id, ..
            } => {
                let parent_path = types::parent_path(path);
                let parent = folder_ids
                    .get(parent_path)
                    .copied()
                    .ok_or_else(|| format!("missing remote parent id for file {}", path))?;
                let local_path = prepared.options.local_root.join(path);
                let local_entry = prepared
                    .local
                    .entries
                    .get(path)
                    .ok_or_else(|| format!("missing local metadata for {}", path))?;
                let file_id = upload_file_atomically(
                    client,
                    AtomicUpload {
                        parent,
                        local_path: &local_path,
                        hash: local_entry.hash,
                        remote_id: Some(*remote_id),
                        action,
                    },
                    retry,
                    &mut journal,
                )
                .await?;
                journal.mark_completed(action, Some(file_id))?;
                println!("replaced file {} (id {})", path, file_id);
            }
            PlanAction::DeleteFile { path, remote_id } => {
                delete_file_conditionally(client, path, *remote_id, retry).await?;
                journal.mark_completed(action, Some(*remote_id))?;
                println!("deleted file {} (id {})", path, remote_id);
            }
            PlanAction::DeleteDirectory { path, remote_id } => {
                delete_directory_conditionally(client, path, *remote_id, retry).await?;
                folder_ids.remove(path);
                journal.mark_completed(action, Some(*remote_id))?;
                println!("deleted directory {} (id {})", path, remote_id);
            }
            PlanAction::Conflict { .. } => {}
        }
    }

    let verification = prepare_sync(client, prepared.options.clone()).await?;
    if verification.plan.has_conflicts() || !verification.plan.actions.is_empty() {
        print_plan(&verification.plan);
        return Err("post-sync verification found remaining differences".to_string());
    }
    if let Err(err) = journal.finish() {
        println!("WARN     {}", err);
    }
    println!("sync completed and remote metadata verification passed");
    Ok(())
}

async fn batch_create_directories(
    client: &Client,
    prepared: &PreparedSync,
    retry: u8,
    journal: &mut journal::RecoveryJournal,
    folder_ids: &mut BTreeMap<String, u32>,
) -> Result<(), String> {
    for batch in directory_action_batches(&prepared.plan.actions) {
        let mut folders = Vec::with_capacity(batch.len());
        for action in &batch {
            let PlanAction::CreateDirectory { path } = action else {
                unreachable!("directory batch contains a non-directory action")
            };
            let parent = folder_ids
                .get(types::parent_path(path))
                .copied()
                .ok_or_else(|| format!("missing remote parent id for directory {path}"))?;
            folders.push(CreateFolderInput {
                parent,
                name: entry_name(path)?.to_string(),
            });
        }
        for action in &batch {
            journal.mark_started(action)?;
        }

        let input = BatchEnsureFoldersInput {
            request_id: random_request_id()?,
            folders,
        };
        let output = retry_sync_call(retry, || client.batch_ensure_folders(input.clone())).await?;
        if output.results.len() != batch.len() {
            return Err(format!(
                "batch folder result count mismatch: expected {}, got {}",
                batch.len(),
                output.results.len()
            ));
        }

        let mut first_error = None;
        for (action, result) in batch.into_iter().zip(output.results) {
            let PlanAction::CreateDirectory { path } = action else {
                unreachable!("directory batch contains a non-directory action")
            };
            match result {
                Ok(folder) => {
                    folder_ids.insert(path.clone(), folder.id);
                    journal.mark_completed(action, Some(folder.id))?;
                    println!("created directory {} (id {}, batched)", path, folder.id);
                }
                Err(err) => {
                    first_error.get_or_insert_with(|| {
                        format!("batch directory creation failed for {path}: {err:?}")
                    });
                }
            }
        }
        if let Some(err) = first_error {
            return Err(err);
        }
    }
    Ok(())
}

async fn batch_upload_small_files(
    client: &Client,
    prepared: &PreparedSync,
    retry: u8,
    journal: &mut journal::RecoveryJournal,
    folder_ids: &BTreeMap<String, u32>,
) -> Result<(), String> {
    for batch in small_file_action_batches(&prepared.plan.actions) {
        let mut files = Vec::with_capacity(batch.len());
        for action in &batch {
            let PlanAction::UploadFile { path, size } = action else {
                unreachable!("small-file batch contains a non-upload action")
            };
            let parent = folder_ids
                .get(types::parent_path(path))
                .copied()
                .ok_or_else(|| format!("missing remote parent id for file {path}"))?;
            let local = prepared
                .local
                .entries
                .get(path)
                .ok_or_else(|| format!("missing local metadata for {path}"))?;
            let local_path = prepared.options.local_root.join(path);
            let content = tokio::fs::read(&local_path)
                .await
                .map_err(|err| format!("failed to read {local_path:?}: {err}"))?;
            if content.len() as u64 != *size {
                return Err(format!(
                    "local file changed after scan: {path} size was {size}, now {}",
                    content.len()
                ));
            }
            let hash = sha3_256(&content);
            if local.hash != Some(hash) {
                return Err(format!(
                    "local file changed after scan: {path} hash differs"
                ));
            }
            files.push(CreateFileInput {
                parent,
                name: entry_name(path)?.to_string(),
                content_type: content_type(&local_path)?,
                size: Some(*size),
                content: (!content.is_empty()).then(|| ByteBuf::from(content)),
                status: None,
                hash: Some(hash.into()),
                dek: None,
                custom: None,
            });
        }
        for action in &batch {
            journal.mark_started(action)?;
        }

        let input = BatchCreateSmallFilesInput {
            request_id: random_request_id()?,
            files,
        };
        let output =
            retry_sync_call(retry, || client.batch_create_small_files(input.clone())).await?;
        if output.results.len() != batch.len() {
            return Err(format!(
                "batch file result count mismatch: expected {}, got {}",
                batch.len(),
                output.results.len()
            ));
        }

        let mut first_error = None;
        for (action, result) in batch.into_iter().zip(output.results) {
            let PlanAction::UploadFile { path, .. } = action else {
                unreachable!("small-file batch contains a non-upload action")
            };
            match result {
                Ok(file) => {
                    journal.mark_completed(action, Some(file.id))?;
                    println!("uploaded file {} (id {}, batched)", path, file.id);
                }
                Err(err) => {
                    first_error.get_or_insert_with(|| {
                        format!("batch small-file upload failed for {path}: {err:?}")
                    });
                }
            }
        }
        if let Some(err) = first_error {
            return Err(err);
        }
    }
    Ok(())
}

fn directory_action_batches(actions: &[PlanAction]) -> Vec<Vec<&PlanAction>> {
    let mut by_depth: BTreeMap<usize, Vec<&PlanAction>> = BTreeMap::new();
    for action in actions {
        if let PlanAction::CreateDirectory { path } = action {
            by_depth
                .entry(types::path_depth(path))
                .or_default()
                .push(action);
        }
    }
    by_depth
        .into_values()
        .flat_map(|actions| {
            actions
                .chunks(MAX_BATCH_ITEMS)
                .map(<[_]>::to_vec)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn small_file_action_batches(actions: &[PlanAction]) -> Vec<Vec<&PlanAction>> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;
    for action in actions {
        let PlanAction::UploadFile { size, .. } = action else {
            continue;
        };
        let Ok(size) = usize::try_from(*size) else {
            continue;
        };
        if size > MAX_BATCH_FILE_BYTES {
            continue;
        }
        if !current.is_empty()
            && (current.len() == MAX_BATCH_ITEMS
                || current_bytes.saturating_add(size) > MAX_BATCH_INLINE_BYTES)
        {
            batches.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current.push(action);
        current_bytes = current_bytes.saturating_add(size);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

async fn delete_file_conditionally(
    client: &Client,
    path: &str,
    remote_id: u32,
    retry: u8,
) -> Result<(), String> {
    let file = client.get_file_info(remote_id).await?;
    let input = DeleteEntryIfInput {
        request_id: random_request_id()?,
        id: remote_id,
        kind: ApiEntryKind::File,
        expected_parent: file.parent,
        expected_revision: file.revision,
        expected_hash: file.hash,
    };
    let deleted = retry_sync_call(retry, || client.delete_entry_if(input.clone())).await?;
    if !deleted {
        return Err(format!("remote file disappeared before deletion: {path}"));
    }
    Ok(())
}

async fn delete_directory_conditionally(
    client: &Client,
    path: &str,
    remote_id: u32,
    retry: u8,
) -> Result<(), String> {
    let folder = client.get_folder_info(remote_id).await?;
    let input = DeleteEntryIfInput {
        request_id: random_request_id()?,
        id: remote_id,
        kind: ApiEntryKind::Folder,
        expected_parent: folder.parent,
        expected_revision: folder.revision,
        expected_hash: None,
    };
    let deleted = retry_sync_call(retry, || client.delete_entry_if(input.clone())).await?;
    if !deleted {
        return Err(format!(
            "remote directory disappeared before deletion: {path}"
        ));
    }
    Ok(())
}

async fn create_directory(
    client: &Client,
    prepared: &PreparedSync,
    parent: u32,
    name: &str,
) -> Result<CreateFolderOutput, String> {
    if !prepared.supports_ensure_folder {
        return client
            .create_folder(CreateFolderInput {
                parent,
                name: name.to_string(),
            })
            .await;
    }

    let mut request_id = vec![0u8; 16];
    SystemRandom::new()
        .fill(&mut request_id)
        .map_err(|_| "failed to generate ensure_folder request id".to_string())?;
    let output = client
        .ensure_folder(EnsureFolderInput {
            request_id: request_id.into(),
            parent,
            name: name.to_string(),
        })
        .await
        .map_err(|err| format!("ensure_folder failed: {:?}", err))?;
    Ok(CreateFolderOutput {
        id: output.id,
        created_at: output.created_at,
    })
}

struct AtomicUpload<'a> {
    parent: u32,
    local_path: &'a std::path::Path,
    hash: Option<[u8; 32]>,
    remote_id: Option<u32>,
    action: &'a PlanAction,
}

async fn upload_file_atomically(
    client: &Client,
    upload: AtomicUpload<'_>,
    retry: u8,
    journal: &mut journal::RecoveryJournal,
) -> Result<u32, String> {
    let AtomicUpload {
        parent,
        local_path,
        hash,
        remote_id,
        action,
    } = upload;
    let metadata = std::fs::metadata(local_path)
        .map_err(|err| format!("failed to inspect {:?}: {}", local_path, err))?;
    if !metadata.is_file() {
        return Err(format!("not a file: {:?}", local_path));
    }
    let size = metadata.len();
    let name = local_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid local filename: {:?}", local_path))?;
    let parent_revision = client.get_folder_info(parent).await?.revision;
    let replace = if let Some(remote_id) = remote_id {
        let remote = client.get_file_info(remote_id).await?;
        if remote.parent != parent || remote.name != name {
            return Err(format!(
                "remote replacement target changed: expected {}/{} at {}, got {}/{} at {}",
                parent, remote_id, name, remote.parent, remote.id, remote.name
            ));
        }
        Some(ReplaceFileInput {
            id: remote_id,
            expected_revision: remote.revision,
        })
    } else {
        None
    };

    let mut begin_input = BeginUploadInput {
        request_id: ByteBuf::from(Vec::new()),
        parent,
        name: name.to_string(),
        content_type: content_type(local_path)?,
        size,
        status: 0,
        hash: hash.map(Into::into),
        dek: None,
        custom: None,
        expected_parent_revision: parent_revision,
        replace,
    };
    let fingerprint = sha3_256(&to_cbor_bytes(&begin_input));
    let mut checkpoint =
        journal.prepare_upload(action, fingerprint, random_request_id()?.into_vec())?;
    let mut resumed = false;
    let (session_id, file_id, total_chunks, mut uploaded_chunks) = loop {
        if let Some(session_id) = checkpoint.session_id.clone() {
            match get_all_upload_status(client, ByteBuf::from(session_id.clone())).await {
                Ok((status, uploaded_chunks)) => {
                    validate_upload_status(&status, size, remote_id)?;
                    journal.set_uploaded_chunks(action, uploaded_chunks.clone())?;
                    resumed = true;
                    break (
                        ByteBuf::from(session_id),
                        status.file_id,
                        status.total_chunks,
                        uploaded_chunks,
                    );
                }
                Err(SyncError::NotFound(_)) => {
                    journal.reset_upload(action)?;
                    checkpoint = journal.prepare_upload(
                        action,
                        fingerprint,
                        random_request_id()?.into_vec(),
                    )?;
                    continue;
                }
                Err(SyncError::Conflict { message, .. }) if message.contains("expired") => {
                    journal.reset_upload(action)?;
                    checkpoint = journal.prepare_upload(
                        action,
                        fingerprint,
                        random_request_id()?.into_vec(),
                    )?;
                    continue;
                }
                Err(err) => {
                    return Err(format!("failed to resume upload session: {err:?}"));
                }
            }
        }

        begin_input.request_id = ByteBuf::from(checkpoint.begin_request_id.clone());
        let session = retry_sync_call(retry, || client.begin_upload(begin_input.clone())).await?;
        journal.set_upload_session(action, session.session_id.clone().into_vec())?;
        break (
            session.session_id,
            session.file_id,
            session.total_chunks,
            BTreeSet::new(),
        );
    };

    if resumed {
        println!(
            "resuming atomic upload {}: {}/{} chunks already stored",
            local_path.display(),
            uploaded_chunks.len(),
            total_chunks
        );
        let renew = RenewUploadInput {
            request_id: random_request_id()?,
            session_id: session_id.clone(),
        };
        retry_sync_call(retry, || client.renew_upload(renew.clone())).await?;
    }

    let mut file = tokio::fs::File::open(local_path)
        .await
        .map_err(|err| format!("failed to open {:?}: {}", local_path, err))?;

    for chunk_index in 0..total_chunks {
        if uploaded_chunks.contains(&chunk_index) {
            continue;
        }
        if chunk_index > 0 && chunk_index % 512 == 0 {
            let renew = RenewUploadInput {
                request_id: random_request_id()?,
                session_id: session_id.clone(),
            };
            retry_sync_call(retry, || client.renew_upload(renew.clone())).await?;
        }
        let offset = chunk_index as u64 * CHUNK_SIZE as u64;
        let expected = (size - offset).min(CHUNK_SIZE as u64) as usize;
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|err| format!("failed to seek {:?}: {}", local_path, err))?;
        let mut content = vec![0u8; expected];
        file.read_exact(&mut content)
            .await
            .map_err(|err| format!("failed to read {:?}: {}", local_path, err))?;
        let input = UploadChunkInput {
            request_id: random_request_id()?,
            session_id: session_id.clone(),
            chunk_index,
            content: ByteBuf::from(content),
        };
        let progress = retry_sync_call(retry, || client.upload_chunk(input.clone())).await?;
        uploaded_chunks.insert(chunk_index);
        journal.mark_uploaded_chunk(action, chunk_index)?;
        println!(
            "uploaded atomically {}: {}/{} bytes",
            local_path.display(),
            progress.filled,
            size
        );
    }

    let commit = CommitUploadInput {
        request_id: random_request_id()?,
        session_id,
    };
    let committed = retry_sync_call(retry, || client.commit_upload(commit.clone())).await?;
    let expected_created = remote_id.is_none();
    if committed.id != file_id || committed.created != expected_created {
        return Err(format!(
            "atomic upload returned unexpected file id {} (created={})",
            committed.id, committed.created
        ));
    }
    journal.reset_upload(action)?;
    Ok(committed.id)
}

async fn get_all_upload_status(
    client: &Client,
    session_id: ByteBuf,
) -> Result<(UploadStatusOutput, BTreeSet<u32>), SyncError> {
    let mut cursor = None;
    let mut first = None;
    let mut uploaded_chunks = BTreeSet::new();
    loop {
        let page = client
            .get_upload_status(GetUploadStatusInput {
                session_id: session_id.clone(),
                start: cursor,
                take: Some(1024),
            })
            .await?;
        for range in &page.ranges {
            uploaded_chunks.extend(range.start..=range.end);
        }
        if first.is_none() {
            first = Some(page.clone());
        }
        match page.next {
            Some(next) => cursor = Some(next),
            None => {
                return Ok((
                    first.expect("upload status returned at least one page"),
                    uploaded_chunks,
                ));
            }
        }
    }
}

fn validate_upload_status(
    status: &UploadStatusOutput,
    expected_size: u64,
    expected_file_id: Option<u32>,
) -> Result<(), String> {
    let expected_chunks = expected_size.div_ceil(CHUNK_SIZE as u64);
    if status.size != expected_size || u64::from(status.total_chunks) != expected_chunks {
        return Err("saved upload session no longer matches the local file size".to_string());
    }
    if expected_file_id.is_some_and(|id| id != status.file_id) {
        return Err("saved upload session no longer matches the replacement target".to_string());
    }
    Ok(())
}

async fn retry_sync_call<T, F, Fut>(retry: u8, mut call: F) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, SyncError>>,
{
    let mut attempt = 0u8;
    loop {
        match call().await {
            Ok(output) => return Ok(output),
            Err(err @ SyncError::Internal(_)) if attempt < retry => {
                attempt = attempt.saturating_add(1);
                let delay = Duration::from_secs((1u64 << attempt.saturating_sub(1).min(5)).min(30));
                println!(
                    "transient sync operation error: {:?}; retry {} after {:?}",
                    err, attempt, delay
                );
                tokio::time::sleep(delay).await;
            }
            Err(err) => return Err(format!("sync operation failed: {:?}", err)),
        }
    }
}

fn random_request_id() -> Result<ByteBuf, String> {
    let mut request_id = vec![0u8; 16];
    SystemRandom::new()
        .fill(&mut request_id)
        .map_err(|_| "failed to generate upload request id".to_string())?;
    Ok(ByteBuf::from(request_id))
}

fn manifest_hash(manifest: &types::LocalManifest) -> [u8; 32] {
    use sha3::{Digest, Sha3_256};

    let mut hasher = Sha3_256::new();
    for (path, entry) in &manifest.entries {
        hasher.update((path.len() as u64).to_be_bytes());
        hasher.update(path.as_bytes());
        hasher.update([match entry.kind {
            types::EntryKind::File => 0,
            types::EntryKind::Directory => 1,
        }]);
        hasher.update(entry.size.to_be_bytes());
        if let Some(hash) = entry.hash {
            hasher.update([1]);
            hasher.update(hash);
        } else {
            hasher.update([0]);
        }
    }
    hasher.finalize().into()
}

fn entry_name(path: &str) -> Result<&str, String> {
    path.rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| format!("invalid empty entry name in relative path {:?}", path))
}

fn content_type(path: &std::path::Path) -> Result<String, String> {
    Ok(infer::get_from_path(path)
        .map_err(|err| format!("failed to infer content type for {path:?}: {err}"))?
        .map(|kind| kind.mime_type().to_string())
        .or_else(|| path.to_str().and_then(mime_db::lookup).map(str::to_string))
        .unwrap_or_else(|| "application/octet-stream".to_string()))
}

pub fn print_plan(plan: &Plan) {
    println!("Directory sync plan");
    println!("===================");
    for warning in &plan.warnings {
        println!("WARN     {}", warning);
    }
    for action in &plan.actions {
        match action {
            PlanAction::Conflict { path, reason } => {
                println!("CONFLICT {:<48} {}", path, reason)
            }
            PlanAction::CreateDirectory { path } => println!("MKDIR    {}", path),
            PlanAction::UploadFile { path, size } => {
                println!("UPLOAD   {:<48} {}", path, format_bytes(*size))
            }
            PlanAction::ReplaceFile {
                path,
                remote_id,
                size,
            } => println!(
                "REPLACE  {:<48} {} (remote id {})",
                path,
                format_bytes(*size),
                remote_id
            ),
            PlanAction::DeleteFile { path, remote_id } => {
                println!("DELETE   {:<48} file {}", path, remote_id)
            }
            PlanAction::DeleteDirectory { path, remote_id } => {
                println!("RMDIR    {:<48} folder {}", path, remote_id)
            }
        }
    }

    println!();
    println!("Create directories: {}", plan.create_directories);
    println!("Upload files:       {}", plan.upload_files);
    println!("Replace files:      {}", plan.replace_files);
    println!("Delete files:       {}", plan.delete_files);
    println!("Delete directories: {}", plan.delete_directories);
    println!("Conflicts:          {}", plan.conflicts);
    println!("Unchanged:          {}", plan.unchanged);
    println!("Remote-only kept:   {}", plan.retained_remote);
    println!("Upload bytes:       {}", format_bytes(plan.upload_bytes));
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{:.1} {}", value, UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_batches_respect_depth_and_item_limit() {
        let mut actions = (0..34)
            .map(|index| PlanAction::CreateDirectory {
                path: format!("folder-{index:02}"),
            })
            .collect::<Vec<_>>();
        actions.push(PlanAction::CreateDirectory {
            path: "folder-00/nested".to_string(),
        });
        actions.push(PlanAction::UploadFile {
            path: "ignored.txt".to_string(),
            size: 1,
        });

        let batches = directory_action_batches(&actions);
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![32, 2, 1]
        );
        assert!(batches[0].iter().all(|action| matches!(
            action,
            PlanAction::CreateDirectory { path } if types::path_depth(path) == 1
        )));
        assert!(matches!(
            batches[2][0],
            PlanAction::CreateDirectory { path } if path == "folder-00/nested"
        ));
    }

    #[test]
    fn small_file_batches_respect_byte_item_and_per_file_limits() {
        let byte_limited = (0..7)
            .map(|index| PlanAction::UploadFile {
                path: format!("file-{index}.bin"),
                size: MAX_BATCH_FILE_BYTES as u64,
            })
            .collect::<Vec<_>>();
        let batches = small_file_action_batches(&byte_limited);
        assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), vec![6, 1]);

        let mut count_limited = (0..33)
            .map(|index| PlanAction::UploadFile {
                path: format!("empty-{index}.bin"),
                size: 0,
            })
            .collect::<Vec<_>>();
        count_limited.push(PlanAction::UploadFile {
            path: "large.bin".to_string(),
            size: MAX_BATCH_FILE_BYTES as u64 + 1,
        });
        let batches = small_file_action_batches(&count_limited);
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![32, 1]
        );
        assert!(batches.iter().flatten().all(|action| matches!(
            action,
            PlanAction::UploadFile { path, .. } if path != "large.bin"
        )));
    }
}
