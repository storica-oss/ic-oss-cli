use super::{
    local, remote,
    types::{path_depth, EntryKind, LocalManifest, RemoteEntry, RemoteManifest},
};
use globset::{Glob, GlobSet, GlobSetBuilder};
use ic_oss::bucket::Client;
use ic_oss_types::file::{FileInfo, CHUNK_SIZE};
use sha3::{Digest, Sha3_256};
use std::{
    cmp::Ordering,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
    time::Duration,
};
use tokio::io::{AsyncWriteExt, BufWriter};

pub const MAX_DOWNLOAD_RETRIES: u8 = 10;
const DOWNLOAD_CHUNKS_PER_CALL: u32 = 6;
static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct PullOptions {
    pub local_root: PathBuf,
    pub remote_parent: u32,
    pub delete: bool,
    pub overwrite: bool,
    pub excludes: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PullAction {
    Conflict {
        path: String,
        reason: String,
    },
    CreateDirectory {
        path: String,
    },
    DownloadFile {
        path: String,
        remote_id: u32,
        size: u64,
    },
    ReplaceFile {
        path: String,
        remote_id: u32,
        size: u64,
    },
    DeleteFile {
        path: String,
    },
    DeleteDirectory {
        path: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct PullPlan {
    pub actions: Vec<PullAction>,
    pub warnings: Vec<String>,
    pub create_directories: usize,
    pub download_files: usize,
    pub replace_files: usize,
    pub delete_files: usize,
    pub delete_directories: usize,
    pub conflicts: usize,
    pub unchanged: usize,
    pub retained_local: usize,
    pub download_bytes: u64,
}

impl PullPlan {
    pub fn has_conflicts(&self) -> bool {
        self.conflicts > 0
    }

    fn push(&mut self, action: PullAction) {
        match &action {
            PullAction::Conflict { .. } => self.conflicts += 1,
            PullAction::CreateDirectory { .. } => self.create_directories += 1,
            PullAction::DownloadFile { size, .. } => {
                self.download_files += 1;
                self.download_bytes += size;
            }
            PullAction::ReplaceFile { size, .. } => {
                self.replace_files += 1;
                self.download_bytes += size;
            }
            PullAction::DeleteFile { .. } => self.delete_files += 1,
            PullAction::DeleteDirectory { .. } => self.delete_directories += 1,
        }
        self.actions.push(action);
    }
}

#[derive(Clone, Debug)]
pub struct PreparedPull {
    pub plan: PullPlan,
    options: PullOptions,
    remote: RemoteManifest,
}

pub async fn prepare_pull(
    client: &Client,
    mut options: PullOptions,
) -> Result<PreparedPull, String> {
    options.local_root = normalize_local_root(&options.local_root)?;
    let bucket = client.get_bucket_info().await?;
    let target = client.get_folder_info(options.remote_parent).await?;
    let capabilities = client.get_capabilities().await.ok();
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

    let local = if options.local_root.exists() {
        local::scan_local(
            &options.local_root,
            &options.excludes,
            u64::MAX,
            usize::MAX,
            0,
            usize::MAX,
        )?
    } else {
        validate_excludes(&options.excludes)?;
        LocalManifest::default()
    };
    println!(
        "local scan: {} cached hashes, {} files hashed",
        local.cache_hits, local.hashed_files
    );
    let mut plan = plan_pull(
        &local,
        &remote,
        options.delete,
        options.overwrite,
        &options.excludes,
    )?;
    if !bucket.enable_hash_index
        && remote
            .entries
            .values()
            .any(|entry| entry.kind == EntryKind::File && entry.hash.is_none())
    {
        plan.warnings.push(
            "the bucket does not expose hashes for every file; existing same-size files remain conflicts unless --overwrite is used"
                .to_string(),
        );
    }
    Ok(PreparedPull {
        plan,
        options,
        remote,
    })
}

fn normalize_local_root(path: &Path) -> Result<PathBuf, String> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|err| format!("failed to resolve current directory: {err}"))?
            .join(path)
    };
    match std::fs::symlink_metadata(&absolute) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(format!(
            "local download root must not be a symbolic link: {:?}",
            absolute
        )),
        Ok(metadata) if !metadata.is_dir() => Err(format!(
            "local download path is not a directory: {:?}",
            absolute
        )),
        Ok(_) => absolute
            .canonicalize()
            .map_err(|err| format!("failed to resolve local root {:?}: {err}", absolute)),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(absolute),
        Err(err) => Err(format!(
            "failed to inspect local root {:?}: {err}",
            absolute
        )),
    }
}

fn plan_pull(
    local: &LocalManifest,
    remote: &RemoteManifest,
    delete: bool,
    overwrite: bool,
    excludes: &[String],
) -> Result<PullPlan, String> {
    let excludes = compile_excludes(excludes)?;
    let mut plan = PullPlan::default();
    plan.warnings.extend(local.warnings.iter().cloned());
    plan.warnings.extend(remote.warnings.iter().cloned());

    for (path, reason) in &remote.conflicts {
        if !is_excluded(&excludes, path) {
            plan.push(PullAction::Conflict {
                path: path.clone(),
                reason: reason.clone(),
            });
        }
    }

    for (path, remote_entry) in &remote.entries {
        if is_excluded(&excludes, path) {
            continue;
        }
        if let Err(reason) = validate_relative_path(path) {
            plan.push(PullAction::Conflict {
                path: path.clone(),
                reason,
            });
            continue;
        }
        match local.entries.get(path) {
            None if remote_entry.kind == EntryKind::Directory => {
                plan.push(PullAction::CreateDirectory { path: path.clone() });
            }
            None => plan.push(PullAction::DownloadFile {
                path: path.clone(),
                remote_id: remote_entry.id,
                size: remote_entry.size,
            }),
            Some(local_entry) if local_entry.kind != remote_entry.kind => {
                plan.push(PullAction::Conflict {
                    path: path.clone(),
                    reason: format!(
                        "remote {:?} conflicts with local {:?}; move or remove the local entry first",
                        remote_entry.kind, local_entry.kind
                    ),
                });
            }
            Some(_) if remote_entry.kind == EntryKind::Directory => plan.unchanged += 1,
            Some(local_entry)
                if remote_entry.hash.is_some() && remote_entry.hash == local_entry.hash =>
            {
                plan.unchanged += 1;
            }
            Some(_) if !overwrite => plan.push(PullAction::Conflict {
                path: path.clone(),
                reason: format!(
                    "local file differs from remote file {}; rerun with --overwrite after reviewing the plan",
                    remote_entry.id
                ),
            }),
            Some(_) => plan.push(PullAction::ReplaceFile {
                path: path.clone(),
                remote_id: remote_entry.id,
                size: remote_entry.size,
            }),
        }
    }

    for (path, local_entry) in &local.entries {
        if remote.entries.contains_key(path) || is_excluded(&excludes, path) {
            continue;
        }
        if !delete || is_path_protected(local, path) {
            plan.retained_local += 1;
        } else if local_entry.kind == EntryKind::File {
            plan.push(PullAction::DeleteFile { path: path.clone() });
        } else {
            plan.push(PullAction::DeleteDirectory { path: path.clone() });
        }
    }

    plan.actions.sort_by(compare_actions);
    Ok(plan)
}

fn is_path_protected(local: &LocalManifest, path: &str) -> bool {
    local.protected_paths.iter().any(|protected| {
        protected == path
            || protected
                .strip_prefix(path)
                .is_some_and(|suffix| suffix.starts_with('/'))
            || path
                .strip_prefix(protected)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

fn compare_actions(left: &PullAction, right: &PullAction) -> Ordering {
    let left = action_sort_key(left);
    let right = action_sort_key(right);
    left.0
        .cmp(&right.0)
        .then_with(|| left.1.cmp(&right.1))
        .then_with(|| left.2.cmp(right.2))
}

fn action_sort_key(action: &PullAction) -> (u8, isize, &str) {
    match action {
        PullAction::Conflict { path, .. } => (0, path_depth(path) as isize, path),
        PullAction::CreateDirectory { path } => (1, path_depth(path) as isize, path),
        PullAction::DownloadFile { path, .. } => (2, path_depth(path) as isize, path),
        PullAction::ReplaceFile { path, .. } => (3, path_depth(path) as isize, path),
        PullAction::DeleteFile { path } => (4, -(path_depth(path) as isize), path),
        PullAction::DeleteDirectory { path } => (5, -(path_depth(path) as isize), path),
    }
}

fn validate_excludes(patterns: &[String]) -> Result<(), String> {
    compile_excludes(patterns).map(|_| ())
}

fn compile_excludes(patterns: &[String]) -> Result<GlobSet, String> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(
            Glob::new(pattern)
                .map_err(|err| format!("invalid exclude pattern {:?}: {err}", pattern))?,
        );
    }
    builder
        .build()
        .map_err(|err| format!("failed to compile exclude patterns: {err}"))
}

fn is_excluded(excludes: &GlobSet, path: &str) -> bool {
    let mut candidate = path;
    loop {
        if excludes.is_match(candidate) || excludes.is_match(format!("{candidate}/")) {
            return true;
        }
        let Some((parent, _)) = candidate.rsplit_once('/') else {
            return false;
        };
        candidate = parent;
    }
}

fn validate_relative_path(path: &str) -> Result<(), String> {
    if path.is_empty() || path.starts_with('/') || path.ends_with('/') {
        return Err("remote entry has an invalid relative path".to_string());
    }
    for component in path.split('/') {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.contains('\\')
            || component.contains('\0')
        {
            return Err(format!(
                "remote entry contains an unsafe path component {:?}",
                component
            ));
        }
    }
    Ok(())
}

fn local_path(root: &Path, relative: &str) -> Result<PathBuf, String> {
    validate_relative_path(relative)?;
    let mut path = root.to_path_buf();
    for component in relative.split('/') {
        path.push(component);
    }
    Ok(path)
}

pub async fn execute_pull(
    client: &Client,
    prepared: &PreparedPull,
    retry: u8,
) -> Result<(), String> {
    if retry > MAX_DOWNLOAD_RETRIES {
        return Err(format!(
            "retry count {retry} exceeds maximum {MAX_DOWNLOAD_RETRIES}"
        ));
    }
    if prepared.plan.has_conflicts() {
        return Err("refusing to execute a download plan containing conflicts".to_string());
    }
    let root = ensure_root(&prepared.options.local_root).await?;
    for action in &prepared.plan.actions {
        match action {
            PullAction::Conflict { .. } => unreachable!("conflicts are rejected before execution"),
            PullAction::CreateDirectory { path } => {
                ensure_local_directory(&root, path).await?;
                println!("created local directory {path}");
            }
            PullAction::DownloadFile {
                path, remote_id, ..
            } => {
                let expected = prepared
                    .remote
                    .entries
                    .get(path)
                    .ok_or_else(|| format!("missing prepared remote entry for {path}"))?;
                download_file(client, &root, path, *remote_id, expected, false, retry).await?;
            }
            PullAction::ReplaceFile {
                path, remote_id, ..
            } => {
                let expected = prepared
                    .remote
                    .entries
                    .get(path)
                    .ok_or_else(|| format!("missing prepared remote entry for {path}"))?;
                download_file(client, &root, path, *remote_id, expected, true, retry).await?;
            }
            PullAction::DeleteFile { path } => delete_local_file(&root, path).await?,
            PullAction::DeleteDirectory { path } => delete_local_directory(&root, path).await?,
        }
    }
    println!("download sync completed; file hashes and remote revisions verified");
    Ok(())
}

async fn ensure_root(root: &Path) -> Result<PathBuf, String> {
    match tokio::fs::symlink_metadata(root).await {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(format!(
                "local download root became a symbolic link: {root:?}"
            ));
        }
        Ok(metadata) if !metadata.is_dir() => {
            return Err(format!("local download root is not a directory: {root:?}"));
        }
        Ok(_) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => tokio::fs::create_dir_all(root)
            .await
            .map_err(|err| format!("failed to create local root {root:?}: {err}"))?,
        Err(err) => return Err(format!("failed to inspect local root {root:?}: {err}")),
    }
    root.canonicalize()
        .map_err(|err| format!("failed to resolve local root {root:?}: {err}"))
}

async fn ensure_local_directory(root: &Path, relative: &str) -> Result<(), String> {
    let path = local_path(root, relative)?;
    ensure_secure_parent(root, &path).await?;
    match tokio::fs::symlink_metadata(&path).await {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(format!("refusing to traverse symbolic link {path:?}"))
        }
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(format!(
            "local directory path is occupied by a file: {path:?}"
        )),
        Err(err) if err.kind() == ErrorKind::NotFound => tokio::fs::create_dir(&path)
            .await
            .map_err(|err| format!("failed to create local directory {path:?}: {err}")),
        Err(err) => Err(format!("failed to inspect local directory {path:?}: {err}")),
    }
}

async fn ensure_secure_parent(root: &Path, path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("local path has no parent: {path:?}"))?;
    let relative = parent
        .strip_prefix(root)
        .map_err(|_| format!("local path escapes download root: {path:?}"))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        match tokio::fs::symlink_metadata(&current).await {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!("refusing to traverse symbolic link {current:?}"));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(format!("local parent is not a directory: {current:?}"));
            }
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {
                tokio::fs::create_dir(&current).await.map_err(|err| {
                    format!("failed to create local directory {current:?}: {err}")
                })?;
            }
            Err(err) => return Err(format!("failed to inspect {current:?}: {err}")),
        }
    }
    let resolved = parent
        .canonicalize()
        .map_err(|err| format!("failed to resolve local parent {parent:?}: {err}"))?;
    if !resolved.starts_with(root) {
        return Err(format!("local parent escapes download root: {resolved:?}"));
    }
    Ok(())
}

async fn download_file(
    client: &Client,
    root: &Path,
    relative: &str,
    remote_id: u32,
    expected: &RemoteEntry,
    replace: bool,
    retry: u8,
) -> Result<(), String> {
    let destination = local_path(root, relative)?;
    ensure_secure_parent(root, &destination).await?;
    validate_destination(&destination, replace).await?;
    let before = get_file_info_retry(client, remote_id, retry).await?;
    validate_remote_file(&before, expected)?;

    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
    let temporary = destination.with_file_name(format!(
        ".ic-oss-download-{}-{remote_id}-{sequence}.part",
        std::process::id()
    ));
    let result = download_to_temporary(client, &temporary, relative, &before, retry).await;
    if let Err(err) = result {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(err);
    }
    let after = match get_file_info_retry(client, remote_id, retry).await {
        Ok(after) => after,
        Err(err) => {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(err);
        }
    };
    if before.revision != after.revision
        || before.generation != after.generation
        || before.size != after.size
        || before.filled != after.filled
        || before.hash != after.hash
    {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(format!(
            "remote file changed during download: {relative} (id {remote_id})"
        ));
    }
    if let Err(err) = validate_destination(&destination, replace).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(err);
    }
    tokio::fs::rename(&temporary, &destination)
        .await
        .map_err(|err| {
            let _ = std::fs::remove_file(&temporary);
            format!("failed to publish downloaded file {destination:?}: {err}")
        })?;
    println!(
        "{} {} ({} bytes, id {})",
        if replace { "replaced" } else { "downloaded" },
        relative,
        before.size,
        remote_id
    );
    Ok(())
}

async fn validate_destination(path: &Path, replace: bool) -> Result<(), String> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(format!("refusing to replace symbolic link {path:?}"))
        }
        Ok(metadata) if !metadata.is_file() => Err(format!(
            "local file path is occupied by a directory: {path:?}"
        )),
        Ok(_) if !replace => Err(format!("local file appeared during sync: {path:?}")),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound && replace => {
            Err(format!("local replacement target disappeared: {path:?}"))
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!(
            "failed to inspect local destination {path:?}: {err}"
        )),
    }
}

fn validate_remote_file(info: &FileInfo, expected: &RemoteEntry) -> Result<(), String> {
    if info.id != expected.id
        || info.size != expected.size
        || info.filled != expected.filled
        || info.hash.as_ref().map(|hash| **hash) != expected.hash
    {
        return Err(format!(
            "remote file changed after planning: {} (id {})",
            expected.path, expected.id
        ));
    }
    if info.size != info.filled {
        return Err(format!(
            "remote file is incomplete: {} ({}/{})",
            expected.path, info.filled, info.size
        ));
    }
    Ok(())
}

async fn download_to_temporary(
    client: &Client,
    temporary: &Path,
    relative: &str,
    info: &FileInfo,
    retry: u8,
) -> Result<(), String> {
    let file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temporary)
        .await
        .map_err(|err| format!("failed to create temporary file {temporary:?}: {err}"))?;
    let mut file = BufWriter::new(file);
    let mut hasher = Sha3_256::new();
    let mut next_chunk = 0u32;
    let mut downloaded = 0u64;
    while next_chunk < info.chunks {
        let mut chunks = get_file_chunks_retry(client, info.id, next_chunk, retry).await?;
        if chunks.is_empty() {
            return Err(format!(
                "remote file returned no chunk at index {next_chunk}: {relative}"
            ));
        }
        chunks.sort_by_key(|chunk| chunk.0);
        for chunk in chunks {
            if chunk.0 != next_chunk {
                return Err(format!(
                    "remote file returned unexpected chunk {}, expected {}: {}",
                    chunk.0, next_chunk, relative
                ));
            }
            let remaining = info.size.saturating_sub(downloaded);
            if chunk.1.len() as u64 > remaining
                || (chunk.1.len() as u32 != CHUNK_SIZE && next_chunk + 1 < info.chunks)
            {
                return Err(format!("remote file returned an invalid chunk: {relative}"));
            }
            hasher.update(&chunk.1);
            file.write_all(&chunk.1)
                .await
                .map_err(|err| format!("failed to write temporary file {temporary:?}: {err}"))?;
            downloaded += chunk.1.len() as u64;
            next_chunk += 1;
        }
        println!(
            "downloaded {}: {}/{} bytes",
            relative, downloaded, info.size
        );
    }
    if downloaded != info.size || next_chunk != info.chunks {
        return Err(format!(
            "downloaded file length mismatch for {relative}: {downloaded}/{} bytes",
            info.size
        ));
    }
    let hash: [u8; 32] = hasher.finalize().into();
    if let Some(expected) = info.hash.as_ref() {
        if **expected != hash {
            return Err(format!(
                "downloaded file hash mismatch for {relative}: expected {}, got {}",
                hex::encode(**expected),
                hex::encode(hash)
            ));
        }
    }
    file.flush()
        .await
        .map_err(|err| format!("failed to flush temporary file {temporary:?}: {err}"))?;
    file.get_ref()
        .sync_all()
        .await
        .map_err(|err| format!("failed to sync temporary file {temporary:?}: {err}"))?;
    Ok(())
}

async fn get_file_info_retry(client: &Client, id: u32, retry: u8) -> Result<FileInfo, String> {
    let mut attempt = 0u8;
    loop {
        match client.get_file_info(id).await {
            Ok(info) => return Ok(info),
            Err(err) if attempt < retry => {
                attempt += 1;
                let delay = retry_delay(attempt);
                eprintln!(
                    "file info query failed for id {id}: {err}; retry {attempt}/{retry} after {delay:?}"
                );
                tokio::time::sleep(delay).await;
            }
            Err(err) => return Err(format!("failed to read remote file info {id}: {err}")),
        }
    }
}

async fn get_file_chunks_retry(
    client: &Client,
    id: u32,
    index: u32,
    retry: u8,
) -> Result<Vec<ic_oss_types::file::FileChunk>, String> {
    let mut attempt = 0u8;
    loop {
        match client
            .get_file_chunks(id, index, Some(DOWNLOAD_CHUNKS_PER_CALL))
            .await
        {
            Ok(chunks) => return Ok(chunks),
            Err(err) if attempt < retry => {
                attempt += 1;
                let delay = retry_delay(attempt);
                eprintln!(
                    "chunk query failed for file {id} at {index}: {err}; retry {attempt}/{retry} after {delay:?}"
                );
                tokio::time::sleep(delay).await;
            }
            Err(err) => {
                return Err(format!(
                    "failed to download file {id} at chunk {index}: {err}"
                ));
            }
        }
    }
}

fn retry_delay(attempt: u8) -> Duration {
    Duration::from_secs((1u64 << attempt.saturating_sub(1).min(5)).min(30))
}

async fn delete_local_file(root: &Path, relative: &str) -> Result<(), String> {
    let path = local_path(root, relative)?;
    ensure_secure_parent(root, &path).await?;
    let metadata = tokio::fs::symlink_metadata(&path)
        .await
        .map_err(|err| format!("failed to inspect local file {path:?}: {err}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "refusing to delete non-regular local file {path:?}"
        ));
    }
    tokio::fs::remove_file(&path)
        .await
        .map_err(|err| format!("failed to delete local file {path:?}: {err}"))?;
    println!("deleted local file {relative}");
    Ok(())
}

async fn delete_local_directory(root: &Path, relative: &str) -> Result<(), String> {
    let path = local_path(root, relative)?;
    ensure_secure_parent(root, &path).await?;
    let metadata = tokio::fs::symlink_metadata(&path)
        .await
        .map_err(|err| format!("failed to inspect local directory {path:?}: {err}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "refusing to delete non-directory local entry {path:?}"
        ));
    }
    tokio::fs::remove_dir(&path)
        .await
        .map_err(|err| format!("failed to delete local directory {path:?}: {err}"))?;
    println!("deleted local directory {relative}");
    Ok(())
}

pub fn print_pull_plan(plan: &PullPlan) {
    println!("Directory download sync plan");
    println!("============================");
    for warning in &plan.warnings {
        println!("WARN      {warning}");
    }
    for action in &plan.actions {
        match action {
            PullAction::Conflict { path, reason } => {
                println!("CONFLICT  {path:<48} {reason}")
            }
            PullAction::CreateDirectory { path } => println!("MKDIR     {path}"),
            PullAction::DownloadFile {
                path,
                remote_id,
                size,
            } => println!(
                "DOWNLOAD  {:<48} {} (remote id {})",
                path,
                format_bytes(*size),
                remote_id
            ),
            PullAction::ReplaceFile {
                path,
                remote_id,
                size,
            } => println!(
                "REPLACE   {:<48} {} (remote id {})",
                path,
                format_bytes(*size),
                remote_id
            ),
            PullAction::DeleteFile { path } => println!("DELETE    {path}"),
            PullAction::DeleteDirectory { path } => println!("RMDIR     {path}"),
        }
    }
    println!();
    println!("Create directories: {}", plan.create_directories);
    println!("Download files:     {}", plan.download_files);
    println!("Replace files:      {}", plan.replace_files);
    println!("Delete files:       {}", plan.delete_files);
    println!("Delete directories: {}", plan.delete_directories);
    println!("Conflicts:          {}", plan.conflicts);
    println!("Unchanged:          {}", plan.unchanged);
    println!("Local-only kept:    {}", plan.retained_local);
    println!("Download bytes:     {}", format_bytes(plan.download_bytes));
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
    use crate::sync::types::{LocalEntry, RemoteEntry};

    fn local_file(path: &str, hash: u8) -> LocalEntry {
        LocalEntry {
            path: path.to_string(),
            kind: EntryKind::File,
            size: 10,
            hash: Some([hash; 32]),
        }
    }

    fn remote_file(path: &str, id: u32, hash: Option<u8>) -> RemoteEntry {
        RemoteEntry {
            path: path.to_string(),
            id,
            parent: 0,
            kind: EntryKind::File,
            size: 10,
            filled: 10,
            hash: hash.map(|value| [value; 32]),
            status: 0,
        }
    }

    #[test]
    fn plans_recursive_downloads_and_detects_changed_local_files() {
        let mut remote = RemoteManifest::default();
        remote.entries.insert(
            "docs".into(),
            RemoteEntry {
                path: "docs".into(),
                id: 1,
                parent: 0,
                kind: EntryKind::Directory,
                size: 0,
                filled: 0,
                hash: None,
                status: 0,
            },
        );
        remote.entries.insert(
            "docs/readme.md".into(),
            remote_file("docs/readme.md", 2, Some(7)),
        );
        let plan = plan_pull(&LocalManifest::default(), &remote, false, false, &[]).unwrap();
        assert!(matches!(
            plan.actions[0],
            PullAction::CreateDirectory { .. }
        ));
        assert!(matches!(plan.actions[1], PullAction::DownloadFile { .. }));

        let mut local = LocalManifest::default();
        local
            .entries
            .insert("docs/readme.md".into(), local_file("docs/readme.md", 8));
        let plan = plan_pull(&local, &remote, false, false, &[]).unwrap();
        assert!(plan.has_conflicts());
        let plan = plan_pull(&local, &remote, false, true, &[]).unwrap();
        assert!(plan
            .actions
            .iter()
            .any(|action| matches!(action, PullAction::ReplaceFile { .. })));
    }

    #[test]
    fn local_only_entries_are_deleted_deepest_first() {
        let mut local = LocalManifest::default();
        local.entries.insert(
            "old".into(),
            LocalEntry {
                path: "old".into(),
                kind: EntryKind::Directory,
                size: 0,
                hash: None,
            },
        );
        local
            .entries
            .insert("old/file.txt".into(), local_file("old/file.txt", 1));
        let plan = plan_pull(&local, &RemoteManifest::default(), true, false, &[]).unwrap();
        assert!(matches!(plan.actions[0], PullAction::DeleteFile { .. }));
        assert!(matches!(
            plan.actions[1],
            PullAction::DeleteDirectory { .. }
        ));
    }

    #[test]
    fn protected_descendants_keep_their_parent_directory() {
        let mut local = LocalManifest::default();
        local.entries.insert(
            "cache".into(),
            LocalEntry {
                path: "cache".into(),
                kind: EntryKind::Directory,
                size: 0,
                hash: None,
            },
        );
        local.protected_paths.insert("cache/link".into());
        let plan = plan_pull(&local, &RemoteManifest::default(), true, false, &[]).unwrap();
        assert!(plan.actions.is_empty());
        assert_eq!(plan.retained_local, 1);
    }

    #[test]
    fn rejects_unsafe_remote_paths_and_honors_excluded_subtrees() {
        for path in ["../secret", "a/../secret", "/absolute", "a\\b"] {
            assert!(validate_relative_path(path).is_err(), "accepted {path}");
        }
        assert!(validate_relative_path("docs/指南.txt").is_ok());

        let excludes = compile_excludes(&["cache".to_string()]).unwrap();
        assert!(is_excluded(&excludes, "cache/a.bin"));
        assert!(!is_excluded(&excludes, "docs/cache/a.bin"));
    }

    #[test]
    fn hashless_remote_file_requires_explicit_overwrite() {
        let mut local = LocalManifest::default();
        local.entries.insert("a.bin".into(), local_file("a.bin", 1));
        let mut remote = RemoteManifest::default();
        remote
            .entries
            .insert("a.bin".into(), remote_file("a.bin", 9, None));
        assert!(plan_pull(&local, &remote, false, false, &[])
            .unwrap()
            .has_conflicts());
        assert!(matches!(
            plan_pull(&local, &remote, false, true, &[])
                .unwrap()
                .actions[0],
            PullAction::ReplaceFile { .. }
        ));
    }
}
