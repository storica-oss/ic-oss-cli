use bytes::{Bytes, BytesMut};
use candid::{CandidType, Principal};
use ic_agent::Agent;
use ic_oss_types::{
    batch::*, bucket::*, entry::*, file::*, folder::*, format_error, gc::*, reader::*, storage::*,
    upload::*,
};
use serde::{Deserialize, Serialize};
use serde_bytes::{ByteArray, ByteBuf};
use sha3::{Digest, Sha3_256};
use std::{collections::BTreeSet, sync::Arc};
use tokio::io::AsyncRead;
use tokio::sync::{mpsc, RwLock, Semaphore};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, FramedRead};

use crate::agent::{query_call, update_call};

type AccessTokenProvider = Arc<dyn Fn() -> Result<Option<ByteBuf>, String> + Send + Sync>;

#[derive(Clone)]
pub struct Client {
    concurrency: u8,
    agent: Arc<Agent>,
    bucket: Principal,
    set_readonly: bool,
    access_token: Option<ByteBuf>,
    access_token_provider: Option<AccessTokenProvider>,
}

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct UploadFileChunksResult {
    pub id: u32,
    pub filled: u64,
    pub uploaded_chunks: BTreeSet<u32>,
    pub error: Option<String>, // if any error occurs during upload
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Progress {
    pub filled: u64,
    pub size: Option<u64>, // total size of file, may be unknown
    pub chunk_index: u32,
    pub concurrency: u8,
}

impl Client {
    pub fn new(agent: Arc<Agent>, bucket: Principal) -> Client {
        Client {
            concurrency: 16,
            agent,
            bucket,
            set_readonly: false,
            access_token: None,
            access_token_provider: None,
        }
    }

    pub fn set_concurrency(&mut self, concurrency: u8) {
        if concurrency > 0 && concurrency <= 64 {
            self.concurrency = concurrency;
        }
    }

    pub fn set_readonly(&mut self, readonly: bool) {
        self.set_readonly = readonly;
    }

    pub fn set_access_token(&mut self, access_token: Option<ByteBuf>) {
        self.access_token = access_token;
        self.access_token_provider = None;
    }

    pub fn with_access_token(mut self, access_token: Option<ByteBuf>) -> Self {
        self.set_access_token(access_token);
        self
    }

    /// Sets a provider evaluated before every authenticated bucket request.
    ///
    /// Calling [`Self::set_access_token`] later removes the provider.
    pub fn set_access_token_provider<F>(&mut self, provider: F)
    where
        F: Fn() -> Result<Option<ByteBuf>, String> + Send + Sync + 'static,
    {
        self.access_token_provider = Some(Arc::new(provider));
    }

    /// Builds a client with a provider evaluated before every authenticated bucket request.
    pub fn with_access_token_provider<F>(mut self, provider: F) -> Self
    where
        F: Fn() -> Result<Option<ByteBuf>, String> + Send + Sync + 'static,
    {
        self.set_access_token_provider(provider);
        self
    }

    fn current_access_token(&self) -> Result<Option<ByteBuf>, String> {
        match &self.access_token_provider {
            Some(provider) => provider(),
            None => Ok(self.access_token.clone()),
        }
    }

    fn current_sync_access_token(&self) -> Result<Option<ByteBuf>, SyncError> {
        self.current_access_token().map_err(SyncError::Internal)
    }

    /// the caller of agent should be canister controller
    pub async fn admin_set_managers(&self, args: BTreeSet<Principal>) -> Result<(), String> {
        update_call(&self.agent, &self.bucket, "admin_set_managers", (args,)).await?
    }

    /// the caller of agent should be canister controller
    pub async fn admin_set_auditors(&self, args: BTreeSet<Principal>) -> Result<(), String> {
        update_call(&self.agent, &self.bucket, "admin_set_auditors", (args,)).await?
    }

    /// the caller of agent should be canister controller
    pub async fn admin_update_bucket(&self, args: UpdateBucketInput) -> Result<(), String> {
        update_call(&self.agent, &self.bucket, "admin_update_bucket", (args,)).await?
    }

    /// Sets or clears the only principal allowed to mutate Reader Grants.
    /// The caller must be a canister controller.
    pub async fn admin_set_reader_authority(
        &self,
        authority: Option<Principal>,
    ) -> Result<(), String> {
        update_call(
            &self.agent,
            &self.bucket,
            "admin_set_reader_authority",
            (authority,),
        )
        .await?
    }

    /// Creates or renews a versioned Reader Grant as the configured authority.
    pub async fn admin_upsert_reader_grant(
        &self,
        input: UpsertReaderGrantInput,
    ) -> Result<Result<ReaderGrant, ReaderGrantError>, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "admin_upsert_reader_grant",
            (input,),
        )
        .await
    }

    /// Revokes a versioned Reader Grant as the configured authority.
    pub async fn admin_revoke_reader_grant(
        &self,
        input: RevokeReaderGrantInput,
    ) -> Result<Result<ReaderGrant, ReaderGrantError>, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "admin_revoke_reader_grant",
            (input,),
        )
        .await
    }

    /// Returns the grant attached to the authenticated agent principal.
    pub async fn get_my_reader_grant(
        &self,
    ) -> Result<Result<Option<ReaderGrant>, ReaderGrantError>, String> {
        query_call(&self.agent, &self.bucket, "get_my_reader_grant", ()).await
    }

    pub async fn admin_migrate_directory_storage(
        &self,
        input: MigrateDirectoryStorageInput,
    ) -> Result<MigrateDirectoryStorageOutput, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "admin_migrate_directory_storage",
            (input,),
        )
        .await
    }

    pub async fn admin_retry_directory_migration(
        &self,
    ) -> Result<MigrateDirectoryStorageOutput, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "admin_retry_directory_migration",
            (),
        )
        .await
    }

    pub async fn get_bucket_info(&self) -> Result<BucketInfo, String> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_bucket_info",
            (self.current_access_token()?,),
        )
        .await?
    }

    pub async fn get_capabilities(&self) -> Result<BucketCapabilities, String> {
        query_call(&self.agent, &self.bucket, "get_capabilities", ()).await
    }

    pub async fn get_file_info(&self, id: u32) -> Result<FileInfo, String> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_file_info",
            (id, self.current_access_token()?),
        )
        .await?
    }

    /// Returns the immutable descriptor used by protected media readers.
    pub async fn get_file_descriptor(&self, id: u32) -> Result<FileDescriptor, String> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_file_descriptor",
            (id, self.current_access_token()?),
        )
        .await?
    }

    /// Reads a chunk only if the file is still at the expected generation.
    pub async fn read_file_chunk(&self, input: ReadFileChunkInput) -> Result<ByteBuf, String> {
        query_call(
            &self.agent,
            &self.bucket,
            "read_file_chunk",
            (input, self.current_access_token()?),
        )
        .await?
    }

    /// Reads a bounded range only if the file is still at the expected generation.
    pub async fn read_file_range(&self, input: ReadFileRangeInput) -> Result<ByteBuf, String> {
        query_call(
            &self.agent,
            &self.bucket,
            "read_file_range",
            (input, self.current_access_token()?),
        )
        .await?
    }

    pub async fn get_file_info_by_hash(&self, hash: ByteArray<32>) -> Result<FileInfo, String> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_file_info_by_hash",
            (hash, self.current_access_token()?),
        )
        .await?
    }

    pub async fn get_file_ancestors(&self, id: u32) -> Result<Vec<FolderName>, String> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_file_ancestors",
            (id, self.current_access_token()?),
        )
        .await?
    }

    pub async fn get_file_chunks(
        &self,
        id: u32,
        index: u32,
        take: Option<u32>,
    ) -> Result<Vec<FileChunk>, String> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_file_chunks",
            (id, index, take, self.current_access_token()?),
        )
        .await?
    }

    pub async fn list_files(
        &self,
        parent: u32,
        prev: Option<u32>,
        take: Option<u32>,
    ) -> Result<Vec<FileInfo>, String> {
        query_call(
            &self.agent,
            &self.bucket,
            "list_files",
            (parent, prev, take, self.current_access_token()?),
        )
        .await?
    }

    pub async fn get_folder_info(&self, id: u32) -> Result<FolderInfo, String> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_folder_info",
            (id, self.current_access_token()?),
        )
        .await?
    }

    pub async fn get_entry(&self, input: GetEntryInput) -> Result<Option<EntryInfoV2>, SyncError> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_entry",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn list_entries(
        &self,
        input: ListEntriesInput,
    ) -> Result<ListEntriesOutput, SyncError> {
        query_call(
            &self.agent,
            &self.bucket,
            "list_entries",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn get_directory_storage_health(&self) -> Result<DirectoryStorageHealth, SyncError> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_directory_storage_health",
            (self.current_sync_access_token()?,),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn get_subtree_manifest(
        &self,
        input: SubtreeManifestInput,
    ) -> Result<SubtreeManifestOutput, SyncError> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_subtree_manifest",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn get_folder_ancestors(&self, id: u32) -> Result<Vec<FolderName>, String> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_folder_ancestors",
            (id, self.current_access_token()?),
        )
        .await?
    }

    pub async fn list_folders(
        &self,
        parent: u32,
        prev: Option<u32>,
        take: Option<u32>,
    ) -> Result<Vec<FolderInfo>, String> {
        query_call(
            &self.agent,
            &self.bucket,
            "list_folders",
            (parent, prev, take, self.current_access_token()?),
        )
        .await?
    }

    pub async fn create_file(&self, file: CreateFileInput) -> Result<CreateFileOutput, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "create_file",
            (file, self.current_access_token()?),
        )
        .await?
    }

    pub async fn batch_create_small_files(
        &self,
        input: BatchCreateSmallFilesInput,
    ) -> Result<BatchCreateSmallFilesOutput, SyncError> {
        update_call(
            &self.agent,
            &self.bucket,
            "batch_create_small_files",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn update_file_chunk(
        &self,
        input: UpdateFileChunkInput,
    ) -> Result<UpdateFileChunkOutput, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "update_file_chunk",
            (input, self.current_access_token()?),
        )
        .await?
    }

    pub async fn update_file_info(
        &self,
        input: UpdateFileInput,
    ) -> Result<UpdateFileOutput, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "update_file_info",
            (input, self.current_access_token()?),
        )
        .await?
    }

    pub async fn move_file(&self, input: MoveInput) -> Result<UpdateFileOutput, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "move_file",
            (input, self.current_access_token()?),
        )
        .await?
    }

    pub async fn delete_file(&self, id: u32) -> Result<bool, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "delete_file",
            (id, self.current_access_token()?),
        )
        .await?
    }

    pub async fn batch_delete_subfiles(
        &self,
        parent: u32,
        ids: BTreeSet<u32>,
    ) -> Result<Vec<u32>, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "batch_delete_subfiles",
            (parent, ids, self.current_access_token()?),
        )
        .await?
    }

    pub async fn create_folder(
        &self,
        input: CreateFolderInput,
    ) -> Result<CreateFolderOutput, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "create_folder",
            (input, self.current_access_token()?),
        )
        .await?
    }

    pub async fn ensure_folder(
        &self,
        input: EnsureFolderInput,
    ) -> Result<EnsureFolderOutput, SyncError> {
        update_call(
            &self.agent,
            &self.bucket,
            "ensure_folder",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn batch_ensure_folders(
        &self,
        input: BatchEnsureFoldersInput,
    ) -> Result<BatchEnsureFoldersOutput, SyncError> {
        update_call(
            &self.agent,
            &self.bucket,
            "batch_ensure_folders",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn delete_entry_if(&self, input: DeleteEntryIfInput) -> Result<bool, SyncError> {
        update_call(
            &self.agent,
            &self.bucket,
            "delete_entry_if",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn begin_upload(
        &self,
        input: BeginUploadInput,
    ) -> Result<BeginUploadOutput, SyncError> {
        update_call(
            &self.agent,
            &self.bucket,
            "begin_upload",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn upload_chunk(
        &self,
        input: UploadChunkInput,
    ) -> Result<UploadChunkOutput, SyncError> {
        update_call(
            &self.agent,
            &self.bucket,
            "upload_chunk",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn get_upload_status(
        &self,
        input: GetUploadStatusInput,
    ) -> Result<UploadStatusOutput, SyncError> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_upload_status",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn get_upload_health(&self) -> Result<UploadHealth, SyncError> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_upload_health",
            (self.current_sync_access_token()?,),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn renew_upload(
        &self,
        input: RenewUploadInput,
    ) -> Result<RenewUploadOutput, SyncError> {
        update_call(
            &self.agent,
            &self.bucket,
            "renew_upload",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn commit_upload(
        &self,
        input: CommitUploadInput,
    ) -> Result<CommitUploadOutput, SyncError> {
        update_call(
            &self.agent,
            &self.bucket,
            "commit_upload",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn abort_upload(&self, input: AbortUploadInput) -> Result<bool, SyncError> {
        update_call(
            &self.agent,
            &self.bucket,
            "abort_upload",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn get_gc_health(&self) -> Result<GcHealth, SyncError> {
        query_call(
            &self.agent,
            &self.bucket,
            "get_gc_health",
            (self.current_sync_access_token()?,),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn collect_garbage(
        &self,
        input: CollectGarbageInput,
    ) -> Result<CollectGarbageOutput, SyncError> {
        update_call(
            &self.agent,
            &self.bucket,
            "collect_garbage",
            (input, self.current_sync_access_token()?),
        )
        .await
        .map_err(SyncError::Internal)?
    }

    pub async fn update_folder_info(
        &self,
        input: UpdateFolderInput,
    ) -> Result<UpdateFolderOutput, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "update_folder_info",
            (input, self.current_access_token()?),
        )
        .await?
    }

    pub async fn move_folder(&self, input: MoveInput) -> Result<UpdateFolderOutput, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "move_folder",
            (input, self.current_access_token()?),
        )
        .await?
    }

    pub async fn delete_folder(&self, id: u32) -> Result<bool, String> {
        update_call(
            &self.agent,
            &self.bucket,
            "delete_folder",
            (id, self.current_access_token()?),
        )
        .await?
    }

    pub async fn upload<T, F>(
        &self,
        stream: T,
        mut file: CreateFileInput,
        on_progress: F,
    ) -> Result<UploadFileChunksResult, String>
    where
        T: AsyncRead,
        F: Fn(Progress),
    {
        if let Some(size) = file.size {
            if size <= MAX_FILE_SIZE_PER_CALL {
                // upload a small file in one request
                let content = try_read_all(stream, size as u32).await?;
                if file.hash.is_none() {
                    let mut hasher = Sha3_256::new();
                    hasher.update(&content);
                    let hash: [u8; 32] = hasher.finalize().into();
                    file.hash = Some(hash.into());
                }
                let apply_readonly_after_create =
                    prepare_inline_content(&mut file, &content, self.set_readonly);
                let res = self.create_file(file).await?;
                if apply_readonly_after_create {
                    self.update_file_info(UpdateFileInput {
                        id: res.id,
                        status: Some(1),
                        ..Default::default()
                    })
                    .await?;
                }

                on_progress(Progress {
                    filled: size,
                    size: Some(size),
                    chunk_index: 0,
                    concurrency: 1,
                });
                return Ok(UploadFileChunksResult {
                    id: res.id,
                    filled: size,
                    uploaded_chunks: BTreeSet::new(),
                    error: None,
                });
            }
        }

        // create file
        let hash = file.hash;
        let size = file.size;
        let res = self.create_file(file).await?;
        let res = self
            .upload_chunks(stream, res.id, size, hash, &BTreeSet::new(), on_progress)
            .await;
        Ok(res)
    }

    pub async fn upload_chunks<T, F>(
        &self,
        stream: T,
        id: u32,
        size: Option<u64>,
        hash: Option<ByteArray<32>>,
        exclude_chunks: &BTreeSet<u32>,
        on_progress: F,
    ) -> UploadFileChunksResult
    where
        T: AsyncRead,
        F: Fn(Progress),
    {
        // upload chunks
        let bucket = self.bucket;
        let has_hash = hash.is_some();
        let mut frames = Box::pin(FramedRead::new(stream, ChunksCodec::new(CHUNK_SIZE)));
        let (tx, mut rx) = mpsc::channel::<Result<Progress, String>>(self.concurrency as usize);
        let output = Arc::new(RwLock::new(UploadFileChunksResult {
            id,
            filled: 0,
            uploaded_chunks: exclude_chunks.clone(),
            error: None,
        }));

        let uploading_loop = async {
            let mut index = 0;
            let mut hasher = Sha3_256::new();
            let semaphore = Arc::new(Semaphore::new(self.concurrency as usize));

            loop {
                let access_token = self.current_access_token()?;
                let tx1 = tx.clone();
                let output = output.clone();
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(format_error)?;
                let concurrency = (self.concurrency as usize - semaphore.available_permits()) as u8;

                match frames.next().await {
                    None => {
                        drop(tx);
                        semaphore.close();
                        return Ok(Into::<[u8; 32]>::into(hasher.finalize()));
                    }
                    Some(Err(err)) => {
                        drop(tx);
                        semaphore.close();
                        return Err(err.to_string());
                    }
                    Some(Ok(chunk)) => {
                        let chunk_index = index;
                        index += 1;
                        let chunk_len = chunk.len() as u32;

                        if !has_hash {
                            hasher.update(&chunk);
                        }

                        if exclude_chunks.contains(&chunk_index) {
                            let mut r = output.write().await;
                            r.filled += chunk_len as u64;
                            on_progress(Progress {
                                filled: r.filled,
                                size,
                                chunk_index,
                                concurrency: 0,
                            });
                            drop(permit);
                            continue;
                        }

                        let agent = self.agent.clone();
                        tokio::spawn(async move {
                            let res = async {
                                let out: Result<UpdateFileChunkOutput, String> = update_call(
                                    &agent,
                                    &bucket,
                                    "update_file_chunk",
                                    (
                                        UpdateFileChunkInput {
                                            id,
                                            chunk_index,
                                            content: ByteBuf::from(chunk.to_vec()),
                                        },
                                        &access_token,
                                    ),
                                )
                                .await?;
                                let out = out?;
                                Ok(Progress {
                                    filled: out.filled,
                                    size,
                                    chunk_index,
                                    concurrency,
                                })
                            }
                            .await;

                            if res.is_ok() {
                                let mut r = output.write().await;
                                r.filled += chunk_len as u64;
                                r.uploaded_chunks.insert(chunk_index);
                                drop(permit);
                            }
                            let _ = tx1.send(res).await;
                        });
                    }
                }
            }
        };

        let uploading_result = async {
            while let Some(res) = rx.recv().await {
                match res {
                    Ok(progress) => {
                        on_progress(progress);
                    }
                    Err(err) => return Err(err),
                }
            }

            Ok(())
        };

        let result = async {
            let (hash_new, _) = futures::future::try_join(uploading_loop, uploading_result).await?;

            // commit file
            let _ = self
                .update_file_info(UpdateFileInput {
                    id,
                    hash: Some(hash.unwrap_or(hash_new.into())),
                    status: if self.set_readonly { Some(1) } else { None },
                    size,
                    ..Default::default()
                })
                .await?;
            Ok::<(), String>(())
        }
        .await;

        let mut output = output.read().await.to_owned();
        if let Err(err) = result {
            output.error = Some(err);
        }

        output
    }
}

fn prepare_inline_content(file: &mut CreateFileInput, content: &Bytes, set_readonly: bool) -> bool {
    let is_empty = content.is_empty();
    file.content = (!is_empty).then(|| ByteBuf::from(content.to_vec()));
    // create_file applies status while processing inline content. Empty files have no inline
    // content, so readonly status must be applied in a follow-up metadata update.
    file.status = if set_readonly && !is_empty {
        Some(1)
    } else {
        None
    };
    set_readonly && is_empty
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn access_token_provider_refreshes_and_fixed_setter_clears_it() {
        let agent = Agent::builder()
            .with_url("http://127.0.0.1:1")
            .build()
            .expect("build test agent");
        let token = Arc::new(Mutex::new(Some(ByteBuf::from(vec![1]))));
        let provider_token = Arc::clone(&token);
        let mut client = Client::new(Arc::new(agent), Principal::anonymous());
        client.set_access_token_provider(move || {
            provider_token
                .lock()
                .map(|token| token.clone())
                .map_err(|_| "token provider lock is poisoned".to_string())
        });

        assert_eq!(client.current_access_token().unwrap(), Some(vec![1].into()));
        *token.lock().expect("replace provider token") = Some(ByteBuf::from(vec![2]));
        assert_eq!(client.current_access_token().unwrap(), Some(vec![2].into()));

        client.set_access_token(Some(ByteBuf::from(vec![3])));
        *token.lock().expect("replace stale provider token") = Some(ByteBuf::from(vec![4]));
        assert_eq!(client.current_access_token().unwrap(), Some(vec![3].into()));
    }

    #[test]
    fn empty_inline_content_is_created_without_an_empty_blob() {
        let mut file = CreateFileInput::default();
        let apply_readonly_after_create = prepare_inline_content(&mut file, &Bytes::new(), true);

        assert!(file.content.is_none());
        assert!(file.status.is_none());
        assert!(apply_readonly_after_create);
    }

    #[test]
    fn non_empty_inline_content_applies_readonly_during_create() {
        let mut file = CreateFileInput::default();
        let apply_readonly_after_create =
            prepare_inline_content(&mut file, &Bytes::from_static(b"data"), true);

        assert_eq!(
            file.content.as_ref().map(|content| content.as_ref()),
            Some(b"data".as_slice())
        );
        assert_eq!(file.status, Some(1));
        assert!(!apply_readonly_after_create);
    }

    #[tokio::test]
    async fn zero_sized_read_returns_empty_content() {
        let content = try_read_all(tokio::io::empty(), 0).await.unwrap();
        assert!(content.is_empty());
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ChunksCodec(u32);

impl ChunksCodec {
    pub fn new(len: u32) -> ChunksCodec {
        ChunksCodec(len)
    }
}

impl Decoder for ChunksCodec {
    type Item = Bytes;
    type Error = tokio::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() >= self.0 as usize {
            Ok(Some(BytesMut::freeze(buf.split_to(self.0 as usize))))
        } else {
            Ok(None)
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.is_empty() {
            Ok(None)
        } else {
            let len = buf.len();
            Ok(Some(BytesMut::freeze(buf.split_to(len))))
        }
    }
}

async fn try_read_all<T: AsyncRead>(stream: T, size: u32) -> Result<Bytes, String> {
    if size == 0 {
        return Ok(Bytes::new());
    }
    let mut frames = Box::pin(FramedRead::new(stream, ChunksCodec::new(size)));

    let res = frames.next().await.ok_or("no bytes to read".to_string())?;
    if frames.next().await.is_some() {
        return Err("too many bytes to read".to_string());
    }
    let res = res.map_err(format_error)?;
    if res.len() != size as usize {
        return Err("insufficient bytes to read".to_string());
    }
    Ok(res)
}
