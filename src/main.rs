use base64::{engine::general_purpose, Engine as _};
use candid::{pretty::candid::value::pp_value, CandidType, IDLValue, Principal};
use clap::{Parser, Subcommand};
use ic_agent::{
    identity::{AnonymousIdentity, BasicIdentity, Secp256k1Identity},
    Identity,
};
use ic_oss::agent::build_agent;
use ic_oss_types::{
    cluster::AddWasmInput,
    entry::MigrationState,
    file::{MoveInput, CHUNK_SIZE},
    folder::CreateFolderInput,
    format_error,
    gc::CollectGarbageInput,
    reader::{RevokeReaderGrantInput, UpsertReaderGrantInput},
    storage::MigrateDirectoryStorageInput,
};
use ring::{rand, signature::Ed25519KeyPair};
use serde_bytes::{ByteArray, ByteBuf};
use sha3::{Digest, Sha3_256};
use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

mod file;
mod sync;
mod transcode;

use file::upload_file;

static IC_HOST: &str = "https://icp-api.io";
const MAX_ACCESS_TOKEN_SIZE: u64 = 64 * 1024;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// The user identity to run this command as.
    #[arg(short, long, value_name = "PEM_FILE", default_value = "Anonymous")]
    identity: String,

    /// Read a bearer access token from a protected raw-COSE or `base64:` file.
    #[arg(long, global = true, value_name = "TOKEN_FILE")]
    access_token_file: Option<PathBuf>,

    /// The host to connect to. it will be set to "https://icp-api.io" with option '--ic'
    #[arg(long, default_value = "http://127.0.0.1:4943")]
    host: String,

    /// Use the ic network
    #[arg(long, default_value = "false")]
    ic: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

impl Cli {
    async fn bucket(
        &self,
        identity: Arc<dyn Identity>,
        ic: &bool,
        bucket: &str,
    ) -> Result<ic_oss::bucket::Client, String> {
        let is_ic = *ic || self.ic;
        let host = if is_ic { IC_HOST } else { self.host.as_str() };
        let agent = build_agent(host, identity).await?;
        let bucket = Principal::from_text(bucket).map_err(format_error)?;
        let access_token = load_access_token(self.access_token_file.as_deref())?;
        let mut client =
            ic_oss::bucket::Client::new(Arc::new(agent), bucket).with_access_token(access_token);
        if let Some(path) = self.access_token_file.clone() {
            client.set_access_token_provider(move || load_access_token(Some(&path)));
        }
        Ok(client)
    }

    async fn cluster(
        &self,
        identity: Arc<dyn Identity>,
        ic: &bool,
        cluster: &str,
    ) -> Result<ic_oss::cluster::Client, String> {
        let is_ic = *ic || self.ic;
        let host = if is_ic { IC_HOST } else { self.host.as_str() };
        let agent = build_agent(host, identity).await?;
        let cluster = Principal::from_text(cluster).map_err(format_error)?;
        Ok(ic_oss::cluster::Client::new(Arc::new(agent), cluster))
    }
}

#[derive(Subcommand)]
pub enum Commands {
    Identity {
        /// file path
        #[arg(long)]
        path: Option<String>,
        /// create a identity
        #[arg(long)]
        new: bool,
    },
    /// Add a bucket wasm to cluster
    ClusterAddWasm {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        cluster: String,

        /// wasm file path
        #[arg(long)]
        path: String,

        /// description
        #[arg(short, long, default_value = "")]
        description: String,

        /// previous wasm hash
        #[arg(long)]
        prev_hash: Option<String>,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Add a folder to a bucket
    Add {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// parent folder id
        #[arg(short, long, default_value = "0")]
        parent: u32,

        /// folder name
        #[arg(short, long)]
        name: String,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Uploads a file to a bucket
    #[command(visible_alias = "upload")]
    Put {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// parent folder id
        #[arg(short, long, default_value = "0")]
        parent: u32,

        /// file path
        #[arg(long)]
        path: String,

        /// retry times
        #[arg(long, default_value = "3")]
        retry: u8,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,

        /// digest algorithm, default is SHA3-256
        #[arg(long, default_value = "SHA3-256")]
        digest: String,
    },
    /// Runs a resumable local Personal Hub transcode job through ffmpeg/ffprobe
    TranscodeRun {
        /// Personal Hub canister
        #[arg(long, value_name = "CANISTER")]
        hub: String,

        /// Transcode job ID created by the Hub Owner
        #[arg(long)]
        job_id: u64,

        /// Local source file whose size and SHA3-256 must match the Hub job snapshot
        #[arg(long)]
        source: PathBuf,

        /// Registered output Bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// Output Bucket folder
        #[arg(short, long, default_value = "0")]
        parent: u32,

        /// Persistent directory for output files, upload journal, and Owner approval JSON
        #[arg(long)]
        work_dir: PathBuf,

        /// ffmpeg executable
        #[arg(long, default_value = "ffmpeg")]
        ffmpeg: PathBuf,

        /// ffprobe executable
        #[arg(long, default_value = "ffprobe")]
        ffprobe: PathBuf,

        /// retry times for interrupted Bucket uploads
        #[arg(long, default_value = "3")]
        retry: u8,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Applies a transcode worker report as the independent Personal Hub Owner verifier
    TranscodeApprove {
        /// Personal Hub canister; must match the report
        #[arg(long, value_name = "CANISTER")]
        hub: String,

        /// `owner-approval.json` generated by `transcode-run`
        #[arg(long)]
        report: PathBuf,

        /// ffprobe executable used by the Owner to independently inspect every output
        #[arg(long, default_value = "ffprobe")]
        ffprobe: PathBuf,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Deletes journal-owned remote outputs after a transcode job fails or is cancelled
    TranscodeCleanup {
        /// Personal Hub canister
        #[arg(long, value_name = "CANISTER")]
        hub: String,

        /// Failed or cancelled transcode job ID
        #[arg(long)]
        job_id: u64,

        /// Output Bucket recorded by the worker journal
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// Persistent directory containing `worker-journal.json`
        #[arg(long)]
        work_dir: PathBuf,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Compares a local directory with a bucket folder
    Sync {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// local directory whose contents should be synchronized
        #[arg(long)]
        path: PathBuf,

        /// target folder id
        #[arg(short, long, default_value = "0")]
        parent: u32,

        /// include remote-only entries as delete actions
        #[arg(long, default_value = "false")]
        delete: bool,

        /// include changed remote files as replace actions
        #[arg(long, default_value = "false")]
        overwrite: bool,

        /// print the synchronization plan without modifying remote state
        #[arg(long, default_value = "false")]
        dry_run: bool,

        /// exclude a relative path using glob syntax
        #[arg(long)]
        exclude: Vec<String>,

        /// retry times for interrupted file chunk uploads
        #[arg(long, default_value = "3")]
        retry: u8,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Synchronizes a bucket folder into a local directory
    #[command(visible_alias = "sync-download")]
    Pull {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// local directory that receives the bucket folder contents
        #[arg(long)]
        path: PathBuf,

        /// source folder id
        #[arg(short, long, default_value = "0")]
        parent: u32,

        /// delete local-only files and empty directories
        #[arg(long, default_value = "false")]
        delete: bool,

        /// replace changed local files
        #[arg(long, default_value = "false")]
        overwrite: bool,

        /// print the synchronization plan without modifying local state
        #[arg(long, default_value = "false")]
        dry_run: bool,

        /// exclude a relative path using glob syntax
        #[arg(long)]
        exclude: Vec<String>,

        /// retry times for interrupted file chunk downloads
        #[arg(long, default_value = "3")]
        retry: u8,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Displays bucket sync capabilities and migration state
    BucketCapabilities {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Sets or clears the Hub/service principal authorized to manage Reader Grants
    ReaderAuthority {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// principal authorized to upsert and revoke Reader Grants
        #[arg(
            long,
            value_name = "PRINCIPAL",
            required_unless_present = "clear",
            conflicts_with = "clear"
        )]
        authority: Option<String>,

        /// clear the current Reader Grant authority
        #[arg(long, default_value = "false")]
        clear: bool,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Creates or renews a versioned Reader Grant
    ReaderGrantUpsert {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// reader principal
        #[arg(long, value_name = "PRINCIPAL")]
        subject: String,

        /// absolute expiry time in Unix milliseconds; omit for permanent access
        #[arg(long)]
        expires_at_ms: Option<u64>,

        /// monotonically increasing entitlement version
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        version: u64,

        /// stable idempotency key encoded as hexadecimal bytes (1–64 bytes)
        #[arg(long, value_name = "HEX")]
        request_id: String,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Revokes a versioned Reader Grant
    ReaderGrantRevoke {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// reader principal
        #[arg(long, value_name = "PRINCIPAL")]
        subject: String,

        /// monotonically increasing entitlement version
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        version: u64,

        /// stable idempotency key encoded as hexadecimal bytes (1–64 bytes)
        #[arg(long, value_name = "HEX")]
        request_id: String,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Displays the Reader Grant for the current authenticated principal
    ReaderGrantSelf {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Displays directory storage, upload session, and garbage collection health
    BucketHealth {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Runs bounded garbage collection for deleted files and expired uploads
    BucketCollectGarbage {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// maximum chunk slots processed by each canister call
        #[arg(long, default_value = "1024", value_parser = clap::value_parser!(u32).range(1..=1024))]
        max_chunks: u32,

        /// keep submitting bounded collection calls until the backlog is empty
        #[arg(long, default_value = "false")]
        until_clean: bool,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Migrates legacy directory storage to the v2 stable indexes
    BucketMigrateDirectory {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// maximum entries processed by each canister call
        #[arg(long, default_value = "1000", value_parser = clap::value_parser!(u16).range(1..=1000))]
        max_items: u16,

        /// keep submitting bounded migration calls until the bucket becomes Ready
        #[arg(long, default_value = "false")]
        until_complete: bool,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Clears a failed directory migration so it can be resumed
    BucketRetryDirectoryMigration {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Downloads an file from a target bucket to the local file system
    Get {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// downloads file by id
        #[arg(long)]
        id: Option<u32>,

        /// downloads file by hash
        #[arg(long)]
        hash: Option<String>,

        /// file path to save
        #[arg(long, default_value = "./")]
        path: String,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,

        /// digest algorithm to verify the file, default is SHA3-256
        #[arg(long, default_value = "SHA3-256")]
        digest: String,
    },
    /// Lists files or folders in a folder
    Ls {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// parent folder id
        #[arg(short, long, default_value = "0")]
        parent: u32,

        /// kind 0: file, 1: folder
        #[arg(short, long, default_value = "0")]
        kind: u8,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Displays information on file, folder, or bucket, including metadata
    Stat {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// file or folder id
        #[arg(long, default_value = "0")]
        id: u32,

        /// kind 0: file, 1: folder, other: bucket
        #[arg(short, long, default_value = "0")]
        kind: u8,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,

        /// Displays file information by file hash
        #[arg(long)]
        hash: Option<String>,
    },
    /// Removes file or folder from a bucket
    Mv {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// file or folder id
        #[arg(long)]
        id: u32,

        /// file or folder's parent id
        #[arg(long)]
        from: u32,

        /// target folder id
        #[arg(long)]
        to: u32,

        /// kind 0: file, 1: folder
        #[arg(short, long, default_value = "0")]
        kind: u8,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
    /// Removes file or folder from a bucket
    Rm {
        /// bucket
        #[arg(short, long, value_name = "CANISTER")]
        bucket: String,

        /// file or folder id
        #[arg(long)]
        id: u32,

        /// kind 0: file, 1: folder
        #[arg(short, long, default_value = "0")]
        kind: u8,

        /// Use the ic network
        #[arg(long, default_value = "false")]
        ic: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let cli = Cli::parse();
    let identity = load_identity(&cli.identity).map_err(format_error)?;
    let identity = Arc::new(identity);

    match &cli.command {
        Some(Commands::Identity { new, path }) => {
            if !new {
                let principal = identity.sender()?;
                println!("principal: {}", principal);
                return Ok(());
            }

            let doc =
                Ed25519KeyPair::generate_pkcs8(&rand::SystemRandom::new()).map_err(format_error)?;

            let doc = pem::Pem::new("PRIVATE KEY", doc.as_ref());
            let doc = pem::encode(&doc);
            let id = BasicIdentity::from_pem(doc.as_bytes()).map_err(format_error)?;
            let principal = id.sender()?;

            let file = match path {
                Some(path) => Path::new(path).to_path_buf(),
                None => PathBuf::from(format!("{}.pem", principal)),
            };

            if file.try_exists().unwrap_or_default() {
                Err(format!("file already exists: {:?}", file))?;
            }

            std::fs::write(&file, doc.as_bytes()).map_err(format_error)?;
            println!("principal: {}", principal);
            println!("new identity: {}", file.to_str().unwrap());
            return Ok(());
        }

        Some(Commands::ClusterAddWasm {
            cluster,
            path,
            description,
            prev_hash,
            ic,
        }) => {
            let cli = cli.cluster(identity, ic, cluster).await?;
            let wasm = std::fs::read(path).map_err(format_error)?;
            let prev_hash = prev_hash.as_ref().map(|s| parse_file_hash(s)).transpose()?;
            cli.admin_add_wasm(
                AddWasmInput {
                    wasm: ByteBuf::from(wasm),
                    description: description.to_owned(),
                },
                prev_hash,
            )
            .await
            .map_err(format_error)?;
            return Ok(());
        }

        Some(Commands::Add {
            bucket,
            parent,
            name,
            ic,
        }) => {
            let cli = cli.bucket(identity, ic, bucket).await?;
            let folder = cli
                .create_folder(CreateFolderInput {
                    parent: *parent,
                    name: name.clone(),
                })
                .await
                .map_err(format_error)?;
            pretty_println(&folder)?;
            return Ok(());
        }

        Some(Commands::Put {
            bucket,
            parent,
            path,
            retry,
            ic,
            digest,
        }) => {
            if digest != "SHA3-256" {
                Err("unsupported digest algorithm".to_string())?;
            }
            if *retry > file::MAX_UPLOAD_RETRIES {
                return Err(format!(
                    "retry count {} exceeds maximum {}",
                    retry,
                    file::MAX_UPLOAD_RETRIES
                ));
            }
            let cli = cli.bucket(identity, ic, bucket).await?;
            let info = cli.get_bucket_info().await.map_err(format_error)?;
            upload_file(&cli, info.enable_hash_index, *parent, path, *retry).await?;

            return Ok(());
        }

        Some(Commands::TranscodeRun {
            hub,
            job_id,
            source,
            bucket,
            parent,
            work_dir,
            ffmpeg,
            ffprobe,
            retry,
            ic,
        }) => {
            if *retry > file::MAX_UPLOAD_RETRIES {
                return Err(format!(
                    "retry count {} exceeds maximum {}",
                    retry,
                    file::MAX_UPLOAD_RETRIES
                ));
            }
            let is_ic = *ic || cli.ic;
            let host = if is_ic { IC_HOST } else { cli.host.as_str() };
            let agent = build_agent(host, identity.clone()).await?;
            let hub = Principal::from_text(hub).map_err(format_error)?;
            let output_bucket = Principal::from_text(bucket).map_err(format_error)?;
            let bucket_client = cli.bucket(identity, ic, bucket).await?;
            let report = transcode::run_worker(
                &agent,
                &bucket_client,
                &transcode::WorkerOptions {
                    hub,
                    job_id: *job_id,
                    source: source.clone(),
                    bucket: output_bucket,
                    parent: *parent,
                    work_dir: work_dir.clone(),
                    ffmpeg: ffmpeg.clone(),
                    ffprobe: ffprobe.clone(),
                    retry: *retry,
                },
            )
            .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(format_error)?
            );
            return Ok(());
        }

        Some(Commands::TranscodeApprove {
            hub,
            report,
            ffprobe,
            ic,
        }) => {
            let is_ic = *ic || cli.ic;
            let host = if is_ic { IC_HOST } else { cli.host.as_str() };
            let agent = build_agent(host, identity).await?;
            let hub = Principal::from_text(hub).map_err(format_error)?;
            let job = transcode::approve_report(&agent, hub, report, ffprobe).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&job).map_err(format_error)?
            );
            return Ok(());
        }

        Some(Commands::TranscodeCleanup {
            hub,
            job_id,
            bucket,
            work_dir,
            ic,
        }) => {
            let is_ic = *ic || cli.ic;
            let host = if is_ic { IC_HOST } else { cli.host.as_str() };
            let agent = build_agent(host, identity.clone()).await?;
            let hub = Principal::from_text(hub).map_err(format_error)?;
            let output_bucket = Principal::from_text(bucket).map_err(format_error)?;
            let bucket_client = cli.bucket(identity, ic, bucket).await?;
            let report = transcode::cleanup_outputs(
                &agent,
                &bucket_client,
                &transcode::CleanupOptions {
                    hub,
                    job_id: *job_id,
                    bucket: output_bucket,
                    work_dir: work_dir.clone(),
                },
            )
            .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(format_error)?
            );
            return Ok(());
        }

        Some(Commands::Sync {
            bucket,
            path,
            parent,
            delete,
            overwrite,
            dry_run,
            exclude,
            retry,
            ic,
        }) => {
            if *retry > file::MAX_UPLOAD_RETRIES {
                return Err(format!(
                    "retry count {} exceeds maximum {}",
                    retry,
                    file::MAX_UPLOAD_RETRIES
                ));
            }

            let cli = cli.bucket(identity, ic, bucket).await?;
            let prepared = sync::prepare_sync(
                &cli,
                sync::SyncOptions {
                    bucket: bucket.clone(),
                    local_root: path.clone(),
                    remote_parent: *parent,
                    delete: *delete,
                    overwrite: *overwrite,
                    excludes: exclude.clone(),
                },
            )
            .await?;
            sync::print_plan(&prepared.plan);
            if prepared.plan.has_conflicts() {
                return Err("sync plan contains conflicts".to_string());
            }
            if !dry_run {
                sync::execute_sync(&cli, &prepared, *retry).await?;
            }
            return Ok(());
        }

        Some(Commands::Pull {
            bucket,
            path,
            parent,
            delete,
            overwrite,
            dry_run,
            exclude,
            retry,
            ic,
        }) => {
            if *retry > sync::MAX_DOWNLOAD_RETRIES {
                return Err(format!(
                    "retry count {} exceeds maximum {}",
                    retry,
                    sync::MAX_DOWNLOAD_RETRIES
                ));
            }
            let client = cli.bucket(identity, ic, bucket).await?;
            let prepared = sync::prepare_pull(
                &client,
                sync::PullOptions {
                    local_root: path.clone(),
                    remote_parent: *parent,
                    delete: *delete,
                    overwrite: *overwrite,
                    excludes: exclude.clone(),
                },
            )
            .await?;
            sync::print_pull_plan(&prepared.plan);
            if prepared.plan.has_conflicts() {
                return Err("download sync plan contains conflicts".to_string());
            }
            if !dry_run {
                sync::execute_pull(&client, &prepared, *retry).await?;
            }
            return Ok(());
        }

        Some(Commands::BucketCapabilities { bucket, ic }) => {
            let client = cli.bucket(identity, ic, bucket).await?;
            let capabilities = client.get_capabilities().await?;
            print_candid_section("capabilities", &capabilities)?;
            return Ok(());
        }

        Some(Commands::ReaderAuthority {
            bucket,
            authority,
            clear,
            ic,
        }) => {
            let client = cli.bucket(identity, ic, bucket).await?;
            let authority = if *clear {
                None
            } else {
                Some(
                    Principal::from_text(
                        authority
                            .as_deref()
                            .ok_or("--authority is required unless --clear is used")?,
                    )
                    .map_err(format_error)?,
                )
            };
            client.admin_set_reader_authority(authority).await?;
            match authority {
                Some(principal) => println!("reader authority: {principal}"),
                None => println!("reader authority cleared"),
            }
            return Ok(());
        }

        Some(Commands::ReaderGrantUpsert {
            bucket,
            subject,
            expires_at_ms,
            version,
            request_id,
            ic,
        }) => {
            let client = cli.bucket(identity, ic, bucket).await?;
            let grant = client
                .admin_upsert_reader_grant(UpsertReaderGrantInput {
                    subject: Principal::from_text(subject).map_err(format_error)?,
                    expires_at_ms: *expires_at_ms,
                    entitlement_version: *version,
                    request_id: parse_request_id(request_id)?,
                })
                .await?
                .map_err(format_error)?;
            pretty_println(&grant)?;
            return Ok(());
        }

        Some(Commands::ReaderGrantRevoke {
            bucket,
            subject,
            version,
            request_id,
            ic,
        }) => {
            let client = cli.bucket(identity, ic, bucket).await?;
            let grant = client
                .admin_revoke_reader_grant(RevokeReaderGrantInput {
                    subject: Principal::from_text(subject).map_err(format_error)?,
                    entitlement_version: *version,
                    request_id: parse_request_id(request_id)?,
                })
                .await?
                .map_err(format_error)?;
            pretty_println(&grant)?;
            return Ok(());
        }

        Some(Commands::ReaderGrantSelf { bucket, ic }) => {
            let client = cli.bucket(identity, ic, bucket).await?;
            let grant = client.get_my_reader_grant().await?.map_err(format_error)?;
            pretty_println(&grant)?;
            return Ok(());
        }

        Some(Commands::BucketHealth { bucket, ic }) => {
            let client = cli.bucket(identity, ic, bucket).await?;
            let capabilities = client.get_capabilities().await?;
            let directory = client
                .get_directory_storage_health()
                .await
                .map_err(format_error)?;
            let uploads = client.get_upload_health().await.map_err(format_error)?;
            let garbage_collection = client.get_gc_health().await.map_err(format_error)?;

            print_candid_section("capabilities", &capabilities)?;
            print_candid_section("directory_storage", &directory)?;
            print_candid_section("upload_sessions", &uploads)?;
            print_candid_section("garbage_collection", &garbage_collection)?;
            return Ok(());
        }

        Some(Commands::BucketCollectGarbage {
            bucket,
            max_chunks,
            until_clean,
            ic,
        }) => {
            let client = cli.bucket(identity, ic, bucket).await?;
            loop {
                let output = client
                    .collect_garbage(CollectGarbageInput {
                        max_chunks: Some(*max_chunks),
                    })
                    .await
                    .map_err(format_error)?;
                pretty_println(&output)?;

                let clean = output.remaining_items == 0 && output.remaining_chunks == 0;
                if clean || !until_clean {
                    return Ok(());
                }
                if output.processed_chunks == 0 {
                    return Err(
                        "garbage collection made no progress while backlog remains".to_string()
                    );
                }
            }
        }

        Some(Commands::BucketMigrateDirectory {
            bucket,
            max_items,
            until_complete,
            ic,
        }) => {
            let client = cli.bucket(identity, ic, bucket).await?;
            let mut no_progress_calls = 0u8;
            loop {
                let output = client
                    .admin_migrate_directory_storage(MigrateDirectoryStorageInput {
                        max_items: Some(*max_items),
                    })
                    .await?;
                pretty_println(&output)?;

                match output.state {
                    MigrationState::Ready => return Ok(()),
                    MigrationState::Failed => {
                        return Err(format!(
                            "directory migration failed: {}; inspect bucket-health, repair the reported data, then run bucket-retry-directory-migration",
                            output.error.as_deref().unwrap_or("unknown error")
                        ));
                    }
                    MigrationState::Legacy | MigrationState::Migrating if !until_complete => {
                        return Ok(());
                    }
                    MigrationState::Legacy | MigrationState::Migrating => {
                        if output.processed == 0 {
                            no_progress_calls = no_progress_calls.saturating_add(1);
                            if no_progress_calls >= 4 {
                                return Err(
                                    "directory migration made no progress after four calls"
                                        .to_string(),
                                );
                            }
                        } else {
                            no_progress_calls = 0;
                        }
                    }
                }
            }
        }

        Some(Commands::BucketRetryDirectoryMigration { bucket, ic }) => {
            let client = cli.bucket(identity, ic, bucket).await?;
            let output = client.admin_retry_directory_migration().await?;
            pretty_println(&output)?;
            return Ok(());
        }

        Some(Commands::Get {
            bucket,
            id,
            path,
            ic,
            digest,
            hash,
        }) => {
            if digest != "SHA3-256" {
                Err("unsupported digest algorithm".to_string())?;
            }
            let cli = cli.bucket(identity, ic, bucket).await?;
            let info = if let Some(hash) = hash {
                let hash = parse_file_hash(hash)?;
                cli.get_file_info_by_hash(hash)
                    .await
                    .map_err(format_error)?
            } else if let Some(id) = id {
                cli.get_file_info(*id).await.map_err(format_error)?
            } else {
                Err("missing file id or hash".to_string())?
            };

            if info.size != info.filled {
                Err("file not fully uploaded".to_string())?;
            }
            let mut f = Path::new(path).to_path_buf();
            if f.is_dir() {
                f = f.join(info.name);
            }
            let mut file = tokio::fs::File::create_new(&f)
                .await
                .map_err(format_error)?;
            file.set_len(info.size as u64).await.map_err(format_error)?;
            let mut hasher = Sha3_256::new();
            let mut filled = 0usize;
            // TODO: support parallel download
            for index in (0..info.chunks).step_by(6) {
                let chunks = cli
                    .get_file_chunks(info.id, index, Some(6))
                    .await
                    .map_err(format_error)?;
                for chunk in chunks.iter() {
                    file.seek(SeekFrom::Start(chunk.0 as u64 * CHUNK_SIZE as u64))
                        .await
                        .map_err(format_error)?;
                    hasher.update(&chunk.1);
                    file.write_all(&chunk.1).await.map_err(format_error)?;
                    filled += chunk.1.len();
                }

                println!(
                    "downloaded chunks: {}/{}, {:.2}%",
                    index as usize + chunks.len(),
                    info.chunks,
                    (filled as f32 / info.size as f32) * 100.0,
                );
            }

            let hash: [u8; 32] = hasher.finalize().into();
            if let Some(h) = info.hash {
                if *h != hash {
                    Err(format!(
                        "file hash mismatch, expected {}, got {}",
                        hex::encode(*h),
                        hex::encode(hash),
                    ))?;
                }
            }

            println!(
                "\n{}:\n{}\t{}",
                digest,
                hex::encode(hash),
                f.to_string_lossy(),
            );

            return Ok(());
        }

        Some(Commands::Ls {
            bucket,
            parent,
            kind,
            ic,
        }) => {
            let cli = cli.bucket(identity, ic, bucket).await?;
            match kind {
                0 => {
                    let files = cli
                        .list_files(*parent, None, None)
                        .await
                        .map_err(format_error)?;
                    pretty_println(&files)?;
                }
                1 => {
                    let folders = cli
                        .list_folders(*parent, None, None)
                        .await
                        .map_err(format_error)?;
                    pretty_println(&folders)?;
                }
                _ => return Err("invalid kind".to_string()),
            }
            return Ok(());
        }

        Some(Commands::Stat {
            bucket,
            id,
            kind,
            ic,
            hash,
        }) => {
            let cli = cli.bucket(identity, ic, bucket).await?;
            match kind {
                0 => {
                    let info = if let Some(hash) = hash {
                        let hash = parse_file_hash(hash)?;
                        cli.get_file_info_by_hash(hash)
                            .await
                            .map_err(format_error)?
                    } else {
                        cli.get_file_info(*id).await.map_err(format_error)?
                    };

                    pretty_println(&info)?;
                }
                1 => {
                    let info = cli.get_folder_info(*id).await.map_err(format_error)?;
                    pretty_println(&info)?;
                }
                _ => {
                    let info = cli.get_bucket_info().await.map_err(format_error)?;
                    pretty_println(&info)?;
                }
            }
            return Ok(());
        }

        Some(Commands::Mv {
            bucket,
            id,
            from,
            to,
            kind,
            ic,
        }) => {
            let cli = cli.bucket(identity, ic, bucket).await?;
            match kind {
                0 => {
                    let res = cli
                        .move_file(MoveInput {
                            id: *id,
                            from: *from,
                            to: *to,
                        })
                        .await
                        .map_err(format_error)?;
                    pretty_println(&res)?;
                }
                1 => {
                    let res = cli
                        .move_folder(MoveInput {
                            id: *id,
                            from: *from,
                            to: *to,
                        })
                        .await
                        .map_err(format_error)?;
                    pretty_println(&res)?;
                }
                _ => return Err("invalid kind".to_string()),
            }
            return Ok(());
        }

        Some(Commands::Rm {
            bucket,
            id,
            kind,
            ic,
        }) => {
            let cli = cli.bucket(identity, ic, bucket).await?;
            match kind {
                0 => {
                    let res = cli.delete_file(*id).await.map_err(format_error)?;
                    pretty_println(&res)?;
                }
                1 => {
                    let res = cli.delete_folder(*id).await.map_err(format_error)?;
                    pretty_println(&res)?;
                }
                _ => return Err("invalid kind".to_string()),
            }
            return Ok(());
        }

        None => {}
    }

    Ok(())
}

fn load_identity(path: &str) -> anyhow::Result<Box<dyn Identity>> {
    if path == "Anonymous" {
        return Ok(Box::new(AnonymousIdentity));
    }

    let content = std::fs::read_to_string(path)?;
    match Secp256k1Identity::from_pem(content.as_bytes()) {
        Ok(identity) => Ok(Box::new(identity)),
        Err(_) => match BasicIdentity::from_pem(content.as_bytes()) {
            Ok(identity) => Ok(Box::new(identity)),
            Err(err) => Err(err.into()),
        },
    }
}

fn load_access_token(path: Option<&Path>) -> Result<Option<ByteBuf>, String> {
    let Some(path) = path else {
        return Ok(None);
    };
    let metadata = std::fs::symlink_metadata(path).map_err(format_error)?;
    if metadata.file_type().is_symlink() {
        return Err("access token file must not be a symbolic link".to_string());
    }
    if !metadata.is_file() {
        return Err("access token path must be a regular file".to_string());
    }
    if metadata.len() == 0 {
        return Err("access token file is empty".to_string());
    }
    if metadata.len() > MAX_ACCESS_TOKEN_SIZE {
        return Err(format!(
            "access token file exceeds the {} byte limit",
            MAX_ACCESS_TOKEN_SIZE
        ));
    }
    check_access_token_permissions(&metadata)?;

    let encoded = std::fs::read(path).map_err(format_error)?;
    let token = if let Some(encoded) = encoded.strip_prefix(b"base64:") {
        let encoded = std::str::from_utf8(encoded)
            .map_err(|_| "base64 access token file must contain UTF-8 text".to_string())?
            .trim();
        decode_access_token_base64(encoded)?
    } else {
        encoded
    };
    if token.is_empty() {
        return Err("decoded access token is empty".to_string());
    }
    if token.len() as u64 > MAX_ACCESS_TOKEN_SIZE {
        return Err(format!(
            "decoded access token exceeds the {} byte limit",
            MAX_ACCESS_TOKEN_SIZE
        ));
    }
    Ok(Some(ByteBuf::from(token)))
}

fn decode_access_token_base64(encoded: &str) -> Result<Vec<u8>, String> {
    for engine in [
        &general_purpose::STANDARD,
        &general_purpose::STANDARD_NO_PAD,
        &general_purpose::URL_SAFE,
        &general_purpose::URL_SAFE_NO_PAD,
    ] {
        if let Ok(token) = engine.decode(encoded) {
            return Ok(token);
        }
    }
    Err("access token file contains invalid base64".to_string())
}

#[cfg(unix)]
fn check_access_token_permissions(metadata: &std::fs::Metadata) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(
            "access token file permissions are too open; run `chmod 600 <TOKEN_FILE>`".to_string(),
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_access_token_permissions(_metadata: &std::fs::Metadata) -> Result<(), String> {
    Ok(())
}

fn pretty_println<T>(data: &T) -> Result<(), String>
where
    T: CandidType,
{
    println!("{}", format_candid(data)?);
    Ok(())
}

fn parse_request_id(value: &str) -> Result<ByteBuf, String> {
    let bytes = hex::decode(value).map_err(|error| format!("invalid request ID hex: {error}"))?;
    if bytes.is_empty() || bytes.len() > 64 {
        return Err("request ID must contain 1 to 64 bytes".to_string());
    }
    Ok(ByteBuf::from(bytes))
}

fn print_candid_section<T>(name: &str, data: &T) -> Result<(), String>
where
    T: CandidType,
{
    println!("{name}:");
    pretty_println(data)
}

fn format_candid<T>(data: &T) -> Result<String, String>
where
    T: CandidType,
{
    let val = IDLValue::try_from_candid_type(data).map_err(format_error)?;
    let doc = pp_value(7, &val);
    Ok(doc.pretty(120).to_string())
}

fn parse_file_hash(s: &str) -> Result<ByteArray<32>, String> {
    let s = s.replace("\\", "");
    let data = hex::decode(s.strip_prefix("0x").unwrap_or(&s)).map_err(format_error)?;
    let hash: [u8; 32] = data.try_into().map_err(format_error)?;
    Ok(hash.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ic_oss_types::storage::MigrateDirectoryStorageOutput;

    const BUCKET: &str = "aaaaa-aa";

    #[test]
    fn parses_global_access_token_file() {
        let cli = Cli::try_parse_from([
            "ic-oss-cli",
            "--access-token-file",
            "token.cose",
            "sync",
            "--bucket",
            BUCKET,
            "--path",
            ".",
        ])
        .expect("parse access token file");
        assert_eq!(cli.access_token_file, Some(PathBuf::from("token.cose")));
    }

    #[test]
    fn parses_transcode_worker_and_owner_commands() {
        let worker = Cli::try_parse_from([
            "ic-oss-cli",
            "--identity",
            "worker.pem",
            "transcode-run",
            "--hub",
            BUCKET,
            "--job-id",
            "42",
            "--source",
            "source.mov",
            "--bucket",
            BUCKET,
            "--work-dir",
            ".transcode/42",
            "--ffmpeg",
            "/opt/ffmpeg",
            "--ffprobe",
            "/opt/ffprobe",
        ])
        .expect("parse transcode worker");
        assert!(matches!(
            worker.command,
            Some(Commands::TranscodeRun {
                job_id: 42,
                retry: 3,
                ..
            })
        ));

        let owner = Cli::try_parse_from([
            "ic-oss-cli",
            "--identity",
            "owner.pem",
            "transcode-approve",
            "--hub",
            BUCKET,
            "--report",
            ".transcode/42/owner-approval.json",
        ])
        .expect("parse transcode approval");
        assert!(matches!(
            owner.command,
            Some(Commands::TranscodeApprove { report, .. })
                if report == Path::new(".transcode/42/owner-approval.json")
        ));

        let cleanup = Cli::try_parse_from([
            "ic-oss-cli",
            "--identity",
            "worker.pem",
            "transcode-cleanup",
            "--hub",
            BUCKET,
            "--job-id",
            "42",
            "--bucket",
            BUCKET,
            "--work-dir",
            ".transcode/42",
        ])
        .expect("parse transcode cleanup");
        assert!(matches!(
            cleanup.command,
            Some(Commands::TranscodeCleanup {
                job_id: 42,
                work_dir,
                ..
            }) if work_dir == Path::new(".transcode/42")
        ));
    }

    #[test]
    fn parses_pull_and_sync_download_alias() {
        for command in ["pull", "sync-download"] {
            let cli = Cli::try_parse_from([
                "ic-oss-cli",
                command,
                "--bucket",
                BUCKET,
                "--path",
                "./backup",
                "--parent",
                "7",
                "--overwrite",
                "--delete",
                "--dry-run",
                "--exclude",
                "cache/**",
                "--retry",
                "4",
            ])
            .expect("parse pull command");
            match cli.command.expect("pull command") {
                Commands::Pull {
                    bucket,
                    path,
                    parent,
                    overwrite,
                    delete,
                    dry_run,
                    exclude,
                    retry,
                    ic,
                } => {
                    assert_eq!(bucket, BUCKET);
                    assert_eq!(path, PathBuf::from("./backup"));
                    assert_eq!(parent, 7);
                    assert!(overwrite);
                    assert!(delete);
                    assert!(dry_run);
                    assert_eq!(exclude, ["cache/**"]);
                    assert_eq!(retry, 4);
                    assert!(!ic);
                }
                _ => panic!("unexpected command"),
            }
        }
    }

    #[test]
    fn decodes_explicit_base64_access_tokens() {
        assert_eq!(decode_access_token_base64("AQID").unwrap(), [1, 2, 3]);
        assert_eq!(decode_access_token_base64("AQIDBA").unwrap(), [1, 2, 3, 4]);
        assert!(decode_access_token_base64("***").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn access_token_file_requires_private_permissions() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("ic-oss-cli-token-{}-{nonce}", std::process::id()));
        std::fs::write(&path, b"base64:AQID\n").expect("write token fixture");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("set public permissions");
        assert!(load_access_token(Some(&path))
            .unwrap_err()
            .contains("chmod 600"));

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("set private permissions");
        assert_eq!(
            load_access_token(Some(&path)).expect("load protected token"),
            Some(ByteBuf::from(vec![1, 2, 3]))
        );
        std::fs::remove_file(path).expect("remove token fixture");
    }

    #[test]
    fn parses_bucket_migrate_directory_options() {
        let cli = Cli::try_parse_from([
            "ic-oss-cli",
            "bucket-migrate-directory",
            "--bucket",
            BUCKET,
            "--max-items",
            "321",
            "--until-complete",
            "--ic",
        ])
        .expect("parse migration command");

        match cli.command.expect("migration command") {
            Commands::BucketMigrateDirectory {
                bucket,
                max_items,
                until_complete,
                ic,
            } => {
                assert_eq!(bucket, BUCKET);
                assert_eq!(max_items, 321);
                assert!(until_complete);
                assert!(ic);
            }
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn rejects_out_of_range_migration_batch_sizes() {
        for value in ["0", "1001"] {
            let result = Cli::try_parse_from([
                "ic-oss-cli",
                "bucket-migrate-directory",
                "--bucket",
                BUCKET,
                "--max-items",
                value,
            ]);
            let error = match result {
                Ok(_) => panic!("batch size {value} must be rejected"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("not in 1..=1000"));
        }
    }

    #[test]
    fn parses_bucket_observability_commands() {
        for command in ["bucket-capabilities", "bucket-health"] {
            let cli = Cli::try_parse_from(["ic-oss-cli", command, "--bucket", BUCKET])
                .expect("parse observability command");
            match cli.command.expect("observability command") {
                Commands::BucketCapabilities { bucket, ic }
                | Commands::BucketHealth { bucket, ic } => {
                    assert_eq!(bucket, BUCKET);
                    assert!(!ic);
                }
                _ => panic!("unexpected command"),
            }
        }
    }

    #[test]
    fn parses_reader_grant_management_commands() {
        let authority = Cli::try_parse_from([
            "ic-oss-cli",
            "reader-authority",
            "--bucket",
            BUCKET,
            "--authority",
            BUCKET,
        ])
        .expect("parse reader authority command");
        assert!(matches!(
            authority.command,
            Some(Commands::ReaderAuthority {
                authority: Some(value),
                clear: false,
                ..
            }) if value == BUCKET
        ));

        let upsert = Cli::try_parse_from([
            "ic-oss-cli",
            "reader-grant-upsert",
            "--bucket",
            BUCKET,
            "--subject",
            BUCKET,
            "--version",
            "7",
            "--request-id",
            "0102",
            "--expires-at-ms",
            "4102444800000",
        ])
        .expect("parse reader grant upsert command");
        assert!(matches!(
            upsert.command,
            Some(Commands::ReaderGrantUpsert {
                version: 7,
                expires_at_ms: Some(4_102_444_800_000),
                ..
            })
        ));

        let revoke = Cli::try_parse_from([
            "ic-oss-cli",
            "reader-grant-revoke",
            "--bucket",
            BUCKET,
            "--subject",
            BUCKET,
            "--version",
            "8",
            "--request-id",
            "0304",
        ])
        .expect("parse reader grant revoke command");
        assert!(matches!(
            revoke.command,
            Some(Commands::ReaderGrantRevoke { version: 8, .. })
        ));

        let self_query =
            Cli::try_parse_from(["ic-oss-cli", "reader-grant-self", "--bucket", BUCKET])
                .expect("parse reader grant self command");
        assert!(matches!(
            self_query.command,
            Some(Commands::ReaderGrantSelf { .. })
        ));
    }

    #[test]
    fn reader_grant_options_reject_unsafe_values() {
        assert!(
            Cli::try_parse_from(["ic-oss-cli", "reader-authority", "--bucket", BUCKET,]).is_err()
        );
        assert!(Cli::try_parse_from([
            "ic-oss-cli",
            "reader-authority",
            "--bucket",
            BUCKET,
            "--authority",
            BUCKET,
            "--clear",
        ])
        .is_err());
        assert!(parse_request_id("").is_err());
        assert!(parse_request_id("zz").is_err());
        assert!(parse_request_id(&"aa".repeat(65)).is_err());
        assert_eq!(parse_request_id("0102").unwrap(), ByteBuf::from([1, 2]));
    }

    #[test]
    fn parses_bucket_collect_garbage_options() {
        let cli = Cli::try_parse_from([
            "ic-oss-cli",
            "bucket-collect-garbage",
            "--bucket",
            BUCKET,
            "--max-chunks",
            "512",
            "--until-clean",
        ])
        .expect("parse garbage collection command");

        match cli.command.expect("garbage collection command") {
            Commands::BucketCollectGarbage {
                bucket,
                max_chunks,
                until_clean,
                ic,
            } => {
                assert_eq!(bucket, BUCKET);
                assert_eq!(max_chunks, 512);
                assert!(until_clean);
                assert!(!ic);
            }
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn rejects_out_of_range_garbage_collection_batch_sizes() {
        for value in ["0", "1025"] {
            let result = Cli::try_parse_from([
                "ic-oss-cli",
                "bucket-collect-garbage",
                "--bucket",
                BUCKET,
                "--max-chunks",
                value,
            ]);
            let error = match result {
                Ok(_) => panic!("batch size {value} must be rejected"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("not in 1..=1024"));
        }
    }

    #[test]
    fn formats_migration_output_for_operators() {
        let output = MigrateDirectoryStorageOutput {
            state: MigrationState::Ready,
            processed: 7,
            folder_cursor: Some(11),
            file_cursor: Some(19),
            error: None,
        };
        let rendered = format_candid(&output).expect("format migration output");
        assert!(rendered.contains("state = variant { Ready }"));
        assert!(rendered.contains("processed = 7"));
        assert!(rendered.contains("folder_cursor = opt (11 : nat32)"));
    }
}
