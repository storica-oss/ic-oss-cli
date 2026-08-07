use base64::{engine::general_purpose, Engine as _};
use candid::{encode_one, CandidType, Principal};
use ic_agent::{identity::BasicIdentity, Identity};
use ic_oss_types::{
    bucket::UpdateBucketInput,
    cose::{cose_sign1, cose_sign1_to_vec, EdDSA, Token, BUCKET_TOKEN_AAD},
    entry::{EnsureFolderInput, EnsureFolderOutput, SyncError},
    permission::Policies,
    storage::{SubtreeManifestInput, SubtreeManifestOutput},
    upload::UploadHealth,
};
use pocket_ic::{query_candid_as, update_candid_as, PocketIc};
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use serde_bytes::{ByteArray, ByteBuf};
use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const INITIAL_CYCLES: u128 = 2_000_000_000_000;

#[derive(CandidType)]
#[allow(dead_code)]
enum CanisterArgs {
    Init(InitArgs),
    Upgrade(UpgradeArgs),
}

#[derive(CandidType)]
struct InitArgs {
    name: String,
    file_id: u32,
    max_file_size: u64,
    max_folder_depth: u8,
    max_children: u16,
    max_custom_data_size: u16,
    enable_hash_index: bool,
    visibility: u8,
    governance_canister: Option<Principal>,
}

#[derive(CandidType)]
struct UpgradeArgs {
    max_file_size: Option<u64>,
    max_folder_depth: Option<u8>,
    max_children: Option<u16>,
    max_custom_data_size: Option<u16>,
    enable_hash_index: Option<bool>,
    governance_canister: Option<Principal>,
}

fn bucket_wasm_path() -> PathBuf {
    env::var_os("IC_OSS_BUCKET_WASM").map_or_else(
        || {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/wasm32-unknown-unknown/release/ic_oss_bucket.wasm")
        },
        PathBuf::from,
    )
}

fn deploy_bucket(pic: &PocketIc, controller: Principal) -> Principal {
    let canister = pic.create_canister_with_settings(Some(controller), None);
    pic.add_cycles(canister, INITIAL_CYCLES);
    let wasm = fs::read(bucket_wasm_path()).expect("read release bucket Wasm");
    pic.install_canister(
        canister,
        wasm,
        encode_one(None::<CanisterArgs>).expect("encode init args"),
        Some(controller),
    );
    canister
}

fn set_manager(pic: &PocketIc, bucket: Principal, manager: Principal) {
    let (result,): (Result<(), String>,) = update_candid_as(
        pic,
        bucket,
        manager,
        "admin_set_managers",
        (BTreeSet::from([manager]),),
    )
    .expect("call admin_set_managers");
    result.expect("set CLI manager");
}

fn create_identity(path: &Path) -> Principal {
    let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("generate identity");
    let document = pem::encode(&pem::Pem::new("PRIVATE KEY", key.as_ref()));
    fs::write(path, document.as_bytes()).expect("write identity");
    BasicIdentity::from_pem(document.as_bytes())
        .expect("parse identity")
        .sender()
        .expect("identity principal")
}

fn run_sync(
    gateway: &str,
    bucket: Principal,
    identity: &Path,
    local_root: &Path,
    cache: &Path,
) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_ic-oss-cli"))
        .env("XDG_CACHE_HOME", cache)
        .args([
            "--identity",
            identity.to_str().expect("identity path"),
            "--host",
            gateway,
            "sync",
            "--bucket",
            &bucket.to_text(),
            "--path",
            local_root.to_str().expect("local root path"),
            "--retry",
            "1",
        ])
        .output()
        .expect("run ic-oss-cli sync");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "sync failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    stdout
}

fn run_pull(
    gateway: &str,
    bucket: Principal,
    identity: &Path,
    local_root: &Path,
    cache: &Path,
    overwrite: bool,
    delete: bool,
) -> String {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ic-oss-cli"));
    command.env("XDG_CACHE_HOME", cache).args([
        "--identity",
        identity.to_str().expect("identity path"),
        "--host",
        gateway,
        "pull",
        "--bucket",
        &bucket.to_text(),
        "--path",
        local_root.to_str().expect("local root path"),
        "--retry",
        "1",
    ]);
    if overwrite {
        command.arg("--overwrite");
    }
    if delete {
        command.arg("--delete");
    }
    let output = command.output().expect("run ic-oss-cli pull");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "pull failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    stdout
}

fn run_token_sync(
    gateway: &str,
    bucket: Principal,
    token: &Path,
    local_root: &Path,
    parent: u32,
    cache: &Path,
) -> String {
    wait_for_token_sync(spawn_token_sync(
        gateway, bucket, token, local_root, parent, cache, false,
    ))
}

fn spawn_token_sync(
    gateway: &str,
    bucket: Principal,
    token: &Path,
    local_root: &Path,
    parent: u32,
    cache: &Path,
    overwrite: bool,
) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ic-oss-cli"));
    command.env("XDG_CACHE_HOME", cache).args([
        "--access-token-file",
        token.to_str().expect("token path"),
        "--host",
        gateway,
        "sync",
        "--bucket",
        &bucket.to_text(),
        "--path",
        local_root.to_str().expect("local root path"),
        "--parent",
        &parent.to_string(),
        "--retry",
        "1",
    ]);
    if overwrite {
        command.arg("--overwrite");
    }
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn token-authenticated sync")
}

fn wait_for_token_sync(child: Child) -> String {
    let output = child
        .wait_with_output()
        .expect("wait for token-authenticated sync");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "token sync failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    stdout
}

fn set_trusted_token_keys(
    pic: &PocketIc,
    bucket: Principal,
    manager: Principal,
    keys: Vec<ByteArray<32>>,
) {
    let (updated,): (Result<(), String>,) = update_candid_as(
        pic,
        bucket,
        manager,
        "admin_update_bucket",
        (UpdateBucketInput {
            trusted_eddsa_pub_keys: Some(keys),
            ..Default::default()
        },),
    )
    .expect("configure CLI token keys");
    updated.expect("update trusted token keys");
}

fn write_access_token(path: &Path, token: &[u8]) {
    fs::write(
        path,
        format!("base64:{}\n", general_purpose::STANDARD.encode(token)),
    )
    .expect("write access token");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("protect access token");
    }
}

fn replace_access_token(path: &Path, token: &[u8]) {
    let replacement = path.with_extension("cose.new");
    write_access_token(&replacement, token);
    fs::rename(&replacement, path).expect("atomically replace access token");
}

fn upload_health(pic: &PocketIc, bucket: Principal, manager: Principal) -> UploadHealth {
    let (health,): (Result<UploadHealth, SyncError>,) = query_candid_as(
        pic,
        bucket,
        manager,
        "get_upload_health",
        (Option::<ByteBuf>::None,),
    )
    .expect("query upload health during CLI sync");
    health.expect("read upload health during CLI sync")
}

fn wait_for_active_upload(
    pic: &PocketIc,
    bucket: Principal,
    manager: Principal,
    child: &mut Child,
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            child
                .try_wait()
                .expect("inspect CLI sync process")
                .is_none(),
            "CLI sync exited before opening an upload session"
        );
        if upload_health(pic, bucket, manager).active_sessions > 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for CLI upload session"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_journaled_chunk(cache: &Path, child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let journal_dir = cache.join("ic-oss/sync-journals");
    loop {
        assert!(
            child
                .try_wait()
                .expect("inspect CLI before interruption")
                .is_none(),
            "CLI sync exited before journaling an uploaded chunk"
        );
        let has_uploaded_chunk = fs::read_dir(&journal_dir).is_ok_and(|entries| {
            entries.filter_map(Result::ok).any(|entry| {
                fs::read(entry.path())
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                    .and_then(|journal| {
                        journal
                            .get("actions")
                            .and_then(|value| value.as_array())
                            .cloned()
                    })
                    .is_some_and(|actions| {
                        actions.iter().any(|action| {
                            action
                                .get("upload")
                                .and_then(|upload| upload.get("uploaded_ranges"))
                                .and_then(|ranges| ranges.as_array())
                                .is_some_and(|ranges| !ranges.is_empty())
                        })
                    })
            })
        });
        if has_uploaded_chunk {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for an uploaded chunk checkpoint"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn signed_access_token(key: &Ed25519KeyPair, subject: Principal, audience: Principal) -> Vec<u8> {
    let now_sec = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_secs() as i64;
    let claims = Token {
        subject,
        audience,
        policies: Policies::all().to_string(),
    }
    .to_cwt(now_sec, 3_600);
    let mut sign1 = cose_sign1(claims, EdDSA, None).expect("create CLI COSE token");
    let signing_input = sign1
        .prepare_signature(None, None, Some(BUCKET_TOKEN_AAD))
        .expect("prepare CLI token signature");
    sign1
        .set_signature(key.sign(&signing_input).as_ref().to_vec())
        .expect("set CLI token signature");
    cose_sign1_to_vec(&sign1).expect("encode CLI token")
}

fn run_bucket_command(
    gateway: &str,
    bucket: Principal,
    identity: &Path,
    cache: &Path,
    command: &str,
    extra: &[&str],
) -> String {
    let bucket = bucket.to_text();
    let mut process = Command::new(env!("CARGO_BIN_EXE_ic-oss-cli"));
    process.env("XDG_CACHE_HOME", cache).args([
        "--identity",
        identity.to_str().expect("identity path"),
        "--host",
        gateway,
        command,
        "--bucket",
        &bucket,
    ]);
    process.args(extra);
    let output = process.output().expect("run ic-oss-cli bucket command");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "{command} failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    stdout
}

fn temporary_root() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    env::temp_dir().join(format!("ic-oss-cli-sync-{}-{nonce}", std::process::id()))
}

#[test]
#[ignore = "requires release bucket Wasm and PocketIC; run `make test-integration`"]
fn cli_sync_uses_batches_and_is_idempotent() {
    let temporary = temporary_root();
    let local = temporary.join("local");
    let cache = temporary.join("cache");
    let identity_path = temporary.join("manager.pem");
    fs::create_dir_all(&temporary).expect("create temporary directory");
    let manager = create_identity(&identity_path);

    let mut pic = PocketIc::new();
    let bucket = deploy_bucket(&pic, manager);
    fs::create_dir_all(local.join("docs")).expect("create docs");
    fs::create_dir_all(local.join("assets")).expect("create assets");
    fs::write(local.join("README.txt"), b"root readme").expect("write README");
    fs::write(local.join("docs/guide.md"), b"guide").expect("write guide");
    fs::write(local.join("assets/empty.bin"), []).expect("write empty file");
    fs::write(local.join("large.bin"), vec![9u8; 256 * 1024 + 1]).expect("write large file");
    set_manager(&pic, bucket, manager);
    let gateway = pic.make_live(None);

    let capabilities = run_bucket_command(
        gateway.as_str(),
        bucket,
        &identity_path,
        &cache,
        "bucket-capabilities",
        &[],
    );
    assert!(capabilities.contains("migration_state = variant { Legacy }"));

    let migration = run_bucket_command(
        gateway.as_str(),
        bucket,
        &identity_path,
        &cache,
        "bucket-migrate-directory",
        &["--max-items", "1", "--until-complete"],
    );
    assert!(migration.contains("state = variant { Ready }"));

    let health = run_bucket_command(
        gateway.as_str(),
        bucket,
        &identity_path,
        &cache,
        "bucket-health",
        &[],
    );
    assert!(health.contains("directory_storage:"));
    assert!(health.contains("duplicate_names = 0"));
    assert!(health.contains("dangling_entries = 0"));
    assert!(health.contains("upload_sessions:"));
    assert!(health.contains("garbage_collection:"));

    let retry = run_bucket_command(
        gateway.as_str(),
        bucket,
        &identity_path,
        &cache,
        "bucket-retry-directory-migration",
        &[],
    );
    assert!(retry.contains("state = variant { Ready }"));

    let first = run_sync(gateway.as_str(), bucket, &identity_path, &local, &cache);
    assert!(
        first.contains("revision-guarded subtree manifest"),
        "manifest fast path was not used:\n{first}"
    );
    assert!(
        first.contains("created directory") && first.contains("batched"),
        "directory batch was not used:\n{first}"
    );
    assert!(
        first.contains("uploaded file") && first.contains("batched"),
        "small-file batch was not used:\n{first}"
    );
    assert!(
        first.contains("uploaded atomically"),
        "large file did not use an atomic upload session: {first}"
    );

    let (manifest,): (Result<SubtreeManifestOutput, SyncError>,) = query_candid_as(
        &pic,
        bucket,
        manager,
        "get_subtree_manifest",
        (
            SubtreeManifestInput {
                root: 0,
                cursor: None,
                take: Some(100),
            },
            Option::<ByteBuf>::None,
        ),
    )
    .expect("query manifest after sync");
    let entries = manifest.expect("manifest result").entries;
    let large_file_id = entries
        .iter()
        .find(|entry| entry.path == "large.bin")
        .expect("large file manifest entry")
        .entry
        .id;
    let paths = entries
        .into_iter()
        .map(|entry| entry.path)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        paths,
        BTreeSet::from([
            "README.txt".to_string(),
            "assets".to_string(),
            "assets/empty.bin".to_string(),
            "docs".to_string(),
            "docs/guide.md".to_string(),
            "large.bin".to_string(),
        ])
    );

    let second = run_sync(gateway.as_str(), bucket, &identity_path, &local, &cache);
    assert!(second.contains("Create directories: 0"));
    assert!(second.contains("Upload files:       0"));
    assert!(second.contains("sync completed and remote metadata verification passed"));

    let pulled = temporary.join("pulled");
    let first_pull = run_pull(
        gateway.as_str(),
        bucket,
        &identity_path,
        &pulled,
        &cache,
        false,
        false,
    );
    assert!(first_pull.contains("revision-guarded subtree manifest"));
    assert!(first_pull.contains("download sync completed"));
    assert_eq!(
        fs::read(pulled.join("README.txt")).expect("read pulled README"),
        b"root readme"
    );
    assert_eq!(
        fs::read(pulled.join("docs/guide.md")).expect("read pulled guide"),
        b"guide"
    );
    assert_eq!(
        fs::metadata(pulled.join("large.bin"))
            .expect("inspect pulled large file")
            .len(),
        256 * 1024 + 1
    );
    let second_pull = run_pull(
        gateway.as_str(),
        bucket,
        &identity_path,
        &pulled,
        &cache,
        false,
        false,
    );
    assert!(second_pull.contains("Download files:     0"));
    assert!(second_pull.contains("Replace files:      0"));

    fs::write(pulled.join("README.txt"), b"changed locally").expect("change pulled README");
    fs::write(pulled.join("local-only.txt"), b"delete me").expect("write local-only file");
    let mirrored_pull = run_pull(
        gateway.as_str(),
        bucket,
        &identity_path,
        &pulled,
        &cache,
        true,
        true,
    );
    assert!(mirrored_pull.contains("replaced README.txt"));
    assert!(!pulled.join("local-only.txt").exists());
    assert_eq!(
        fs::read(pulled.join("README.txt")).expect("read replaced README"),
        b"root readme"
    );

    let token_key_document =
        Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("generate token key");
    let token_key =
        Ed25519KeyPair::from_pkcs8(token_key_document.as_ref()).expect("parse token key");
    let public_key: [u8; 32] = token_key
        .public_key()
        .as_ref()
        .try_into()
        .expect("Ed25519 public key");
    set_trusted_token_keys(&pic, bucket, manager, vec![public_key.into()]);
    let (delegated_root,): (Result<EnsureFolderOutput, SyncError>,) = update_candid_as(
        &pic,
        bucket,
        manager,
        "ensure_folder",
        (
            EnsureFolderInput {
                request_id: ByteBuf::from(vec![90]),
                parent: 0,
                name: "delegated-root".to_string(),
            },
            Option::<ByteBuf>::None,
        ),
    )
    .expect("create delegated sync root");
    let delegated_root = delegated_root.expect("delegated sync root");
    let delegated_local = temporary.join("delegated-local");
    let token_path = temporary.join("access-token.cose");
    fs::create_dir_all(&delegated_local).expect("create delegated local root");
    fs::write(delegated_local.join("token.txt"), b"delegated sync")
        .expect("write delegated fixture");
    let token = signed_access_token(
        &token_key,
        Principal::self_authenticating(b"cli-delegated-sync"),
        bucket,
    );
    write_access_token(&token_path, &token);
    let delegated = run_token_sync(
        gateway.as_str(),
        bucket,
        &token_path,
        &delegated_local,
        delegated_root.id,
        &cache,
    );
    assert!(delegated.contains("sync completed and remote metadata verification passed"));

    let rotated_key_document =
        Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("generate rotated token key");
    let rotated_key =
        Ed25519KeyPair::from_pkcs8(rotated_key_document.as_ref()).expect("parse rotated token key");
    let rotated_public_key: [u8; 32] = rotated_key
        .public_key()
        .as_ref()
        .try_into()
        .expect("rotated Ed25519 public key");
    let rotated_token = signed_access_token(
        &rotated_key,
        Principal::self_authenticating(b"cli-delegated-sync"),
        bucket,
    );
    let rotated_size = 16 * 1024 * 1024;
    fs::write(delegated_local.join("token.txt"), vec![7u8; rotated_size])
        .expect("write token rotation fixture");
    let mut rotating_sync = spawn_token_sync(
        gateway.as_str(),
        bucket,
        &token_path,
        &delegated_local,
        delegated_root.id,
        &cache,
        true,
    );
    wait_for_active_upload(&pic, bucket, manager, &mut rotating_sync);
    wait_for_journaled_chunk(&cache, &mut rotating_sync);
    rotating_sync
        .kill()
        .expect("interrupt CLI during atomic upload");
    let interrupted = rotating_sync
        .wait_with_output()
        .expect("wait for interrupted CLI sync");
    assert!(!interrupted.status.success());
    assert_eq!(
        upload_health(&pic, bucket, manager).active_sessions,
        1,
        "interrupted upload session was not retained"
    );

    let mut resumed_sync = spawn_token_sync(
        gateway.as_str(),
        bucket,
        &token_path,
        &delegated_local,
        delegated_root.id,
        &cache,
        true,
    );
    thread::sleep(Duration::from_millis(200));
    assert!(
        resumed_sync
            .try_wait()
            .expect("inspect resumed CLI process")
            .is_none(),
        "resumed CLI exited before key rotation"
    );
    assert_eq!(
        upload_health(&pic, bucket, manager).active_sessions,
        1,
        "resumed CLI created a second session instead of reusing the journaled session"
    );
    set_trusted_token_keys(
        &pic,
        bucket,
        manager,
        vec![public_key.into(), rotated_public_key.into()],
    );
    replace_access_token(&token_path, &rotated_token);
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        upload_health(&pic, bucket, manager).active_sessions,
        1,
        "CLI rotation fixture completed before the old key could be retired"
    );
    set_trusted_token_keys(&pic, bucket, manager, vec![rotated_public_key.into()]);
    let rotated = wait_for_token_sync(resumed_sync);
    assert!(rotated.contains("resuming atomic upload"));
    assert!(rotated.contains("replaced file token.txt"));
    assert!(rotated.contains("sync completed and remote metadata verification passed"));

    let (delegated_manifest,): (Result<SubtreeManifestOutput, SyncError>,) = query_candid_as(
        &pic,
        bucket,
        manager,
        "get_subtree_manifest",
        (
            SubtreeManifestInput {
                root: delegated_root.id,
                cursor: None,
                take: Some(10),
            },
            Option::<ByteBuf>::None,
        ),
    )
    .expect("query delegated manifest");
    let delegated_entry = &delegated_manifest.expect("delegated manifest").entries[0];
    assert_eq!(delegated_entry.path, "token.txt");
    assert_eq!(delegated_entry.entry.size, Some(rotated_size as u64));

    let large_file_id = large_file_id.to_string();
    let deleted = run_bucket_command(
        gateway.as_str(),
        bucket,
        &identity_path,
        &cache,
        "rm",
        &["--id", &large_file_id],
    );
    assert!(deleted.contains("true"));

    let pending_health = run_bucket_command(
        gateway.as_str(),
        bucket,
        &identity_path,
        &cache,
        "bucket-health",
        &[],
    );
    assert!(pending_health.contains("pending_items = 2"));
    assert!(pending_health.contains("pending_chunks = 3"));

    let garbage_collection = run_bucket_command(
        gateway.as_str(),
        bucket,
        &identity_path,
        &cache,
        "bucket-collect-garbage",
        &["--max-chunks", "1", "--until-clean"],
    );
    assert_eq!(
        garbage_collection.matches("processed_chunks = 1").count(),
        3
    );
    assert!(garbage_collection.contains("remaining_items = 0"));
    assert!(garbage_collection.contains("remaining_chunks = 0"));

    let clean_health = run_bucket_command(
        gateway.as_str(),
        bucket,
        &identity_path,
        &cache,
        "bucket-health",
        &[],
    );
    assert!(clean_health.contains("pending_items = 0"));
    assert!(clean_health.contains("pending_chunks = 0"));

    pic.stop_live();
    fs::remove_dir_all(temporary).expect("remove temporary sync directory");
}
