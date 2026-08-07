#![cfg(unix)]

use candid::{encode_one, CandidType, Principal};
use ic_agent::{identity::BasicIdentity, Identity};
use pocket_ic::{query_candid_as, update_candid_as, PocketIc};
use ring::{rand::SystemRandom, signature::Ed25519KeyPair};
use serde::Deserialize;
use sha3::{Digest, Sha3_256};
use std::{
    collections::BTreeSet,
    env, fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

const INITIAL_CYCLES: u128 = 2_000_000_000_000;

#[derive(CandidType)]
#[allow(dead_code)]
enum BucketCanisterArgs {
    Init(BucketInitArgs),
    Upgrade(BucketUpgradeArgs),
}

#[derive(CandidType)]
struct BucketInitArgs {
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
struct BucketUpgradeArgs {
    max_file_size: Option<u64>,
    max_folder_depth: Option<u8>,
    max_children: Option<u16>,
    max_custom_data_size: Option<u16>,
    enable_hash_index: Option<bool>,
    governance_canister: Option<Principal>,
}

#[derive(CandidType)]
struct HubInitArgs {
    site_name: String,
}

#[derive(CandidType, Clone, Copy, Deserialize)]
#[allow(dead_code)]
enum BucketClass {
    Public,
    Protected,
}

#[derive(CandidType)]
#[allow(dead_code)]
enum BucketStatus {
    Provisioning,
    Active,
    Draining,
    Offline,
}

#[derive(CandidType)]
struct BucketRegistration {
    canister: Principal,
    class: BucketClass,
    status: BucketStatus,
    label: String,
}

#[derive(CandidType)]
struct RegisterAssetInput {
    bucket: Principal,
    file_id: u32,
    content_type: String,
    size: u64,
    hash: Option<Vec<u8>>,
    generation: u64,
}

#[derive(CandidType, Deserialize)]
#[allow(dead_code)]
struct Asset {
    id: u64,
    bucket: Principal,
    file_id: u32,
    class: BucketClass,
    content_type: String,
    size: u64,
    hash: Option<Vec<u8>>,
    generation: u64,
}

#[derive(CandidType, Clone)]
#[allow(dead_code)]
enum MediaVariantKind {
    Poster,
    Thumbnail,
    Transcode { profile: String },
}

#[derive(CandidType, Clone, Copy)]
#[allow(dead_code)]
enum SourceRetentionPolicy {
    RetainOwnerOnly,
    AllowOriginalDownload,
}

#[derive(CandidType, Clone)]
struct MediaVariantSpec {
    label: String,
    kind: MediaVariantKind,
    content_type: String,
    codec: String,
    width: Option<u32>,
    height: Option<u32>,
    bitrate_bps: Option<u64>,
}

#[derive(CandidType)]
struct CreateTranscodeJobInput {
    idempotency_key: String,
    source_asset_id: u64,
    worker: Principal,
    variants: Vec<MediaVariantSpec>,
    source_retention: Option<SourceRetentionPolicy>,
}

#[derive(CandidType)]
struct VerifyTranscodeOutputInput {
    label: String,
    bucket: Principal,
    file_id: u32,
    content_hash: Vec<u8>,
    generation: u64,
    codec: String,
    width: Option<u32>,
    height: Option<u32>,
    bitrate_bps: Option<u64>,
}

#[derive(CandidType, Deserialize)]
struct RecordId {
    id: u64,
}

#[derive(CandidType, Deserialize)]
#[allow(dead_code)]
struct AssetDescriptor {
    bucket: Principal,
    file_id: u32,
    content_type: String,
    size: u64,
    hash: Option<Vec<u8>>,
    generation: u64,
}

fn wasm_path(package: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
        "../../target/wasm32-unknown-unknown/release/{package}.wasm"
    ))
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

fn update<In, Out>(
    pic: &PocketIc,
    canister: Principal,
    caller: Principal,
    method: &str,
    input: In,
) -> Out
where
    In: candid::utils::ArgumentEncoder,
    Out: for<'a> candid::utils::ArgumentDecoder<'a>,
{
    update_candid_as(pic, canister, caller, method, input)
        .unwrap_or_else(|error| panic!("{method}: {error}"))
}

fn write_fake_media_tools(directory: &Path) -> (PathBuf, PathBuf) {
    let ffmpeg = directory.join("ffmpeg");
    let ffprobe = directory.join("ffprobe");
    fs::write(
        &ffmpeg,
        r#"#!/bin/sh
if [ "$1" = "-version" ]; then
  echo "ffmpeg fixture"
  exit 0
fi
input=""
output=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-i" ]; then
    shift
    input="$1"
  fi
  output="$1"
  shift
done
cp "$input" "$output"
"#,
    )
    .expect("write fake ffmpeg");
    fs::write(
        &ffprobe,
        r#"#!/bin/sh
if [ "$1" = "-version" ]; then
  echo "ffprobe fixture"
  exit 0
fi
cat <<'JSON'
{"streams":[{"codec_type":"video","codec_name":"h264","profile":"Main","level":31,"width":1280,"height":720,"bit_rate":"2800000"},{"codec_type":"audio","codec_name":"aac","profile":"LC","bit_rate":"128000"}],"format":{"format_name":"mov,mp4,m4a,3gp,3g2,mj2"}}
JSON
"#,
    )
    .expect("write fake ffprobe");
    fs::set_permissions(&ffmpeg, fs::Permissions::from_mode(0o700))
        .expect("make fake ffmpeg executable");
    fs::set_permissions(&ffprobe, fs::Permissions::from_mode(0o700))
        .expect("make fake ffprobe executable");
    (ffmpeg, ffprobe)
}

fn run_cli(arguments: &[String]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_ic-oss-cli"))
        .args(arguments)
        .output()
        .expect("run ic-oss-cli");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(
        output.status.success(),
        "CLI failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    stdout
}

#[test]
#[ignore = "requires compiled Personal Hub and Bucket Wasm artifacts"]
fn local_transcode_worker_uploads_and_owner_independently_promotes_ready_asset() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let root = env::temp_dir().join(format!(
        "ic-oss-transcode-pocket-ic-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("create transcode fixture directory");
    let owner_pem = root.join("owner.pem");
    let worker_pem = root.join("worker.pem");
    let owner = create_identity(&owner_pem);
    let worker = create_identity(&worker_pem);

    let mut pic = PocketIc::new();
    let hub = pic.create_canister_with_settings(Some(owner), None);
    pic.add_cycles(hub, INITIAL_CYCLES);
    pic.install_canister(
        hub,
        fs::read(wasm_path("personal_hub")).expect("read Personal Hub Wasm"),
        encode_one(HubInitArgs {
            site_name: "CLI transcode".into(),
        })
        .expect("encode Hub init"),
        Some(owner),
    );
    let bucket = pic.create_canister_with_settings(Some(owner), None);
    pic.add_cycles(bucket, INITIAL_CYCLES);
    pic.install_canister(
        bucket,
        fs::read(wasm_path("ic_oss_bucket")).expect("read Bucket Wasm"),
        encode_one(None::<BucketCanisterArgs>).expect("encode Bucket init"),
        Some(owner),
    );
    let (managers,): (Result<(), String>,) = update(
        &pic,
        bucket,
        owner,
        "admin_set_managers",
        (BTreeSet::from([worker, hub]),),
    );
    managers.expect("configure worker and Hub as Bucket managers");
    let (registered,): (Result<(), String>,) = update(
        &pic,
        hub,
        owner,
        "admin_register_bucket",
        (BucketRegistration {
            canister: bucket,
            class: BucketClass::Protected,
            status: BucketStatus::Active,
            label: "transcode-output".into(),
        },),
    );
    registered.expect("register transcode Bucket");

    let source = root.join("source.mov");
    let source_bytes = (0..700_000)
        .map(|index| ((index * 19 + 11) % 256) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &source_bytes).expect("write source");
    let source_hash = Sha3_256::digest(&source_bytes).to_vec();
    let (source_asset,): (Result<Asset, String>,) = update(
        &pic,
        hub,
        owner,
        "admin_register_asset",
        (RegisterAssetInput {
            bucket,
            file_id: 900,
            content_type: "video/quicktime".into(),
            size: source_bytes.len() as u64,
            hash: Some(source_hash),
            generation: 1,
        },),
    );
    let source_asset = source_asset.expect("register source Asset");
    assert_eq!(source_asset.id, 1);
    let (job,): (Result<RecordId, String>,) = update(
        &pic,
        hub,
        owner,
        "admin_create_transcode_job",
        (CreateTranscodeJobInput {
            idempotency_key: "cli-transcode".into(),
            source_asset_id: source_asset.id,
            worker,
            source_retention: None,
            variants: vec![MediaVariantSpec {
                label: "720p".into(),
                kind: MediaVariantKind::Transcode {
                    profile: "h264-main".into(),
                },
                content_type: "video/mp4".into(),
                codec: "avc1.4d401f,mp4a.40.2".into(),
                width: Some(1_280),
                height: Some(720),
                bitrate_bps: Some(3_000_000),
            }],
        },),
    );
    let job = job.expect("create transcode job");
    assert_eq!(job.id, 2);

    let (ffmpeg, ffprobe) = write_fake_media_tools(&root);
    let work_dir = root.join("work");
    let gateway = pic.make_live(None);
    let worker_stdout = run_cli(&[
        "--identity".into(),
        worker_pem.display().to_string(),
        "--host".into(),
        gateway.to_string(),
        "transcode-run".into(),
        "--hub".into(),
        hub.to_text(),
        "--job-id".into(),
        job.id.to_string(),
        "--source".into(),
        source.display().to_string(),
        "--bucket".into(),
        bucket.to_text(),
        "--work-dir".into(),
        work_dir.display().to_string(),
        "--ffmpeg".into(),
        ffmpeg.display().to_string(),
        "--ffprobe".into(),
        ffprobe.display().to_string(),
        "--retry".into(),
        "1".into(),
    ]);
    assert!(worker_stdout.contains("\"status\": \"AwaitingVerification\""));
    let report = work_dir.join("owner-approval.json");
    assert!(report.is_file());

    let owner_stdout = run_cli(&[
        "--identity".into(),
        owner_pem.display().to_string(),
        "--host".into(),
        gateway.to_string(),
        "transcode-approve".into(),
        "--hub".into(),
        hub.to_text(),
        "--report".into(),
        report.display().to_string(),
        "--ffprobe".into(),
        ffprobe.display().to_string(),
    ]);
    assert!(owner_stdout.contains("\"status\": \"Ready\""));
    assert!(owner_stdout.contains("\"asset_id\": 3"));

    let (descriptor,): (Result<AssetDescriptor, String>,) =
        query_candid_as(&pic, hub, owner, "get_private_asset_descriptor", (3u64,))
            .expect("query promoted Asset");
    let descriptor = descriptor.expect("promoted Asset descriptor");
    assert_eq!(descriptor.bucket, bucket);
    assert_eq!(descriptor.size, source_bytes.len() as u64);
    assert_eq!(
        descriptor.hash,
        Some(Sha3_256::digest(&source_bytes).to_vec())
    );
    assert_eq!(
        descriptor.generation, 0,
        "legacy inline Bucket uploads use generation zero"
    );

    let (cancelled_job,): (Result<RecordId, String>,) = update(
        &pic,
        hub,
        owner,
        "admin_create_transcode_job",
        (CreateTranscodeJobInput {
            idempotency_key: "cli-transcode-cleanup".into(),
            source_asset_id: source_asset.id,
            worker,
            source_retention: None,
            variants: vec![MediaVariantSpec {
                label: "cleanup-720p".into(),
                kind: MediaVariantKind::Transcode {
                    profile: "h264-main".into(),
                },
                content_type: "video/mp4".into(),
                codec: "avc1.4d401f,mp4a.40.2".into(),
                width: Some(1_280),
                height: Some(720),
                bitrate_bps: Some(3_000_000),
            }],
        },),
    );
    let cancelled_job = cancelled_job.expect("create cleanup transcode job");
    let cleanup_work_dir = root.join("cleanup-work");
    run_cli(&[
        "--identity".into(),
        worker_pem.display().to_string(),
        "--host".into(),
        gateway.to_string(),
        "transcode-run".into(),
        "--hub".into(),
        hub.to_text(),
        "--job-id".into(),
        cancelled_job.id.to_string(),
        "--source".into(),
        source.display().to_string(),
        "--bucket".into(),
        bucket.to_text(),
        "--work-dir".into(),
        cleanup_work_dir.display().to_string(),
        "--ffmpeg".into(),
        ffmpeg.display().to_string(),
        "--ffprobe".into(),
        ffprobe.display().to_string(),
        "--retry".into(),
        "1".into(),
    ]);
    let cleanup_report: serde_json::Value = serde_json::from_slice(
        &fs::read(cleanup_work_dir.join("owner-approval.json"))
            .expect("read cleanup approval report"),
    )
    .expect("decode cleanup approval report");
    let cleanup_file_id = cleanup_report["approvals"][0]["file_id"]
        .as_u64()
        .expect("cleanup output file ID") as u32;
    let (cancelled,): (Result<RecordId, String>,) = update(
        &pic,
        hub,
        owner,
        "admin_cancel_transcode_job",
        (cancelled_job.id,),
    );
    assert_eq!(
        cancelled.expect("cancel transcode job").id,
        cancelled_job.id
    );

    let cleanup_stdout = run_cli(&[
        "--identity".into(),
        worker_pem.display().to_string(),
        "--host".into(),
        gateway.to_string(),
        "transcode-cleanup".into(),
        "--hub".into(),
        hub.to_text(),
        "--job-id".into(),
        cancelled_job.id.to_string(),
        "--bucket".into(),
        bucket.to_text(),
        "--work-dir".into(),
        cleanup_work_dir.display().to_string(),
    ]);
    assert!(cleanup_stdout.contains("\"job_status\": \"Cancelled\""));
    assert!(cleanup_stdout
        .contains("\"reason\": \"remote output deleted with hash/revision preconditions\""));
    let (removed,): (Result<AssetDescriptor, String>,) = query_candid_as(
        &pic,
        bucket,
        worker,
        "get_file_descriptor",
        (cleanup_file_id, Option::<Vec<u8>>::None),
    )
    .expect("query cleaned output");
    assert!(removed.is_err(), "cancelled output must be removed");

    let repeated_cleanup = run_cli(&[
        "--identity".into(),
        worker_pem.display().to_string(),
        "--host".into(),
        gateway.to_string(),
        "transcode-cleanup".into(),
        "--hub".into(),
        hub.to_text(),
        "--job-id".into(),
        cancelled_job.id.to_string(),
        "--bucket".into(),
        bucket.to_text(),
        "--work-dir".into(),
        cleanup_work_dir.display().to_string(),
    ]);
    assert!(repeated_cleanup.contains("\"reason\": \"journal has no remote output\""));

    let (failed_job,): (Result<RecordId, String>,) = update(
        &pic,
        hub,
        owner,
        "admin_create_transcode_job",
        (CreateTranscodeJobInput {
            idempotency_key: "cli-transcode-failed-cleanup".into(),
            source_asset_id: source_asset.id,
            worker,
            source_retention: None,
            variants: vec![MediaVariantSpec {
                label: "failed-720p".into(),
                kind: MediaVariantKind::Transcode {
                    profile: "h264-main".into(),
                },
                content_type: "video/mp4".into(),
                codec: "avc1.4d401f,mp4a.40.2".into(),
                width: Some(1_280),
                height: Some(720),
                bitrate_bps: Some(3_000_000),
            }],
        },),
    );
    let failed_job = failed_job.expect("create failed transcode job");
    let failed_work_dir = root.join("failed-work");
    run_cli(&[
        "--identity".into(),
        worker_pem.display().to_string(),
        "--host".into(),
        gateway.to_string(),
        "transcode-run".into(),
        "--hub".into(),
        hub.to_text(),
        "--job-id".into(),
        failed_job.id.to_string(),
        "--source".into(),
        source.display().to_string(),
        "--bucket".into(),
        bucket.to_text(),
        "--work-dir".into(),
        failed_work_dir.display().to_string(),
        "--ffmpeg".into(),
        ffmpeg.display().to_string(),
        "--ffprobe".into(),
        ffprobe.display().to_string(),
        "--retry".into(),
        "1".into(),
    ]);
    let failed_report: serde_json::Value = serde_json::from_slice(
        &fs::read(failed_work_dir.join("owner-approval.json"))
            .expect("read failed approval report"),
    )
    .expect("decode failed approval report");
    let failed_approval = &failed_report["approvals"][0];
    let failed_file_id = failed_approval["file_id"]
        .as_u64()
        .expect("failed output file ID") as u32;
    let rejected_hash = hex::decode(
        failed_approval["content_hash_hex"]
            .as_str()
            .expect("failed output hash"),
    )
    .expect("decode failed output hash");
    let (rejected,): (Result<RecordId, String>,) = update(
        &pic,
        hub,
        owner,
        "admin_verify_transcode_output",
        (
            failed_job.id,
            VerifyTranscodeOutputInput {
                label: "failed-720p".into(),
                bucket,
                file_id: failed_file_id,
                content_hash: rejected_hash,
                generation: failed_approval["generation"]
                    .as_u64()
                    .expect("failed output generation"),
                codec: "avc1.4d401f,mp4a.40.2".into(),
                width: Some(1_280),
                height: Some(720),
                bitrate_bps: Some(3_000_001),
            },
        ),
    );
    assert!(
        rejected.is_err(),
        "out-of-contract verification must fail the job"
    );
    let failed_cleanup = run_cli(&[
        "--identity".into(),
        worker_pem.display().to_string(),
        "--host".into(),
        gateway.to_string(),
        "transcode-cleanup".into(),
        "--hub".into(),
        hub.to_text(),
        "--job-id".into(),
        failed_job.id.to_string(),
        "--bucket".into(),
        bucket.to_text(),
        "--work-dir".into(),
        failed_work_dir.display().to_string(),
    ]);
    assert!(failed_cleanup.contains("\"job_status\": \"Failed\""));
    assert!(failed_cleanup
        .contains("\"reason\": \"remote output deleted with hash/revision preconditions\""));
    let (failed_removed,): (Result<AssetDescriptor, String>,) = query_candid_as(
        &pic,
        bucket,
        worker,
        "get_file_descriptor",
        (failed_file_id, Option::<Vec<u8>>::None),
    )
    .expect("query failed-job cleaned output");
    assert!(failed_removed.is_err(), "failed output must be removed");

    fs::remove_dir_all(root).expect("remove transcode fixture directory");
}
