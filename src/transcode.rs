use candid::{CandidType, Principal};
use ic_agent::Agent;
use ic_oss::{
    agent::{query_call, update_call},
    bucket::Client as BucketClient,
};
use ic_oss_types::entry::{DeleteEntryIfInput, EntryKind as ApiEntryKind};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha3::{Digest, Sha3_256};
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
};
use tokio::process::Command;

use crate::file::upload_file_with_content_type;

const SHA3_256_BYTES: usize = 32;
const JOURNAL_SCHEMA_VERSION: u16 = 2;
const PREVIOUS_JOURNAL_SCHEMA_VERSION: u16 = 1;

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MediaVariantKind {
    Poster,
    Thumbnail,
    Transcode { profile: String },
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MediaVariantSpec {
    pub label: String,
    pub kind: MediaVariantKind,
    pub content_type: String,
    pub codec: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub bitrate_bps: Option<u64>,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MediaVariantStatus {
    Pending,
    Verifying,
    Ready,
    Failed,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MediaVariantCandidate {
    pub bucket: Principal,
    pub file_id: u32,
    pub content_type: String,
    pub size: u64,
    pub content_hash: Vec<u8>,
    pub generation: u64,
    pub submitted_by: Principal,
    pub submitted_at_ms: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MediaVariantSubmission {
    pub bucket: Principal,
    pub file_id: u32,
    pub attempt: u16,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MediaVariantVerification {
    pub bucket: Principal,
    pub file_id: u32,
    pub content_hash: Vec<u8>,
    pub generation: u64,
    pub codec: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub bitrate_bps: Option<u64>,
    pub verified_by: Principal,
    pub verified_at_ms: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MediaVariant {
    pub spec: MediaVariantSpec,
    pub status: MediaVariantStatus,
    pub asset_id: Option<u64>,
    pub content_hash: Option<Vec<u8>>,
    pub last_error: Option<String>,
    pub candidate: Option<MediaVariantCandidate>,
    pub submission: Option<MediaVariantSubmission>,
    pub verification: Option<MediaVariantVerification>,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TranscodeJobStatus {
    Queued,
    Running,
    AwaitingVerification,
    Ready,
    Failed,
    Cancelled,
}

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SourceRetentionPolicy {
    RetainOwnerOnly,
    AllowOriginalDownload,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TranscodeJob {
    pub id: u64,
    pub idempotency_key: String,
    pub source_asset_id: u64,
    pub source_size: u64,
    pub source_hash: Vec<u8>,
    pub source_generation: u64,
    pub source_retention: Option<SourceRetentionPolicy>,
    pub security: BucketClass,
    pub worker: Principal,
    pub status: TranscodeJobStatus,
    pub attempts: u16,
    pub variants: Vec<MediaVariant>,
    pub last_error: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BucketClass {
    Public,
    Protected,
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
struct SubmitTranscodeOutputInput {
    label: String,
    bucket: Principal,
    file_id: u32,
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
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

#[derive(Debug)]
struct CandidateSubmitError {
    message: String,
    cleanup_allowed: bool,
}

#[derive(Clone, Debug)]
pub struct WorkerOptions {
    pub hub: Principal,
    pub job_id: u64,
    pub source: PathBuf,
    pub bucket: Principal,
    pub parent: u32,
    pub work_dir: PathBuf,
    pub ffmpeg: PathBuf,
    pub ffprobe: PathBuf,
    pub retry: u8,
}

#[derive(Clone, Debug)]
pub struct CleanupOptions {
    pub hub: Principal,
    pub job_id: u64,
    pub bucket: Principal,
    pub work_dir: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProbeResult {
    pub codec: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub bitrate_bps: Option<u64>,
    pub observed_video_codec: String,
    pub observed_video_profile: Option<String>,
    pub observed_video_level: Option<u32>,
    pub observed_audio_codec: Option<String>,
    pub observed_format: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApprovalInput {
    pub label: String,
    pub output_path: PathBuf,
    pub bucket: String,
    pub file_id: u32,
    pub content_hash_hex: String,
    pub generation: u64,
    pub codec: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub bitrate_bps: Option<u64>,
    pub observed_video_codec: String,
    pub observed_video_profile: Option<String>,
    pub observed_video_level: Option<u32>,
    pub observed_audio_codec: Option<String>,
    pub observed_format: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerReport {
    pub schema_version: u16,
    pub hub: String,
    pub job_id: u64,
    pub source_hash_hex: String,
    pub status: String,
    pub approvals: Vec<ApprovalInput>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CleanupOutput {
    pub label: String,
    pub file_id: Option<u32>,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CleanupReport {
    pub schema_version: u16,
    pub hub: String,
    pub bucket: String,
    pub job_id: u64,
    pub job_status: String,
    pub deleted: Vec<CleanupOutput>,
    pub retained: Vec<CleanupOutput>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CleanupIntent {
    request_id_hex: String,
    parent: u32,
    revision: u64,
    expected_hash_hex: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct VariantJournal {
    output_path: PathBuf,
    file_id: Option<u32>,
    #[serde(default)]
    content_hash_hex: Option<String>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    cleanup: Option<CleanupIntent>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct WorkerJournal {
    schema_version: u16,
    hub: String,
    bucket: String,
    job_id: u64,
    source_hash_hex: String,
    variants: BTreeMap<String, VariantJournal>,
}

#[derive(Debug, Deserialize)]
struct FfprobeOutput {
    #[serde(default)]
    streams: Vec<FfprobeStream>,
    #[serde(default)]
    format: FfprobeFormat,
}

#[derive(Debug, Deserialize)]
struct FfprobeStream {
    #[serde(default)]
    codec_type: String,
    #[serde(default)]
    codec_name: String,
    profile: Option<String>,
    level: Option<u32>,
    width: Option<u32>,
    height: Option<u32>,
    bit_rate: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct FfprobeFormat {
    #[serde(default)]
    format_name: String,
}

pub async fn run_worker(
    agent: &Agent,
    bucket_client: &BucketClient,
    options: &WorkerOptions,
) -> Result<WorkerReport, String> {
    validate_tools(&options.ffmpeg, &options.ffprobe).await?;
    fs::create_dir_all(&options.work_dir).map_err(|error| {
        format!(
            "create transcode work directory {}: {error}",
            options.work_dir.display()
        )
    })?;

    let source_metadata = fs::metadata(&options.source)
        .map_err(|error| format!("read source {}: {error}", options.source.display()))?;
    if !source_metadata.is_file() {
        return Err(format!(
            "transcode source is not a regular file: {}",
            options.source.display()
        ));
    }
    let source_hash = hash_file(&options.source)?;
    let source_hash_hex = hex::encode(&source_hash);

    let mut job = get_job(agent, options.hub, options.job_id).await?;
    if job.source_size != source_metadata.len() || job.source_hash != source_hash {
        return Err("local source size/hash does not match the immutable Hub job snapshot".into());
    }
    match job.status {
        TranscodeJobStatus::Queued | TranscodeJobStatus::Failed => {
            job = start_job(agent, options.hub, options.job_id).await?;
        }
        TranscodeJobStatus::Running
        | TranscodeJobStatus::AwaitingVerification
        | TranscodeJobStatus::Ready => {}
        TranscodeJobStatus::Cancelled => return Err("transcode job is cancelled".into()),
    }

    let report_failures = job.status == TranscodeJobStatus::Running;
    let result = async {
        let journal_path = options.work_dir.join("worker-journal.json");
        let mut journal =
            load_or_initialize_journal(&journal_path, options, &source_hash_hex, &job.variants)?;
        let mut approvals = Vec::with_capacity(job.variants.len());

        for variant_index in 0..job.variants.len() {
            let spec = job.variants[variant_index].spec.clone();
            let output_path = journal
                .variants
                .get(&spec.label)
                .ok_or_else(|| format!("journal is missing variant {}", spec.label))?
                .output_path
                .clone();
            if !output_path.is_file() {
                let args = build_ffmpeg_args(&options.source, &output_path, &spec)?;
                run_command(&options.ffmpeg, &args, "ffmpeg transcode").await?;
            }
            let probe = probe_output(&options.ffprobe, &output_path, &spec).await?;
            let output_hash = hash_file(&output_path)?;
            let output_size = fs::metadata(&output_path)
                .map_err(|error| format!("read output {}: {error}", output_path.display()))?
                .len();

            let candidate = if let Some(candidate) = &job.variants[variant_index].candidate {
                candidate.clone()
            } else {
                if job.status != TranscodeJobStatus::Running {
                    return Err(format!(
                        "variant {} has no candidate while job is {:?}",
                        spec.label, job.status
                    ));
                }
                let file_id = match journal
                    .variants
                    .get(&spec.label)
                    .and_then(|entry| entry.file_id)
                {
                    Some(file_id) => file_id,
                    None => {
                        let file_id = upload_file_with_content_type(
                            bucket_client,
                            true,
                            options.parent,
                            output_path
                                .to_str()
                                .ok_or("transcode output path is not valid UTF-8")?,
                            options.retry,
                            Some(&spec.content_type),
                        )
                        .await?;
                        journal
                            .variants
                            .get_mut(&spec.label)
                            .expect("variant journal exists")
                            .file_id = Some(file_id);
                        let journal_variant = journal
                            .variants
                            .get_mut(&spec.label)
                            .expect("variant journal exists");
                        journal_variant.content_hash_hex = Some(hex::encode(&output_hash));
                        journal_variant.size = Some(output_size);
                        journal_variant.cleanup = None;
                        save_journal(&journal_path, &journal)?;
                        file_id
                    }
                };
                let descriptor = bucket_client.get_file_descriptor(file_id).await?;
                let descriptor_type = descriptor
                    .content_type
                    .split(';')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase();
                let descriptor_hash = descriptor.hash.as_ref().map(|hash| hash.as_ref().to_vec());
                if descriptor.id != file_id
                    || descriptor.size != output_size
                    || descriptor_hash.as_deref() != Some(output_hash.as_slice())
                    || descriptor_type != spec.content_type
                {
                    return Err(format!(
                    "Bucket descriptor for variant {} does not match local output: id={}/{}, size={}/{}, hash={}, content_type={}/{}",
                    spec.label,
                    descriptor.id,
                    file_id,
                    descriptor.size,
                    output_size,
                    descriptor_hash
                        .as_ref()
                        .map(hex::encode)
                        .unwrap_or_else(|| "none".into()),
                    descriptor.content_type,
                    spec.content_type
                    ));
                }
                match submit_candidate(
                    agent,
                    options.hub,
                    options.job_id,
                    &spec.label,
                    options.bucket,
                    file_id,
                )
                .await
                {
                    Ok(updated) => {
                        job = updated;
                        job.variants
                            .iter()
                            .find(|variant| variant.spec.label == spec.label)
                            .and_then(|variant| variant.candidate.clone())
                            .ok_or_else(|| {
                                format!(
                                    "Hub accepted variant {} without returning its candidate",
                                    spec.label
                                )
                            })?
                    }
                    Err(error) if error.cleanup_allowed => {
                        let cleanup = bucket_client.delete_file(file_id).await;
                        if cleanup.as_ref().is_ok_and(|deleted| *deleted) {
                            journal
                                .variants
                                .get_mut(&spec.label)
                                .expect("variant journal exists")
                                .file_id = None;
                            save_journal(&journal_path, &journal)?;
                            return Err(format!(
                                "Hub rejected variant {} and remote file {} was deleted: {}",
                                spec.label, file_id, error.message
                            ));
                        }
                        return Err(format!(
                        "Hub rejected variant {}: {}; remote cleanup for file {} failed: {:?}",
                        spec.label, error.message, file_id, cleanup
                    ));
                    }
                    Err(error) => return Err(error.message),
                }
            };
            if candidate.size != output_size || candidate.content_hash != output_hash {
                return Err(format!(
                    "local output for variant {} does not match the submitted candidate size/hash",
                    spec.label
                ));
            }
            if candidate.bucket == options.bucket {
                let output_hash_hex = hex::encode(&output_hash);
                let journal_variant = journal
                    .variants
                    .get_mut(&spec.label)
                    .expect("variant journal exists");
                if journal_variant
                    .file_id
                    .is_some_and(|file_id| file_id != candidate.file_id)
                {
                    return Err(format!(
                        "journal output for variant {} differs from the current Hub candidate; preserving both remote files for explicit cleanup",
                        spec.label
                    ));
                }
                if journal_variant.file_id.is_none()
                    || journal_variant.content_hash_hex.as_deref()
                        != Some(output_hash_hex.as_str())
                    || journal_variant.size != Some(output_size)
                {
                    journal_variant.file_id = Some(candidate.file_id);
                    journal_variant.content_hash_hex = Some(output_hash_hex);
                    journal_variant.size = Some(output_size);
                    journal_variant.cleanup = None;
                    save_journal(&journal_path, &journal)?;
                }
            }
            approvals.push(approval_from(&spec, &candidate, &probe, &output_path)?);
        }

        let status = format!("{:?}", job.status);
        let report = WorkerReport {
            schema_version: 1,
            hub: options.hub.to_text(),
            job_id: options.job_id,
            source_hash_hex,
            status,
            approvals,
        };
        save_json(&options.work_dir.join("owner-approval.json"), &report)?;
        Ok(report)
    }
    .await;
    match result {
        Err(error) if report_failures => {
            if let Err(report_error) =
                report_worker_failure(agent, options.hub, options.job_id, &error).await
            {
                return Err(format!(
                    "{error}; additionally failed to report the worker failure to Hub: {report_error}"
                ));
            }
            Err(error)
        }
        result => result,
    }
}

pub async fn approve_report(
    agent: &Agent,
    expected_hub: Principal,
    report_path: &Path,
    ffprobe: &Path,
) -> Result<TranscodeJob, String> {
    run_command(ffprobe, &["-version".into()], "ffprobe availability").await?;
    let report: WorkerReport = serde_json::from_slice(
        &fs::read(report_path)
            .map_err(|error| format!("read approval report {}: {error}", report_path.display()))?,
    )
    .map_err(|error| format!("decode approval report {}: {error}", report_path.display()))?;
    if report.schema_version != 1 {
        return Err(format!(
            "unsupported approval report schema {}",
            report.schema_version
        ));
    }
    if report.hub != expected_hub.to_text() {
        return Err("approval report Hub does not match --hub".into());
    }
    if report.approvals.is_empty() {
        return Err("approval report contains no variants".into());
    }
    let mut labels = std::collections::BTreeSet::new();
    let mut job = get_job(agent, expected_hub, report.job_id).await?;
    for approval in report.approvals {
        if !labels.insert(approval.label.clone()) {
            return Err(format!(
                "approval report repeats variant {}",
                approval.label
            ));
        }
        let content_hash = hex::decode(&approval.content_hash_hex).map_err(|error| {
            format!(
                "approval {} contains an invalid hash: {error}",
                approval.label
            )
        })?;
        if content_hash.len() != SHA3_256_BYTES {
            return Err(format!("approval {} hash is not SHA3-256", approval.label));
        }
        let variant = job
            .variants
            .iter()
            .find(|variant| variant.spec.label == approval.label)
            .ok_or_else(|| format!("job has no variant {}", approval.label))?;
        let candidate = variant
            .candidate
            .as_ref()
            .ok_or_else(|| format!("variant {} has no current candidate", approval.label))?;
        let bucket = Principal::from_text(&approval.bucket)
            .map_err(|error| format!("approval Bucket is invalid: {error}"))?;
        if candidate.bucket != bucket
            || candidate.file_id != approval.file_id
            || candidate.content_hash != content_hash
            || candidate.generation != approval.generation
        {
            return Err(format!(
                "approval {} does not match the current Hub candidate",
                approval.label
            ));
        }
        let local_hash = hash_file(&approval.output_path)?;
        let local_size = fs::metadata(&approval.output_path)
            .map_err(|error| {
                format!(
                    "read approval media {}: {error}",
                    approval.output_path.display()
                )
            })?
            .len();
        if local_hash != candidate.content_hash || local_size != candidate.size {
            return Err(format!(
                "approval media for {} does not match the Hub candidate size/hash",
                approval.label
            ));
        }
        let independent_probe = probe_output(ffprobe, &approval.output_path, &variant.spec).await?;
        if approval.codec != independent_probe.codec
            || approval.width != independent_probe.width
            || approval.height != independent_probe.height
            || approval.bitrate_bps != independent_probe.bitrate_bps
            || approval.observed_video_codec != independent_probe.observed_video_codec
            || approval.observed_video_profile != independent_probe.observed_video_profile
            || approval.observed_video_level != independent_probe.observed_video_level
            || approval.observed_audio_codec != independent_probe.observed_audio_codec
            || approval.observed_format != independent_probe.observed_format
        {
            return Err(format!(
                "independent ffprobe result for {} differs from the worker report",
                approval.label
            ));
        }
        let input = VerifyTranscodeOutputInput {
            label: approval.label,
            bucket,
            file_id: approval.file_id,
            content_hash,
            generation: approval.generation,
            codec: independent_probe.codec,
            width: independent_probe.width,
            height: independent_probe.height,
            bitrate_bps: independent_probe.bitrate_bps,
        };
        job = update_call::<_, Result<TranscodeJob, String>>(
            agent,
            &expected_hub,
            "admin_verify_transcode_output",
            (report.job_id, input),
        )
        .await??;
    }
    Ok(job)
}

pub async fn cleanup_outputs(
    agent: &Agent,
    bucket_client: &BucketClient,
    options: &CleanupOptions,
) -> Result<CleanupReport, String> {
    let journal_path = options.work_dir.join("worker-journal.json");
    let mut journal = load_cleanup_journal(&journal_path, options)?;
    let job = get_job(agent, options.hub, options.job_id).await?;
    if journal.source_hash_hex != hex::encode(&job.source_hash) {
        return Err("transcode journal source hash does not match the Hub job".into());
    }
    let job_labels = job
        .variants
        .iter()
        .map(|variant| variant.spec.label.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if journal
        .variants
        .keys()
        .any(|label| !job_labels.contains(label.as_str()))
    {
        return Err("transcode journal contains a variant that is not in the Hub job".into());
    }
    match job.status {
        TranscodeJobStatus::Failed | TranscodeJobStatus::Cancelled => {}
        TranscodeJobStatus::Queued
        | TranscodeJobStatus::Running
        | TranscodeJobStatus::AwaitingVerification => {
            return Err(format!(
                "transcode cleanup requires a failed or cancelled job, found {:?}",
                job.status
            ))
        }
        TranscodeJobStatus::Ready => {
            return Err(
                "ready transcode outputs are registered Assets and cannot be cleaned".into(),
            )
        }
    }
    if job
        .variants
        .iter()
        .any(|variant| variant.asset_id.is_some())
    {
        return Err("transcode cleanup refused a job containing registered Assets".into());
    }

    let current_candidates = job
        .variants
        .iter()
        .filter_map(|variant| {
            variant
                .candidate
                .as_ref()
                .map(|candidate| (candidate.bucket, candidate.file_id))
        })
        .collect::<std::collections::BTreeSet<_>>();
    let mut deleted = Vec::new();
    let mut retained = Vec::new();
    let labels = journal.variants.keys().cloned().collect::<Vec<_>>();
    for label in labels {
        let Some(file_id) = journal
            .variants
            .get(&label)
            .and_then(|variant| variant.file_id)
        else {
            retained.push(CleanupOutput {
                label,
                file_id: None,
                reason: "journal has no remote output".into(),
            });
            continue;
        };
        if job.status == TranscodeJobStatus::Failed
            && current_candidates.contains(&(options.bucket, file_id))
        {
            retained.push(CleanupOutput {
                label,
                file_id: Some(file_id),
                reason: "current failed-job candidate is retained for a safe retry".into(),
            });
            continue;
        }

        prepare_cleanup_intent(bucket_client, &journal_path, &mut journal, &label, file_id).await?;
        let intent = journal
            .variants
            .get(&label)
            .and_then(|variant| variant.cleanup.clone())
            .ok_or_else(|| format!("cleanup intent for variant {label} was not persisted"))?;
        let request_id = hex::decode(&intent.request_id_hex).map_err(|error| {
            format!("cleanup request ID for variant {label} is invalid: {error}")
        })?;
        let expected_hash = hex::decode(&intent.expected_hash_hex)
            .map_err(|error| format!("cleanup hash for variant {label} is invalid: {error}"))?;
        let expected_hash: [u8; SHA3_256_BYTES] = expected_hash
            .try_into()
            .map_err(|_| format!("cleanup hash for variant {label} is not a SHA3-256 digest"))?;
        let removed = bucket_client
            .delete_entry_if(DeleteEntryIfInput {
                request_id: ByteBuf::from(request_id),
                id: file_id,
                kind: ApiEntryKind::File,
                expected_parent: intent.parent,
                expected_revision: intent.revision,
                expected_hash: Some(expected_hash.into()),
            })
            .await
            .map_err(|error| format!("delete transcode output {file_id}: {error:?}"))?;
        let reason = if removed {
            "remote output deleted with hash/revision preconditions"
        } else {
            "remote output was already absent"
        };
        let variant = journal
            .variants
            .get_mut(&label)
            .expect("journal label was collected from the same map");
        variant.file_id = None;
        variant.cleanup = None;
        save_journal(&journal_path, &journal)?;
        deleted.push(CleanupOutput {
            label,
            file_id: Some(file_id),
            reason: reason.into(),
        });
    }

    Ok(CleanupReport {
        schema_version: 1,
        hub: options.hub.to_text(),
        bucket: options.bucket.to_text(),
        job_id: options.job_id,
        job_status: format!("{:?}", job.status),
        deleted,
        retained,
    })
}

pub async fn report_worker_failure(
    agent: &Agent,
    hub: Principal,
    job_id: u64,
    error: &str,
) -> Result<TranscodeJob, String> {
    let job = get_job(agent, hub, job_id).await?;
    if job.status != TranscodeJobStatus::Running {
        return Ok(job);
    }
    let error = bounded_error(error, 1_000);
    update_call::<_, Result<TranscodeJob, String>>(
        agent,
        &hub,
        "report_transcode_job_failure",
        (job_id, error),
    )
    .await?
}

async fn get_job(agent: &Agent, hub: Principal, id: u64) -> Result<TranscodeJob, String> {
    query_call::<_, Result<TranscodeJob, String>>(agent, &hub, "get_transcode_job", (id,)).await?
}

async fn start_job(agent: &Agent, hub: Principal, id: u64) -> Result<TranscodeJob, String> {
    update_call::<_, Result<TranscodeJob, String>>(agent, &hub, "start_transcode_job", (id,))
        .await?
}

async fn submit_candidate(
    agent: &Agent,
    hub: Principal,
    id: u64,
    label: &str,
    bucket: Principal,
    file_id: u32,
) -> Result<TranscodeJob, CandidateSubmitError> {
    let result = update_call::<_, Result<TranscodeJob, String>>(
        agent,
        &hub,
        "submit_transcode_output",
        (
            id,
            SubmitTranscodeOutputInput {
                label: label.to_string(),
                bucket,
                file_id,
            },
        ),
    )
    .await;
    match result {
        Ok(Ok(job)) => Ok(job),
        Ok(Err(message)) => Err(CandidateSubmitError {
            message,
            cleanup_allowed: true,
        }),
        Err(transport_error) => match get_job(agent, hub, id).await {
            Ok(job)
                if job.variants.iter().any(|variant| {
                    variant.spec.label == label
                        && variant.candidate.as_ref().is_some_and(|candidate| {
                            candidate.bucket == bucket && candidate.file_id == file_id
                        })
                }) =>
            {
                Ok(job)
            }
            Ok(_) => Err(CandidateSubmitError {
                message: format!(
                    "candidate submission response was lost and Hub has not exposed the candidate; rerun without deleting Bucket file {file_id}: {transport_error}"
                ),
                cleanup_allowed: false,
            }),
            Err(query_error) => Err(CandidateSubmitError {
                message: format!(
                    "candidate submission response was lost and recovery query failed; rerun without deleting Bucket file {file_id}: {transport_error}; {query_error}"
                ),
                cleanup_allowed: false,
            }),
        },
    }
}

async fn validate_tools(ffmpeg: &Path, ffprobe: &Path) -> Result<(), String> {
    run_command(ffmpeg, &["-version".into()], "ffmpeg availability").await?;
    run_command(ffprobe, &["-version".into()], "ffprobe availability").await
}

fn avc1_contract(spec: &MediaVariantSpec) -> Result<(&'static str, u8, u32), String> {
    let component = spec
        .codec
        .split(',')
        .next()
        .unwrap_or_default()
        .trim()
        .strip_prefix("avc1.")
        .ok_or_else(|| format!("variant {} has no avc1 codec component", spec.label))?;
    if component.len() != 6 {
        return Err(format!(
            "variant {} avc1 profile-level must contain six hex digits",
            spec.label
        ));
    }
    let bytes = hex::decode(component)
        .map_err(|error| format!("variant {} avc1 codec is invalid: {error}", spec.label))?;
    let profile = match bytes[0] {
        0x42 => "Baseline",
        0x4d => "Main",
        0x64 => "High",
        value => {
            return Err(format!(
                "variant {} uses unsupported H.264 profile id {value:#x}",
                spec.label
            ))
        }
    };
    Ok((profile, bytes[0], bytes[2] as u32))
}

pub fn build_ffmpeg_args(
    source: &Path,
    output: &Path,
    spec: &MediaVariantSpec,
) -> Result<Vec<String>, String> {
    let source = source
        .to_str()
        .ok_or("source path is not valid UTF-8")?
        .to_string();
    let output = output
        .to_str()
        .ok_or("output path is not valid UTF-8")?
        .to_string();
    let mut args = vec![
        "-nostdin".into(),
        "-hide_banner".into(),
        "-loglevel".into(),
        "error".into(),
        "-y".into(),
        "-i".into(),
        source,
    ];
    match &spec.kind {
        MediaVariantKind::Transcode { profile } => {
            if spec.content_type != "video/mp4" || !spec.codec.starts_with("avc1.") {
                return Err(format!(
                    "variant {} uses an unsupported video contract",
                    spec.label
                ));
            }
            let (profile_name, expected_profile_id, level) = avc1_contract(spec)?;
            let level_argument = format!("{}.{}", level / 10, level % 10);
            let profile = if profile.contains("high") {
                ("high", 0x64)
            } else if profile.contains("main") {
                ("main", 0x4d)
            } else if profile.contains("baseline") {
                ("baseline", 0x42)
            } else {
                return Err(format!(
                    "variant {} uses unsupported H.264 profile {}",
                    spec.label, profile
                ));
            };
            if profile.1 != expected_profile_id || !profile_name.eq_ignore_ascii_case(profile.0) {
                return Err(format!(
                    "variant {} profile {} does not match codec {}",
                    spec.label, profile.0, spec.codec
                ));
            }
            args.extend([
                "-map".into(),
                "0:v:0".into(),
                "-map".into(),
                "0:a?".into(),
                "-c:v".into(),
                "libx264".into(),
                "-profile:v".into(),
                profile.0.into(),
                "-level:v".into(),
                level_argument,
                "-pix_fmt".into(),
                "yuv420p".into(),
                "-force_key_frames".into(),
                "expr:gte(t,n_forced*4)".into(),
            ]);
            if let (Some(width), Some(height)) = (spec.width, spec.height) {
                args.extend(["-vf".into(), format!("scale={width}:{height}")]);
            }
            if let Some(bitrate) = spec.bitrate_bps {
                args.extend([
                    "-b:v".into(),
                    bitrate.to_string(),
                    "-maxrate".into(),
                    bitrate.to_string(),
                    "-bufsize".into(),
                    bitrate.saturating_mul(2).to_string(),
                ]);
            }
            if spec.codec.contains("mp4a.") {
                args.extend(["-c:a".into(), "aac".into(), "-b:a".into(), "128k".into()]);
            } else {
                args.push("-an".into());
            }
            args.extend([
                "-movflags".into(),
                "+frag_keyframe+empty_moov+default_base_moof+global_sidx+skip_trailer".into(),
                "-frag_duration".into(),
                "4000000".into(),
                output,
            ]);
        }
        MediaVariantKind::Poster | MediaVariantKind::Thumbnail => {
            let encoder = match spec.content_type.as_str() {
                "image/webp" if spec.codec == "webp" => "libwebp",
                "image/jpeg" if spec.codec == "jpeg" || spec.codec == "mjpeg" => "mjpeg",
                "image/png" if spec.codec == "png" => "png",
                _ => {
                    return Err(format!(
                        "variant {} uses an unsupported image contract",
                        spec.label
                    ))
                }
            };
            let filter = match (spec.width, spec.height) {
                (Some(width), Some(height)) => format!("thumbnail,scale={width}:{height}"),
                (None, None) => "thumbnail".into(),
                _ => return Err("variant dimensions are incomplete".into()),
            };
            args.extend([
                "-frames:v".into(),
                "1".into(),
                "-an".into(),
                "-vf".into(),
                filter,
                "-c:v".into(),
                encoder.into(),
                output,
            ]);
        }
    }
    Ok(args)
}

async fn probe_output(
    ffprobe: &Path,
    output: &Path,
    spec: &MediaVariantSpec,
) -> Result<ProbeResult, String> {
    let args = vec![
        "-v".into(),
        "error".into(),
        "-show_entries".into(),
        "stream=codec_type,codec_name,profile,level,width,height,bit_rate:format=format_name"
            .into(),
        "-of".into(),
        "json".into(),
        output
            .to_str()
            .ok_or("output path is not valid UTF-8")?
            .into(),
    ];
    let bytes = run_command_output(ffprobe, &args, "ffprobe media inspection").await?;
    let output: FfprobeOutput =
        serde_json::from_slice(&bytes).map_err(|error| format!("decode ffprobe JSON: {error}"))?;
    validate_probe(spec, &output)
}

fn validate_probe(spec: &MediaVariantSpec, output: &FfprobeOutput) -> Result<ProbeResult, String> {
    let video = output
        .streams
        .iter()
        .find(|stream| stream.codec_type == "video")
        .ok_or_else(|| format!("variant {} has no video/image stream", spec.label))?;
    let audio = output
        .streams
        .iter()
        .find(|stream| stream.codec_type == "audio");
    if video.width != spec.width || video.height != spec.height {
        return Err(format!(
            "variant {} dimensions {:?}x{:?} do not match {:?}x{:?}",
            spec.label, video.width, video.height, spec.width, spec.height
        ));
    }
    let (expected_video_codec, expected_format, requires_aac, expected_profile, expected_level) =
        match &spec.kind {
            MediaVariantKind::Transcode { .. } => {
                let (profile, _, level) = avc1_contract(spec)?;
                (
                    "h264",
                    "mp4",
                    spec.codec.contains("mp4a."),
                    Some(profile),
                    Some(level),
                )
            }
            MediaVariantKind::Poster | MediaVariantKind::Thumbnail => {
                let codec = match spec.codec.as_str() {
                    "webp" => "webp",
                    "jpeg" | "mjpeg" => "mjpeg",
                    "png" => "png",
                    _ => return Err(format!("unsupported image codec {}", spec.codec)),
                };
                (codec, "", false, None, None)
            }
        };
    if video.codec_name != expected_video_codec {
        return Err(format!(
            "variant {} codec {} does not match {}",
            spec.label, video.codec_name, expected_video_codec
        ));
    }
    if expected_profile.is_some_and(|profile| {
        video
            .profile
            .as_deref()
            .is_none_or(|observed| !observed.eq_ignore_ascii_case(profile))
    }) || expected_level.is_some_and(|level| video.level != Some(level))
    {
        return Err(format!(
            "variant {} H.264 profile/level {:?}/{:?} does not match {:?}/{:?}",
            spec.label, video.profile, video.level, expected_profile, expected_level
        ));
    }
    if !expected_format.is_empty()
        && !output
            .format
            .format_name
            .split(',')
            .any(|v| v == expected_format)
    {
        return Err(format!(
            "variant {} container {} is not {}",
            spec.label, output.format.format_name, expected_format
        ));
    }
    if requires_aac && audio.is_none_or(|stream| stream.codec_name != "aac") {
        return Err(format!(
            "variant {} requires an AAC audio stream",
            spec.label
        ));
    }
    let observed_bitrate = match spec.bitrate_bps {
        Some(maximum) => {
            let observed = video
                .bit_rate
                .as_deref()
                .ok_or_else(|| format!("variant {} has no video bitrate", spec.label))?
                .parse::<u64>()
                .map_err(|error| format!("variant {} bitrate is invalid: {error}", spec.label))?;
            if observed == 0 || observed > maximum {
                return Err(format!(
                    "variant {} bitrate {} exceeds contract maximum {}",
                    spec.label, observed, maximum
                ));
            }
            Some(observed)
        }
        None => None,
    };
    Ok(ProbeResult {
        codec: spec.codec.clone(),
        width: video.width,
        height: video.height,
        bitrate_bps: observed_bitrate,
        observed_video_codec: video.codec_name.clone(),
        observed_video_profile: video.profile.clone(),
        observed_video_level: video.level,
        observed_audio_codec: audio.map(|stream| stream.codec_name.clone()),
        observed_format: output.format.format_name.clone(),
    })
}

fn approval_from(
    spec: &MediaVariantSpec,
    candidate: &MediaVariantCandidate,
    probe: &ProbeResult,
    output_path: &Path,
) -> Result<ApprovalInput, String> {
    if candidate.content_hash.len() != SHA3_256_BYTES {
        return Err(format!(
            "variant {} candidate does not contain a SHA3-256 hash",
            spec.label
        ));
    }
    Ok(ApprovalInput {
        label: spec.label.clone(),
        output_path: output_path.to_path_buf(),
        bucket: candidate.bucket.to_text(),
        file_id: candidate.file_id,
        content_hash_hex: hex::encode(&candidate.content_hash),
        generation: candidate.generation,
        codec: probe.codec.clone(),
        width: probe.width,
        height: probe.height,
        bitrate_bps: probe.bitrate_bps,
        observed_video_codec: probe.observed_video_codec.clone(),
        observed_video_profile: probe.observed_video_profile.clone(),
        observed_video_level: probe.observed_video_level,
        observed_audio_codec: probe.observed_audio_codec.clone(),
        observed_format: probe.observed_format.clone(),
    })
}

fn load_or_initialize_journal(
    path: &Path,
    options: &WorkerOptions,
    source_hash_hex: &str,
    variants: &[MediaVariant],
) -> Result<WorkerJournal, String> {
    if path.is_file() {
        let mut journal = read_journal(path)?;
        if !supported_journal_schema(journal.schema_version)
            || journal.hub != options.hub.to_text()
            || journal.bucket != options.bucket.to_text()
            || journal.job_id != options.job_id
            || journal.source_hash_hex != source_hash_hex
        {
            return Err("existing transcode journal does not match this invocation".into());
        }
        if journal.schema_version != JOURNAL_SCHEMA_VERSION {
            journal.schema_version = JOURNAL_SCHEMA_VERSION;
            save_journal(path, &journal)?;
        }
        return Ok(journal);
    }

    let mut outputs = BTreeMap::new();
    for (index, variant) in variants.iter().enumerate() {
        let extension = output_extension(&variant.spec)?;
        let safe_label = sanitize_label(&variant.spec.label);
        outputs.insert(
            variant.spec.label.clone(),
            VariantJournal {
                output_path: options
                    .work_dir
                    .join(format!("{index:02}-{safe_label}.{extension}")),
                file_id: None,
                content_hash_hex: None,
                size: None,
                cleanup: None,
            },
        );
    }
    let journal = WorkerJournal {
        schema_version: JOURNAL_SCHEMA_VERSION,
        hub: options.hub.to_text(),
        bucket: options.bucket.to_text(),
        job_id: options.job_id,
        source_hash_hex: source_hash_hex.into(),
        variants: outputs,
    };
    save_journal(path, &journal)?;
    Ok(journal)
}

fn load_cleanup_journal(path: &Path, options: &CleanupOptions) -> Result<WorkerJournal, String> {
    let mut journal = read_journal(path)?;
    if !supported_journal_schema(journal.schema_version)
        || journal.hub != options.hub.to_text()
        || journal.bucket != options.bucket.to_text()
        || journal.job_id != options.job_id
    {
        return Err("existing transcode journal does not match this cleanup invocation".into());
    }
    if journal.schema_version != JOURNAL_SCHEMA_VERSION {
        journal.schema_version = JOURNAL_SCHEMA_VERSION;
        save_journal(path, &journal)?;
    }
    Ok(journal)
}

fn read_journal(path: &Path) -> Result<WorkerJournal, String> {
    serde_json::from_slice(
        &fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?,
    )
    .map_err(|error| format!("decode {}: {error}", path.display()))
}

fn supported_journal_schema(schema_version: u16) -> bool {
    matches!(
        schema_version,
        PREVIOUS_JOURNAL_SCHEMA_VERSION | JOURNAL_SCHEMA_VERSION
    )
}

async fn prepare_cleanup_intent(
    bucket_client: &BucketClient,
    journal_path: &Path,
    journal: &mut WorkerJournal,
    label: &str,
    file_id: u32,
) -> Result<(), String> {
    let journal_variant = journal
        .variants
        .get(label)
        .ok_or_else(|| format!("journal is missing variant {label}"))?;
    if journal_variant.cleanup.is_some() {
        return Ok(());
    }
    let expected_hash = match &journal_variant.content_hash_hex {
        Some(hash) => hex::decode(hash)
            .map_err(|error| format!("journal hash for variant {label} is invalid: {error}"))?,
        None if journal_variant.output_path.is_file() => hash_file(&journal_variant.output_path)?,
        None => {
            return Err(format!(
                "variant {label} has neither a recorded hash nor a local output; cleanup refused"
            ))
        }
    };
    if expected_hash.len() != SHA3_256_BYTES {
        return Err(format!("journal hash for variant {label} is not SHA3-256"));
    }
    let expected_size = match journal_variant.size {
        Some(size) => size,
        None => fs::metadata(&journal_variant.output_path)
            .map_err(|error| {
                format!(
                    "read cleanup output {}: {error}",
                    journal_variant.output_path.display()
                )
            })?
            .len(),
    };
    if journal_variant.output_path.is_file() {
        let local_hash = hash_file(&journal_variant.output_path)?;
        let local_size = fs::metadata(&journal_variant.output_path)
            .map_err(|error| {
                format!(
                    "read cleanup output {}: {error}",
                    journal_variant.output_path.display()
                )
            })?
            .len();
        if local_hash != expected_hash || local_size != expected_size {
            return Err(format!(
                "local output for variant {label} no longer matches the journal size/hash; cleanup refused"
            ));
        }
    }
    let remote = bucket_client
        .get_file_info(file_id)
        .await
        .map_err(|error| format!("inspect transcode output {file_id} before cleanup: {error}"))?;
    let remote_hash = remote.hash.as_ref().map(|hash| hash.as_ref().to_vec());
    if remote.id != file_id
        || remote.size != expected_size
        || remote_hash.as_deref() != Some(expected_hash.as_slice())
    {
        return Err(format!(
            "remote file {file_id} no longer matches the journal size/hash; cleanup refused"
        ));
    }
    let request_id = random_request_id()?;
    let variant = journal
        .variants
        .get_mut(label)
        .expect("journal variant was checked above");
    variant.content_hash_hex = Some(hex::encode(&expected_hash));
    variant.size = Some(expected_size);
    variant.cleanup = Some(CleanupIntent {
        request_id_hex: hex::encode(request_id),
        parent: remote.parent,
        revision: remote.revision,
        expected_hash_hex: hex::encode(expected_hash),
    });
    save_journal(journal_path, journal)
}

fn random_request_id() -> Result<Vec<u8>, String> {
    let mut request_id = vec![0u8; 16];
    SystemRandom::new()
        .fill(&mut request_id)
        .map_err(|_| "failed to generate transcode cleanup request ID".to_string())?;
    Ok(request_id)
}

fn bounded_error(error: &str, maximum_bytes: usize) -> String {
    let mut bounded = String::new();
    for character in error.trim().chars() {
        if bounded.len() + character.len_utf8() > maximum_bytes {
            break;
        }
        bounded.push(character);
    }
    if bounded.is_empty() {
        "transcode worker failed without an error message".into()
    } else {
        bounded
    }
}

fn output_extension(spec: &MediaVariantSpec) -> Result<&'static str, String> {
    match spec.content_type.as_str() {
        "video/mp4" => Ok("mp4"),
        "image/webp" => Ok("webp"),
        "image/jpeg" => Ok("jpg"),
        "image/png" => Ok("png"),
        _ => Err(format!(
            "variant {} content type {} is unsupported",
            spec.label, spec.content_type
        )),
    }
}

fn sanitize_label(label: &str) -> String {
    let safe = label
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if safe.is_empty() {
        "variant".into()
    } else {
        safe
    }
}

fn hash_file(path: &Path) -> Result<Vec<u8>, String> {
    let mut file =
        fs::File::open(path).map_err(|error| format!("open {}: {error}", path.display()))?;
    let mut buffer = vec![0; 2 * 1024 * 1024];
    let mut hasher = Sha3_256::new();
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hasher.finalize().to_vec())
}

async fn run_command(program: &Path, args: &[String], context: &str) -> Result<(), String> {
    let status = Command::new(program)
        .args(args)
        .status()
        .await
        .map_err(|error| format!("{context}: launch {}: {error}", program.display()))?;
    if !status.success() {
        return Err(format!(
            "{context}: {} exited with {}",
            program.display(),
            status
        ));
    }
    Ok(())
}

async fn run_command_output(
    program: &Path,
    args: &[String],
    context: &str,
) -> Result<Vec<u8>, String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .await
        .map_err(|error| format!("{context}: launch {}: {error}", program.display()))?;
    if !output.status.success() {
        return Err(format!(
            "{context}: {} exited with {}: {}",
            program.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(output.stdout)
}

fn save_journal(path: &Path, journal: &WorkerJournal) -> Result<(), String> {
    save_json(path, journal)
}

fn save_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| format!("encode {}: {error}", path.display()))?;
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, bytes)
        .map_err(|error| format!("write {}: {error}", temporary.display()))?;
    fs::rename(&temporary, path).map_err(|error| format!("replace {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn video_spec() -> MediaVariantSpec {
        MediaVariantSpec {
            label: "720p".into(),
            kind: MediaVariantKind::Transcode {
                profile: "h264-main".into(),
            },
            content_type: "video/mp4".into(),
            codec: "avc1.4d401f,mp4a.40.2".into(),
            width: Some(1_280),
            height: Some(720),
            bitrate_bps: Some(3_000_000),
        }
    }

    #[test]
    fn ffmpeg_plan_is_bounded_by_the_variant_contract() {
        let args = build_ffmpeg_args(
            Path::new("source.mov"),
            Path::new("output.mp4"),
            &video_spec(),
        )
        .unwrap();
        assert!(args.windows(2).any(|pair| pair == ["-c:v", "libx264"]));
        assert!(args.windows(2).any(|pair| pair == ["-profile:v", "main"]));
        assert!(args.windows(2).any(|pair| pair == ["-level:v", "3.1"]));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["-vf", "scale=1280:720"]));
        assert!(args.windows(2).any(|pair| pair == ["-maxrate", "3000000"]));
        assert!(args.windows(2).any(|pair| pair == ["-c:a", "aac"]));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["-force_key_frames", "expr:gte(t,n_forced*4)"]));
        assert!(args.iter().any(|argument| argument.contains("global_sidx")));
    }

    #[test]
    fn ffprobe_contract_rejects_wrong_codec_dimensions_and_bitrate() {
        let valid = FfprobeOutput {
            streams: vec![
                FfprobeStream {
                    codec_type: "video".into(),
                    codec_name: "h264".into(),
                    profile: Some("Main".into()),
                    level: Some(31),
                    width: Some(1_280),
                    height: Some(720),
                    bit_rate: Some("2800000".into()),
                },
                FfprobeStream {
                    codec_type: "audio".into(),
                    codec_name: "aac".into(),
                    profile: Some("LC".into()),
                    level: None,
                    width: None,
                    height: None,
                    bit_rate: Some("128000".into()),
                },
            ],
            format: FfprobeFormat {
                format_name: "mov,mp4,m4a,3gp,3g2,mj2".into(),
            },
        };
        let probe = validate_probe(&video_spec(), &valid).unwrap();
        assert_eq!(probe.bitrate_bps, Some(2_800_000));

        let mut wrong_codec = valid;
        wrong_codec.streams[0].codec_name = "vp9".into();
        assert!(validate_probe(&video_spec(), &wrong_codec).is_err());
        wrong_codec.streams[0].codec_name = "h264".into();
        wrong_codec.streams[0].width = Some(640);
        assert!(validate_probe(&video_spec(), &wrong_codec).is_err());
        wrong_codec.streams[0].width = Some(1_280);
        wrong_codec.streams[0].bit_rate = Some("3000001".into());
        assert!(validate_probe(&video_spec(), &wrong_codec).is_err());
    }

    #[test]
    fn labels_and_extensions_cannot_escape_the_work_directory() {
        assert_eq!(sanitize_label("../../720 p"), "______720_p");
        assert_eq!(output_extension(&video_spec()).unwrap(), "mp4");
        let mut unsupported = video_spec();
        unsupported.content_type = "video/webm".into();
        assert!(output_extension(&unsupported).is_err());
    }

    #[test]
    fn worker_failure_is_bounded_on_utf8_boundaries() {
        let error = "错".repeat(400);
        let bounded = bounded_error(&error, 1_000);
        assert!(bounded.len() <= 1_000);
        assert!(!bounded.is_empty());
        assert_eq!(
            bounded_error("  ", 1_000),
            "transcode worker failed without an error message"
        );
    }
}
