use super::{persistence, PlanAction};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::{collections::BTreeSet, fs, path::Path};

const JOURNAL_VERSION: u8 = 2;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum ActionState {
    Pending,
    InProgress,
    Completed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum ActionKind {
    CreateDirectory,
    UploadFile,
    ReplaceFile,
    DeleteFile,
    DeleteDirectory,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct ChunkRange {
    pub start: u32,
    pub end: u32,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct UploadCheckpoint {
    pub fingerprint: Vec<u8>,
    pub begin_request_id: Vec<u8>,
    pub session_id: Option<Vec<u8>>,
    pub uploaded_ranges: Vec<ChunkRange>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct JournalAction {
    kind: ActionKind,
    path: String,
    state: ActionState,
    remote_id: Option<u32>,
    #[serde(default)]
    upload: Option<UploadCheckpoint>,
}

#[derive(Debug, Deserialize, Serialize)]
struct JournalData {
    version: u8,
    bucket: String,
    remote_parent: u32,
    local_root: String,
    local_manifest_hash: String,
    actions: Vec<JournalAction>,
}

pub struct RecoveryJournal {
    path: std::path::PathBuf,
    data: JournalData,
}

impl RecoveryJournal {
    pub fn start(
        bucket: &str,
        remote_parent: u32,
        local_root: &Path,
        local_manifest_hash: &[u8; 32],
        actions: &[PlanAction],
    ) -> Result<Self, String> {
        let path = journal_path(bucket, remote_parent, local_root);
        let mut data = JournalData {
            version: JOURNAL_VERSION,
            bucket: bucket.to_string(),
            remote_parent,
            local_root: local_root.to_string_lossy().into_owned(),
            local_manifest_hash: hex::encode(local_manifest_hash),
            actions: actions.iter().filter_map(journal_action).collect(),
        };
        if let Ok(bytes) = fs::read(&path) {
            if let Ok(previous) = serde_json::from_slice::<JournalData>(&bytes) {
                if previous.version == JOURNAL_VERSION
                    && previous.bucket == data.bucket
                    && previous.remote_parent == data.remote_parent
                    && previous.local_root == data.local_root
                    && previous.local_manifest_hash == data.local_manifest_hash
                {
                    for action in &mut data.actions {
                        if let Some(old) = previous
                            .actions
                            .iter()
                            .find(|old| old.kind == action.kind && old.path == action.path)
                        {
                            action.upload = old.upload.clone();
                        }
                    }
                }
            }
        }
        let journal = Self { path, data };
        journal.save()?;
        Ok(journal)
    }

    pub fn mark_started(&mut self, action: &PlanAction) -> Result<(), String> {
        self.set_state(action, ActionState::InProgress, None)
    }

    pub fn mark_completed(
        &mut self,
        action: &PlanAction,
        remote_id: Option<u32>,
    ) -> Result<(), String> {
        self.set_state(action, ActionState::Completed, remote_id)
    }

    pub fn prepare_upload(
        &mut self,
        action: &PlanAction,
        fingerprint: [u8; 32],
        begin_request_id: Vec<u8>,
    ) -> Result<UploadCheckpoint, String> {
        let entry = self.action_mut(action)?;
        if entry
            .upload
            .as_ref()
            .is_some_and(|upload| upload.fingerprint == fingerprint)
        {
            return Ok(entry.upload.clone().expect("checked upload checkpoint"));
        }
        let upload = UploadCheckpoint {
            fingerprint: fingerprint.to_vec(),
            begin_request_id,
            session_id: None,
            uploaded_ranges: Vec::new(),
        };
        entry.upload = Some(upload.clone());
        self.save()?;
        Ok(upload)
    }

    pub fn set_upload_session(
        &mut self,
        action: &PlanAction,
        session_id: Vec<u8>,
    ) -> Result<(), String> {
        let upload = self
            .action_mut(action)?
            .upload
            .as_mut()
            .ok_or_else(|| "upload checkpoint was not prepared".to_string())?;
        upload.session_id = Some(session_id);
        self.save()
    }

    pub fn set_uploaded_chunks(
        &mut self,
        action: &PlanAction,
        uploaded_chunks: BTreeSet<u32>,
    ) -> Result<(), String> {
        let upload = self
            .action_mut(action)?
            .upload
            .as_mut()
            .ok_or_else(|| "upload checkpoint was not prepared".to_string())?;
        upload.uploaded_ranges = chunk_ranges(&uploaded_chunks);
        self.save()
    }

    pub fn mark_uploaded_chunk(
        &mut self,
        action: &PlanAction,
        chunk_index: u32,
    ) -> Result<(), String> {
        let upload = self
            .action_mut(action)?
            .upload
            .as_mut()
            .ok_or_else(|| "upload checkpoint was not prepared".to_string())?;
        insert_chunk(&mut upload.uploaded_ranges, chunk_index);
        self.save()
    }

    pub fn reset_upload(&mut self, action: &PlanAction) -> Result<(), String> {
        self.action_mut(action)?.upload = None;
        self.save()
    }

    pub fn finish(self) -> Result<(), String> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(format!(
                "failed to remove recovery journal {:?}: {}",
                self.path, err
            )),
        }
    }

    fn set_state(
        &mut self,
        action: &PlanAction,
        state: ActionState,
        remote_id: Option<u32>,
    ) -> Result<(), String> {
        let entry = self.action_mut(action)?;
        entry.state = state;
        entry.remote_id = remote_id.or(entry.remote_id);
        self.save()
    }

    fn action_mut(&mut self, action: &PlanAction) -> Result<&mut JournalAction, String> {
        let (kind, path) = action_identity(action)
            .ok_or_else(|| "unsupported action in recovery journal".to_string())?;
        self.data
            .actions
            .iter_mut()
            .find(|entry| entry.kind == kind && entry.path == path)
            .ok_or_else(|| format!("missing recovery journal action {}", path))
    }

    fn save(&self) -> Result<(), String> {
        let bytes = serde_json::to_vec(&self.data)
            .map_err(|err| format!("failed to serialize recovery journal: {}", err))?;
        persistence::atomic_write(&self.path, &bytes)
    }
}

pub fn recovery_warning(
    bucket: &str,
    remote_parent: u32,
    local_root: &Path,
    local_manifest_hash: &[u8; 32],
) -> Option<String> {
    let path = journal_path(bucket, remote_parent, local_root);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
        Err(err) => {
            return Some(format!(
                "could not read recovery journal {:?}: {}; current state was fully rescanned",
                path, err
            ));
        }
    };
    let journal = match serde_json::from_slice::<JournalData>(&bytes) {
        Ok(journal) if journal.version == JOURNAL_VERSION => journal,
        Ok(_) => {
            return Some(
                "ignored a recovery journal with an unsupported version; current state was fully rescanned"
                    .to_string(),
            );
        }
        Err(err) => {
            return Some(format!(
                "ignored an invalid recovery journal: {}; current state was fully rescanned",
                err
            ));
        }
    };

    if journal.local_manifest_hash == hex::encode(local_manifest_hash) {
        Some(
            "found an incomplete prior sync; local and remote state were fully rescanned before replanning"
                .to_string(),
        )
    } else {
        Some(
            "found a stale recovery journal, but the local manifest changed; the old plan was ignored"
                .to_string(),
        )
    }
}

fn journal_action(action: &PlanAction) -> Option<JournalAction> {
    let (kind, path) = action_identity(action)?;
    Some(JournalAction {
        kind,
        path: path.to_string(),
        state: ActionState::Pending,
        remote_id: None,
        upload: None,
    })
}

fn action_identity(action: &PlanAction) -> Option<(ActionKind, &str)> {
    match action {
        PlanAction::CreateDirectory { path } => Some((ActionKind::CreateDirectory, path)),
        PlanAction::UploadFile { path, .. } => Some((ActionKind::UploadFile, path)),
        PlanAction::ReplaceFile { path, .. } => Some((ActionKind::ReplaceFile, path)),
        PlanAction::DeleteFile { path, .. } => Some((ActionKind::DeleteFile, path)),
        PlanAction::DeleteDirectory { path, .. } => Some((ActionKind::DeleteDirectory, path)),
        PlanAction::Conflict { .. } => None,
    }
}

fn journal_path(bucket: &str, remote_parent: u32, local_root: &Path) -> std::path::PathBuf {
    let mut hasher = Sha3_256::new();
    hash_component(&mut hasher, bucket.as_bytes());
    hash_component(&mut hasher, &remote_parent.to_be_bytes());
    hash_component(
        &mut hasher,
        local_root.as_os_str().to_string_lossy().as_bytes(),
    );
    persistence::user_cache_dir()
        .join("sync-journals")
        .join(format!("{}.json", hex::encode(hasher.finalize())))
}

fn hash_component(hasher: &mut Sha3_256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn chunk_ranges(chunks: &BTreeSet<u32>) -> Vec<ChunkRange> {
    let mut ranges: Vec<ChunkRange> = Vec::new();
    for chunk in chunks {
        if let Some(last) = ranges.last_mut() {
            if last.end.saturating_add(1) == *chunk {
                last.end = *chunk;
                continue;
            }
        }
        ranges.push(ChunkRange {
            start: *chunk,
            end: *chunk,
        });
    }
    ranges
}

fn insert_chunk(ranges: &mut Vec<ChunkRange>, chunk: u32) {
    let mut merged = ChunkRange {
        start: chunk,
        end: chunk,
    };
    let mut index = 0;
    while index < ranges.len() {
        if ranges[index].end.saturating_add(1) < merged.start {
            index += 1;
            continue;
        }
        if merged.end.saturating_add(1) < ranges[index].start {
            break;
        }
        let existing = ranges.remove(index);
        merged.start = merged.start.min(existing.start);
        merged.end = merged.end.max(existing.end);
    }
    ranges.insert(index, merged);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn uploaded_chunks_are_compacted_into_ranges() {
        let mut ranges = Vec::new();
        for chunk in [2, 0, 4, 1, 3, 3] {
            insert_chunk(&mut ranges, chunk);
        }
        assert_eq!(ranges, vec![ChunkRange { start: 0, end: 4 }]);
    }

    #[test]
    fn journal_tracks_actions_and_detects_changed_manifest() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("ic-oss-journal-{}-{}", std::process::id(), suffix));
        fs::create_dir_all(&root).unwrap();
        let actions = vec![PlanAction::UploadFile {
            path: "a.txt".to_string(),
            size: 1,
        }];
        let hash = [1u8; 32];
        let mut journal = RecoveryJournal::start("bucket", 7, &root, &hash, &actions).unwrap();
        journal.mark_started(&actions[0]).unwrap();
        let checkpoint = journal
            .prepare_upload(&actions[0], [3u8; 32], vec![4u8; 16])
            .unwrap();
        assert_eq!(checkpoint.begin_request_id, vec![4u8; 16]);
        journal
            .set_upload_session(&actions[0], vec![5u8; 32])
            .unwrap();
        journal.mark_uploaded_chunk(&actions[0], 2).unwrap();
        drop(journal);

        let mut journal = RecoveryJournal::start("bucket", 7, &root, &hash, &actions).unwrap();
        let checkpoint = journal
            .prepare_upload(&actions[0], [3u8; 32], vec![6u8; 16])
            .unwrap();
        assert_eq!(checkpoint.begin_request_id, vec![4u8; 16]);
        assert_eq!(checkpoint.session_id, Some(vec![5u8; 32]));
        assert_eq!(
            checkpoint.uploaded_ranges,
            vec![ChunkRange { start: 2, end: 2 }]
        );
        journal.mark_completed(&actions[0], Some(9)).unwrap();

        assert!(recovery_warning("bucket", 7, &root, &hash)
            .unwrap()
            .contains("incomplete prior sync"));
        assert!(recovery_warning("bucket", 7, &root, &[2u8; 32])
            .unwrap()
            .contains("local manifest changed"));

        journal.finish().unwrap();
        assert!(recovery_warning("bucket", 7, &root, &hash).is_none());
        fs::remove_dir_all(root).unwrap();
    }
}
