use candid::CandidType;
use serde::{Deserialize, Serialize};

use crate::entry::{EntryInfoV2, EntryKind, MigrationState, SyncError};

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntryCursor {
    pub parent_revision: u64,
    pub kind: EntryKind,
    pub id: u32,
}

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct ListEntriesInput {
    pub parent: u32,
    pub cursor: Option<EntryCursor>,
    pub take: Option<u16>,
}

impl ListEntriesInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        if self.take == Some(0) {
            return Err(SyncError::InvalidInput(
                "take must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ListEntriesOutput {
    pub entries: Vec<EntryInfoV2>,
    pub next: Option<EntryCursor>,
    pub parent_revision: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestFrame {
    pub folder_id: u32,
    pub path: String,
    pub after: Option<crate::entry::EntryRef>,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SubtreeManifestCursor {
    pub revision: u64,
    pub stack: Vec<ManifestFrame>,
}

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct SubtreeManifestInput {
    pub root: u32,
    pub cursor: Option<SubtreeManifestCursor>,
    pub take: Option<u16>,
}

impl SubtreeManifestInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        if self.take == Some(0) {
            return Err(SyncError::InvalidInput(
                "take must be greater than zero".to_string(),
            ));
        }
        if self
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.stack.len() > 1024)
        {
            return Err(SyncError::LimitExceeded(
                "manifest cursor depth exceeds 1024".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestEntry {
    pub path: String,
    pub entry: EntryInfoV2,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SubtreeManifestOutput {
    pub entries: Vec<ManifestEntry>,
    pub next: Option<SubtreeManifestCursor>,
    pub revision: u64,
}

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct MigrateDirectoryStorageInput {
    pub max_items: Option<u16>,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MigrateDirectoryStorageOutput {
    pub state: MigrationState,
    pub processed: u16,
    pub folder_cursor: Option<u32>,
    pub file_cursor: Option<u32>,
    pub error: Option<String>,
}

#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DirectoryStorageHealth {
    pub legacy_folders: u64,
    pub stable_folders: u64,
    pub stable_children: u64,
    pub stable_names: u64,
    pub duplicate_names: u64,
    pub dangling_entries: u64,
    pub migration_error: Option<String>,
}
