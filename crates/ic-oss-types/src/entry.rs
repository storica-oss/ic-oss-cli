use candid::CandidType;
use serde::{Deserialize, Serialize};
use serde_bytes::{ByteArray, ByteBuf};

use crate::file::valid_file_name;

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum EntryKind {
    File,
    Folder,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntryRef {
    pub kind: EntryKind,
    pub id: u32,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntryInfoV2 {
    pub kind: EntryKind,
    pub id: u32,
    pub parent: u32,
    pub name: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub status: i8,
    pub revision: u64,
    pub size: Option<u64>,
    pub filled: Option<u64>,
    pub hash: Option<ByteArray<32>>,
    pub content_type: Option<String>,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SyncError {
    InvalidInput(String),
    Unauthorized(String),
    PermissionDenied(String),
    NotFound(String),
    Conflict {
        message: String,
        entries: Vec<EntryRef>,
    },
    LimitExceeded(String),
    Internal(String),
}

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct GetEntryInput {
    pub parent: u32,
    pub name: String,
}

impl GetEntryInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        if !valid_file_name(&self.name) {
            return Err(SyncError::InvalidInput("invalid entry name".to_string()));
        }
        Ok(())
    }
}

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct EnsureFolderInput {
    pub request_id: ByteBuf,
    pub parent: u32,
    pub name: String,
}

impl EnsureFolderInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        validate_request_id(&self.request_id)?;
        if !valid_file_name(&self.name) {
            return Err(SyncError::InvalidInput("invalid folder name".to_string()));
        }
        Ok(())
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EnsureFolderOutput {
    pub id: u32,
    pub created: bool,
    pub created_at: u64,
    pub revision: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct DeleteEntryIfInput {
    pub request_id: ByteBuf,
    pub id: u32,
    pub kind: EntryKind,
    pub expected_parent: u32,
    pub expected_revision: u64,
    pub expected_hash: Option<ByteArray<32>>,
}

impl DeleteEntryIfInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        validate_request_id(&self.request_id)?;
        if self.kind == EntryKind::Folder && self.expected_hash.is_some() {
            return Err(SyncError::InvalidInput(
                "expected_hash is only valid for files".to_string(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn validate_request_id(request_id: &ByteBuf) -> Result<(), SyncError> {
    if request_id.is_empty() || request_id.len() > 64 {
        return Err(SyncError::InvalidInput(
            "request_id must contain 1 to 64 bytes".to_string(),
        ));
    }
    Ok(())
}

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MigrationState {
    Legacy,
    Migrating,
    Ready,
    Failed,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BucketCapabilities {
    pub api_version: u16,
    pub storage_version: u16,
    pub unique_names: bool,
    pub get_entry: bool,
    pub ensure_folder: bool,
    pub conditional_delete: bool,
    pub upload_sessions: bool,
    pub atomic_commit: bool,
    pub incremental_gc: bool,
    pub manifest: bool,
    pub batch_operations: Option<bool>,
    pub reader_grants: Option<bool>,
    pub http_read_modes: Option<bool>,
    pub storage_metrics: Option<bool>,
    pub migration_state: MigrationState,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_entry_inputs() {
        assert!(GetEntryInput {
            parent: 0,
            name: "docs".to_string(),
        }
        .validate()
        .is_ok());
        assert!(GetEntryInput {
            parent: 0,
            name: "../docs".to_string(),
        }
        .validate()
        .is_err());
        assert!(EnsureFolderInput {
            request_id: ByteBuf::from(vec![1]),
            parent: 0,
            name: "docs".to_string(),
        }
        .validate()
        .is_ok());
        assert!(EnsureFolderInput {
            request_id: ByteBuf::new(),
            parent: 0,
            name: "docs".to_string(),
        }
        .validate()
        .is_err());
        assert!(DeleteEntryIfInput {
            request_id: ByteBuf::from(vec![1]),
            id: 1,
            kind: EntryKind::Folder,
            expected_parent: 0,
            expected_revision: 1,
            expected_hash: Some(ByteArray::from([1; 32])),
        }
        .validate()
        .is_err());
    }
}
