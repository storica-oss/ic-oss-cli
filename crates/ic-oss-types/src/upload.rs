use candid::CandidType;
use serde::{Deserialize, Serialize};
use serde_bytes::{ByteArray, ByteBuf};

use crate::{
    entry::{validate_request_id, SyncError},
    file::valid_file_name,
    MapValue,
};

pub const UPLOAD_SESSION_ID_SIZE: usize = 32;

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UploadHealth {
    pub active_sessions: u64,
    pub max_active_sessions: u16,
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct ReplaceFileInput {
    pub id: u32,
    pub expected_revision: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct BeginUploadInput {
    pub request_id: ByteBuf,
    pub parent: u32,
    pub name: String,
    pub content_type: String,
    pub size: u64,
    pub status: i8,
    pub hash: Option<ByteArray<32>>,
    pub dek: Option<ByteBuf>,
    pub custom: Option<MapValue>,
    pub expected_parent_revision: u64,
    pub replace: Option<ReplaceFileInput>,
}

impl BeginUploadInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        validate_request_id(&self.request_id)?;
        if !valid_file_name(&self.name) {
            return Err(SyncError::InvalidInput("invalid file name".to_string()));
        }
        if self.content_type.is_empty() {
            return Err(SyncError::InvalidInput(
                "content_type cannot be empty".to_string(),
            ));
        }
        if !(0..=1).contains(&self.status) {
            return Err(SyncError::InvalidInput("status must be 0 or 1".to_string()));
        }
        if self.status == 1 && self.hash.is_none() {
            return Err(SyncError::InvalidInput(
                "readonly uploads require a hash".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BeginUploadOutput {
    pub session_id: ByteBuf,
    pub file_id: u32,
    pub generation: u64,
    pub chunk_size: u32,
    pub total_chunks: u32,
    pub expires_at: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct UploadChunkInput {
    pub request_id: ByteBuf,
    pub session_id: ByteBuf,
    pub chunk_index: u32,
    pub content: ByteBuf,
}

impl UploadChunkInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        validate_request_id(&self.request_id)?;
        validate_session_id(&self.session_id)?;
        if self.content.is_empty() {
            return Err(SyncError::InvalidInput("chunk cannot be empty".to_string()));
        }
        Ok(())
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UploadChunkOutput {
    pub filled: u64,
    pub uploaded_chunks: u32,
    pub expires_at: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct GetUploadStatusInput {
    pub session_id: ByteBuf,
    pub start: Option<u32>,
    pub take: Option<u16>,
}

impl GetUploadStatusInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        validate_session_id(&self.session_id)
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UploadedChunkRange {
    /// Inclusive first chunk index.
    pub start: u32,
    /// Inclusive last chunk index.
    pub end: u32,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UploadStatusOutput {
    pub file_id: u32,
    pub generation: u64,
    pub size: u64,
    pub filled: u64,
    pub total_chunks: u32,
    pub uploaded_chunks: u32,
    pub ranges: Vec<UploadedChunkRange>,
    pub next: Option<u32>,
    pub expires_at: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct RenewUploadInput {
    pub request_id: ByteBuf,
    pub session_id: ByteBuf,
}

impl RenewUploadInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        validate_request_id(&self.request_id)?;
        validate_session_id(&self.session_id)
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RenewUploadOutput {
    pub expires_at: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct AbortUploadInput {
    pub request_id: ByteBuf,
    pub session_id: ByteBuf,
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct CommitUploadInput {
    pub request_id: ByteBuf,
    pub session_id: ByteBuf,
}

impl CommitUploadInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        validate_request_id(&self.request_id)?;
        validate_session_id(&self.session_id)
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommitUploadOutput {
    pub id: u32,
    pub created: bool,
    pub revision: u64,
    pub generation: u64,
    pub committed_at: u64,
}

impl AbortUploadInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        validate_request_id(&self.request_id)?;
        validate_session_id(&self.session_id)
    }
}

fn validate_session_id(session_id: &ByteBuf) -> Result<(), SyncError> {
    if session_id.len() != UPLOAD_SESSION_ID_SIZE {
        return Err(SyncError::InvalidInput(format!(
            "session_id must contain {UPLOAD_SESSION_ID_SIZE} bytes"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_upload_inputs() {
        let valid = BeginUploadInput {
            request_id: ByteBuf::from(vec![1]),
            parent: 0,
            name: "file.txt".to_string(),
            content_type: "text/plain".to_string(),
            size: 0,
            status: 0,
            hash: None,
            dek: None,
            custom: None,
            expected_parent_revision: 1,
            replace: None,
        };
        assert!(valid.validate().is_ok());

        let mut readonly = valid;
        readonly.status = 1;
        assert!(readonly.validate().is_err());

        assert!(GetUploadStatusInput {
            session_id: ByteBuf::from(vec![0; UPLOAD_SESSION_ID_SIZE]),
            start: None,
            take: None,
        }
        .validate()
        .is_ok());
    }
}
