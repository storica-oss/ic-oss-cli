use candid::CandidType;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::{
    entry::{validate_request_id, EnsureFolderOutput, SyncError},
    file::{valid_file_name, CreateFileInput, CreateFileOutput, CHUNK_SIZE},
    folder::CreateFolderInput,
};

pub const MAX_BATCH_ITEMS: usize = 32;
pub const MAX_BATCH_INLINE_BYTES: usize = 6 * CHUNK_SIZE as usize;
pub const MAX_BATCH_FILE_BYTES: usize = CHUNK_SIZE as usize;

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct BatchEnsureFoldersInput {
    pub request_id: ByteBuf,
    pub folders: Vec<CreateFolderInput>,
}

impl BatchEnsureFoldersInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        validate_request_id(&self.request_id)?;
        validate_count(self.folders.len())?;
        if self
            .folders
            .iter()
            .any(|folder| !valid_file_name(&folder.name))
        {
            return Err(SyncError::InvalidInput(
                "batch contains an invalid folder name".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct BatchEnsureFoldersOutput {
    pub results: Vec<Result<EnsureFolderOutput, SyncError>>,
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct BatchCreateSmallFilesInput {
    pub request_id: ByteBuf,
    pub files: Vec<CreateFileInput>,
}

impl BatchCreateSmallFilesInput {
    pub fn validate(&self) -> Result<(), SyncError> {
        validate_request_id(&self.request_id)?;
        validate_count(self.files.len())?;

        let mut total = 0usize;
        for file in &self.files {
            file.validate().map_err(SyncError::InvalidInput)?;
            let content_len = file.content.as_ref().map_or(0, |content| content.len());
            let size = file.size.unwrap_or(content_len as u64);
            if size != content_len as u64 {
                return Err(SyncError::InvalidInput(format!(
                    "inline content size mismatch for file {:?}",
                    file.name
                )));
            }
            if content_len > MAX_BATCH_FILE_BYTES {
                return Err(SyncError::LimitExceeded(format!(
                    "inline file {:?} exceeds {} bytes",
                    file.name, MAX_BATCH_FILE_BYTES
                )));
            }
            if file.status == Some(1) && file.hash.is_none() {
                return Err(SyncError::InvalidInput(format!(
                    "readonly file {:?} requires a hash",
                    file.name
                )));
            }
            total = total.saturating_add(content_len);
        }
        if total > MAX_BATCH_INLINE_BYTES {
            return Err(SyncError::LimitExceeded(format!(
                "batch inline content exceeds {} bytes",
                MAX_BATCH_INLINE_BYTES
            )));
        }
        Ok(())
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Serialize)]
pub struct BatchCreateSmallFilesOutput {
    pub results: Vec<Result<CreateFileOutput, SyncError>>,
}

fn validate_count(count: usize) -> Result<(), SyncError> {
    if count == 0 || count > MAX_BATCH_ITEMS {
        return Err(SyncError::LimitExceeded(format!(
            "batch must contain 1 to {} items",
            MAX_BATCH_ITEMS
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(content: Vec<u8>) -> CreateFileInput {
        CreateFileInput {
            parent: 0,
            name: "a.txt".to_string(),
            content_type: "text/plain".to_string(),
            size: Some(content.len() as u64),
            content: (!content.is_empty()).then(|| ByteBuf::from(content)),
            ..Default::default()
        }
    }

    #[test]
    fn validates_batch_bounds() {
        assert!(BatchEnsureFoldersInput {
            request_id: ByteBuf::from(vec![1]),
            folders: vec![],
        }
        .validate()
        .is_err());

        assert!(BatchCreateSmallFilesInput {
            request_id: ByteBuf::from(vec![1]),
            files: vec![file(vec![1; MAX_BATCH_FILE_BYTES + 1])],
        }
        .validate()
        .is_err());

        assert!(BatchCreateSmallFilesInput {
            request_id: ByteBuf::from(vec![1]),
            files: vec![file(vec![1; 16])],
        }
        .validate()
        .is_ok());
    }
}
