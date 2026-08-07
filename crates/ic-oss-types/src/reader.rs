use candid::{CandidType, Principal};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::ANONYMOUS;

pub const MAX_READER_GRANT_BATCH_ITEMS: usize = 100;
pub const MAX_READER_GRANT_REQUEST_ID_BYTES: usize = 64;

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ReaderGrantStatus {
    Active,
    Revoked,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReaderGrant {
    pub subject: Principal,
    pub expires_at_ms: Option<u64>,
    pub entitlement_version: u64,
    pub status: ReaderGrantStatus,
    pub granted_by: Principal,
    pub updated_at_ms: u64,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ReaderGrantError {
    Unauthorized,
    AnonymousNotAllowed,
    InvalidExpiry,
    InvalidInput(String),
    TooManyItems { max: u16 },
    StaleVersion { current_version: u64 },
    VersionConflict { current_version: u64 },
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReaderGrantSpec {
    pub subject: Principal,
    pub expires_at_ms: Option<u64>,
    pub entitlement_version: u64,
}

impl ReaderGrantSpec {
    pub fn validate(&self, now_ms: u64) -> Result<(), ReaderGrantError> {
        validate_subject(self.subject)?;
        validate_version(self.entitlement_version)?;
        validate_expiry(self.expires_at_ms, now_ms)
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UpsertReaderGrantInput {
    pub subject: Principal,
    pub expires_at_ms: Option<u64>,
    pub entitlement_version: u64,
    pub request_id: ByteBuf,
}

impl UpsertReaderGrantInput {
    pub fn validate(&self, now_ms: u64) -> Result<(), ReaderGrantError> {
        ReaderGrantSpec {
            subject: self.subject,
            expires_at_ms: self.expires_at_ms,
            entitlement_version: self.entitlement_version,
        }
        .validate(now_ms)?;
        validate_request_id(&self.request_id)
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RevokeReaderGrantInput {
    pub subject: Principal,
    pub entitlement_version: u64,
    pub request_id: ByteBuf,
}

impl RevokeReaderGrantInput {
    pub fn validate(&self) -> Result<(), ReaderGrantError> {
        validate_subject(self.subject)?;
        validate_version(self.entitlement_version)?;
        validate_request_id(&self.request_id)
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BatchUpsertReaderGrantsInput {
    pub request_id: ByteBuf,
    pub grants: Vec<ReaderGrantSpec>,
}

impl BatchUpsertReaderGrantsInput {
    pub fn validate(&self) -> Result<(), ReaderGrantError> {
        validate_request_id(&self.request_id)?;
        if self.grants.is_empty() {
            return Err(ReaderGrantError::InvalidInput(
                "grants must not be empty".to_string(),
            ));
        }
        if self.grants.len() > MAX_READER_GRANT_BATCH_ITEMS {
            return Err(ReaderGrantError::TooManyItems {
                max: MAX_READER_GRANT_BATCH_ITEMS as u16,
            });
        }
        Ok(())
    }
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BatchUpsertReaderGrantsOutput {
    pub results: Vec<Result<ReaderGrant, ReaderGrantError>>,
}

fn validate_subject(subject: Principal) -> Result<(), ReaderGrantError> {
    if subject == ANONYMOUS {
        return Err(ReaderGrantError::AnonymousNotAllowed);
    }
    Ok(())
}

fn validate_version(version: u64) -> Result<(), ReaderGrantError> {
    if version == 0 {
        return Err(ReaderGrantError::InvalidInput(
            "entitlement_version must be greater than 0".to_string(),
        ));
    }
    Ok(())
}

fn validate_expiry(expires_at_ms: Option<u64>, now_ms: u64) -> Result<(), ReaderGrantError> {
    if expires_at_ms.is_some_and(|expires_at_ms| expires_at_ms <= now_ms) {
        return Err(ReaderGrantError::InvalidExpiry);
    }
    Ok(())
}

fn validate_request_id(request_id: &ByteBuf) -> Result<(), ReaderGrantError> {
    if request_id.is_empty() || request_id.len() > MAX_READER_GRANT_REQUEST_ID_BYTES {
        return Err(ReaderGrantError::InvalidInput(format!(
            "request_id must contain 1 to {} bytes",
            MAX_READER_GRANT_REQUEST_ID_BYTES
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(seed: u8) -> Principal {
        Principal::from_slice(&[seed; 29])
    }

    #[test]
    fn validates_upsert_input() {
        let input = UpsertReaderGrantInput {
            subject: principal(1),
            expires_at_ms: Some(11),
            entitlement_version: 1,
            request_id: ByteBuf::from(vec![1]),
        };
        assert_eq!(input.validate(10), Ok(()));
        assert_eq!(
            UpsertReaderGrantInput {
                expires_at_ms: Some(10),
                ..input.clone()
            }
            .validate(10),
            Err(ReaderGrantError::InvalidExpiry)
        );
        assert_eq!(
            UpsertReaderGrantInput {
                subject: ANONYMOUS,
                ..input.clone()
            }
            .validate(10),
            Err(ReaderGrantError::AnonymousNotAllowed)
        );
        assert!(matches!(
            UpsertReaderGrantInput {
                entitlement_version: 0,
                ..input
            }
            .validate(10),
            Err(ReaderGrantError::InvalidInput(_))
        ));
    }

    #[test]
    fn validates_request_and_batch_bounds() {
        let input = BatchUpsertReaderGrantsInput {
            request_id: ByteBuf::from(vec![1]),
            grants: vec![ReaderGrantSpec {
                subject: principal(2),
                expires_at_ms: None,
                entitlement_version: 1,
            }],
        };
        assert_eq!(input.validate(), Ok(()));
        assert!(matches!(
            BatchUpsertReaderGrantsInput {
                grants: Vec::new(),
                ..input.clone()
            }
            .validate(),
            Err(ReaderGrantError::InvalidInput(_))
        ));
        assert_eq!(
            BatchUpsertReaderGrantsInput {
                grants: vec![input.grants[0].clone(); MAX_READER_GRANT_BATCH_ITEMS + 1],
                ..input
            }
            .validate(),
            Err(ReaderGrantError::TooManyItems {
                max: MAX_READER_GRANT_BATCH_ITEMS as u16
            })
        );
    }
}
