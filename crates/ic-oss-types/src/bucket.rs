use candid::{CandidType, Nat, Principal};
use serde::{Deserialize, Serialize};
use serde_bytes::{ByteArray, ByteBuf};
use std::collections::BTreeSet;
use url::{Host, Url};

use crate::file::MAX_FILE_SIZE;

#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum HttpReadMode {
    #[default]
    Legacy,
    Public,
    TokenProtected,
    Disabled,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReaderPolicy {
    pub enabled: bool,
    pub authority: Option<Principal>,
    pub allow_by_hash: bool,
}

impl Default for ReaderPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            authority: None,
            allow_by_hash: true,
        }
    }
}

impl ReaderPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.authority == Some(Principal::anonymous()) {
            return Err("reader authority cannot be anonymous".to_string());
        }
        if self.enabled && self.authority.is_none() {
            return Err("enabled reader policy requires an authority".to_string());
        }
        if self.enabled && self.allow_by_hash {
            return Err("enabled reader policy cannot allow by-hash reads".to_string());
        }
        Ok(())
    }
}

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct BucketInfo {
    pub name: String,
    pub file_id: u32,
    pub folder_id: u32,
    pub max_file_size: u64,
    pub max_folder_depth: u8,
    pub max_children: u16,
    pub max_custom_data_size: u16,
    pub enable_hash_index: bool,
    pub status: i8,     // -1: archived; 0: readable and writable; 1: readonly
    pub visibility: u8, // 0: private; 1: public
    pub total_files: u64,
    pub total_chunks: u64,
    pub total_folders: u64,
    pub managers: BTreeSet<Principal>, // managers can read and write
    // auditors can read and list even if the bucket is private
    pub auditors: BTreeSet<Principal>,
    // used to verify the request token signed with SECP256K1
    pub trusted_ecdsa_pub_keys: Vec<ByteBuf>,
    // used to verify the request token signed with ED25519
    pub trusted_eddsa_pub_keys: Vec<ByteArray<32>>,
    pub governance_canister: Option<Principal>,
    pub http_read_mode: Option<HttpReadMode>,
    pub reader_policy: Option<ReaderPolicy>,
}

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DomainConfig {
    pub canister_id: Principal,
    pub custom_domains: Vec<String>,
    pub derivation_origin: String,
}

pub const MAX_CUSTOM_DOMAINS: usize = 10;

pub fn normalize_custom_domains(domains: &[String]) -> Result<Vec<String>, String> {
    if domains.len() > MAX_CUSTOM_DOMAINS {
        return Err(format!(
            "at most {MAX_CUSTOM_DOMAINS} custom domains are allowed"
        ));
    }

    domains
        .iter()
        .map(|domain| {
            let value = domain.trim().to_ascii_lowercase();
            if value.is_empty()
                || value.starts_with('.')
                || value.ends_with('.')
                || value.contains('/')
                || value.contains(':')
                || value.contains('*')
            {
                return Err(
                    "custom domain must be a DNS hostname without scheme or path".to_string(),
                );
            }
            let url = Url::parse(&format!("https://{value}"))
                .map_err(|_| "custom domain is not a valid DNS hostname".to_string())?;
            let Some(Host::Domain(host)) = url.host() else {
                return Err("custom domain must not be an IP address".to_string());
            };
            if host == "localhost" || !host.contains('.') {
                return Err("custom domain must be a public DNS hostname".to_string());
            }
            Ok(host.to_string())
        })
        .collect::<Result<BTreeSet<_>, _>>()
        .map(BTreeSet::into_iter)
        .map(Iterator::collect)
}

/// Capacity signals reported by the Bucket itself. Stable-memory remaining
/// space is necessarily an estimate: the platform limit is fixed per canister,
/// while successful growth also depends on subnet capacity and cycle reserves.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct BucketStorageMetrics {
    pub stable_memory_size: u64,
    pub wasm_memory_size: u64,
    pub total_memory_size: u64,
    pub memory_allocation: u64,
    pub stable_memory_limit: u64,
    /// Spendable cycles currently held by the Bucket.
    pub cycles: Option<Nat>,
    /// Cycles reserved by the subnet for future storage payments.
    pub reserved_cycles: Option<Nat>,
}

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct UpdateBucketInput {
    pub name: Option<String>,
    pub max_file_size: Option<u64>,
    pub max_folder_depth: Option<u8>,
    pub max_children: Option<u16>,
    pub max_custom_data_size: Option<u16>,
    pub enable_hash_index: Option<bool>,
    pub status: Option<i8>, // -1: archived; 0: readable and writable; 1: readonly
    pub visibility: Option<u8>, // 0: private; 1: public
    pub trusted_ecdsa_pub_keys: Option<Vec<ByteBuf>>,
    pub trusted_eddsa_pub_keys: Option<Vec<ByteArray<32>>>,
    pub http_read_mode: Option<HttpReadMode>,
    pub reader_policy: Option<ReaderPolicy>,
}

impl UpdateBucketInput {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(name) = &self.name {
            if name.trim().is_empty() {
                return Err("invalid bucket name".to_string());
            }
        }
        if let Some(max_file_size) = self.max_file_size {
            if max_file_size == 0 {
                return Err("max_file_size should be greater than 0".to_string());
            }
            if max_file_size > MAX_FILE_SIZE {
                return Err(format!(
                    "max_file_size should be smaller than or equal to {}",
                    MAX_FILE_SIZE
                ));
            }
        }

        if let Some(max_folder_depth) = self.max_folder_depth {
            if max_folder_depth == 0 {
                return Err("max_folder_depth should be greater than 0".to_string());
            }
        }

        if let Some(max_children) = self.max_children {
            if max_children == 0 {
                return Err("max_children should be greater than 0".to_string());
            }
        }

        if let Some(max_custom_data_size) = self.max_custom_data_size {
            if max_custom_data_size == 0 {
                return Err("max_custom_data_size should be greater than 0".to_string());
            }
        }

        if let Some(status) = self.status {
            if !(-1i8..=1i8).contains(&status) {
                return Err("status should be -1, 0 or 1".to_string());
            }
        }

        if let Some(visibility) = self.visibility {
            if visibility != 0 && visibility != 1 {
                return Err("visibility should be 0 or 1".to_string());
            }
        }
        if let Some(reader_policy) = &self.reader_policy {
            reader_policy.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_bucket_max_file_size_accepts_supported_range() {
        assert!(UpdateBucketInput {
            max_file_size: Some(1),
            ..Default::default()
        }
        .validate()
        .is_ok());
        assert!(UpdateBucketInput {
            max_file_size: Some(MAX_FILE_SIZE),
            ..Default::default()
        }
        .validate()
        .is_ok());
    }

    #[test]
    fn update_bucket_max_file_size_rejects_invalid_values() {
        assert!(UpdateBucketInput {
            max_file_size: Some(0),
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(UpdateBucketInput {
            max_file_size: Some(MAX_FILE_SIZE + 1),
            ..Default::default()
        }
        .validate()
        .is_err());
    }

    #[test]
    fn reader_policy_defaults_to_legacy_compatible_behavior() {
        let policy = ReaderPolicy::default();
        assert!(!policy.enabled);
        assert_eq!(policy.authority, None);
        assert!(policy.allow_by_hash);
        assert_eq!(HttpReadMode::default(), HttpReadMode::Legacy);
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn enabled_reader_policy_requires_authority_and_disables_by_hash() {
        assert!(ReaderPolicy {
            enabled: true,
            authority: None,
            allow_by_hash: false,
        }
        .validate()
        .is_err());
        assert!(ReaderPolicy {
            enabled: true,
            authority: Some(Principal::anonymous()),
            allow_by_hash: false,
        }
        .validate()
        .is_err());
        assert!(ReaderPolicy {
            enabled: true,
            authority: Some(Principal::from_slice(&[1; 29])),
            allow_by_hash: true,
        }
        .validate()
        .is_err());
        assert!(ReaderPolicy {
            enabled: true,
            authority: Some(Principal::from_slice(&[1; 29])),
            allow_by_hash: false,
        }
        .validate()
        .is_ok());
    }

    #[test]
    fn custom_domains_are_normalized_deduplicated_and_limited() {
        assert_eq!(
            normalize_custom_domains(&[
                "OSS.Example.com".to_string(),
                "oss.example.com".to_string(),
                "files.example.com".to_string(),
            ])
            .unwrap(),
            vec!["files.example.com", "oss.example.com"]
        );
        for invalid in [
            "localhost",
            "https://oss.example.com",
            "oss.example.com/path",
            "127.0.0.1",
            "*.example.com",
        ] {
            assert!(normalize_custom_domains(&[invalid.to_string()]).is_err());
        }
        assert!(normalize_custom_domains(
            &(0..=MAX_CUSTOM_DOMAINS)
                .map(|index| format!("{index}.example.com"))
                .collect::<Vec<_>>()
        )
        .is_err());
    }
}
