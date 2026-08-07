use candid::CandidType;
use serde::{Deserialize, Serialize};

#[derive(CandidType, Clone, Debug, Default, Deserialize, Serialize)]
pub struct CollectGarbageInput {
    /// Maximum number of chunk slots to process in this call.
    pub max_chunks: Option<u32>,
}

#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct CollectGarbageOutput {
    pub processed_chunks: u32,
    pub removed_chunks: u32,
    pub completed_items: u32,
    pub remaining_items: u64,
    pub remaining_chunks: u64,
}

#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcHealth {
    pub pending_items: u64,
    pub pending_chunks: u64,
    pub oldest_enqueued_at: Option<u64>,
}
