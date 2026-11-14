// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `github.com/pingcap/metering_sdk`, pinned by the repository's go.mod.

pub mod config;
pub mod storage;
pub mod writer;

/// Go `common.MeteringData`.
#[derive(Clone, Debug, Default)]
pub struct MeteringData {
    pub self_id: String,
    pub timestamp: i64,
    pub category: String,
    pub shared_pool_id: String,
    pub data: Vec<std::collections::BTreeMap<String, serde_json::Value>>,
}
