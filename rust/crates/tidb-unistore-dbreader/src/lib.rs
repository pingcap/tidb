// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/store/mockstore/unistore/tikv/dbreader`: what the coprocessor
//! handler reads committed data through. The tikv server builds the reader over
//! its MVCC store and hands it to `cophandler`, which never sees the store.

use tidb_unistore_kverrors::KvError;

/// One `kvrpcpb.KvPair` reduced to what this slice produces: a key with a
/// value, or a key with the blocking lock as its error.
#[derive(Clone, Debug, PartialEq)]
pub struct KvPair {
    /// The key.
    pub key: Vec<u8>,
    /// The committed value; empty when `error` carries the answer.
    pub value: Vec<u8>,
    /// Go `Error *kvrpcpb.KeyError`, carried as the store error it converts
    /// from — a blocking lock under SI, an `RcCheckTs` conflict otherwise.
    pub error: Option<Box<KvError>>,
    /// The version's commit ts, answered only when the request sets
    /// `NeedCommitTs` (Go `requestCtx.returnCommitTS`).
    pub commit_ts: u64,
}

/// The `ScanRequest` fields Go's `Scan` reads.
#[derive(Clone, Debug, Default)]
pub struct ScanReq {
    /// `StartKey`.
    pub start_key: Vec<u8>,
    /// `EndKey`; empty falls to the store's internal-key prefix (the region-bound
    /// fallback is a narrowing: no region context in this slice).
    pub end_key: Vec<u8>,
    /// `Limit`.
    pub limit: u32,
    /// `Version`.
    pub version: u64,
    /// `SampleStep`.
    pub sample_step: u32,
    /// `Reverse`.
    pub reverse: bool,
}

/// Go `dbreader.DBReader` as the coprocessor reads through it: the committed
/// rows of a key range at a read version, each blocking lock answered as the
/// pair's error.
pub trait DbReader {
    /// Go `Scan` over `req`'s range and version.
    fn scan(&self, req: &ScanReq) -> Vec<KvPair>;
}
