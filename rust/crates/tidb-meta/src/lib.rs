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

//! `pkg/meta`: the codec for TiDB's catalog, which lives in TiKV under the
//! single-byte `m` namespace.
//!
//! This crate is pure encode/decode — it holds no transaction and talks to no
//! cluster. A caller that owns a KV snapshot combines [`key`] to build the raw
//! keys it reads and [`value`] to interpret what comes back:
//!
//! ```
//! use tidb_meta::{key, value};
//!
//! // Read m + EncodeBytes("DBs") + 'h' + EncodeBytes("DB:3") from the snapshot,
//! // then decode the stored DBInfo JSON.
//! let raw_key = key::database_kv_key(3);
//! let stored = br#"{"id":3,"db_name":{"O":"test","L":"test"},"state":5}"#;
//! let db = value::parse_db_info(stored).unwrap();
//! assert_eq!(db.id, 3);
//! assert!(!raw_key.is_empty());
//! ```

pub mod error;
pub mod key;
pub mod structure;
pub mod value;

pub use error::{MetaError, Result};
