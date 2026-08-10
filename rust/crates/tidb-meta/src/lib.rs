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
//! The crate owns the deterministic codecs plus [`transaction::Mutator`], a
//! transaction-neutral implementation of Go's metadata rules. It talks to no
//! cluster directly: callers supply [`transaction::RawTransaction`], while
//! [`transaction::MemoryTransaction`] provides the exact in-memory boundary
//! used by semantic tests. Snapshot adapters construct a reader with
//! [`transaction::Mutator::new_reader`], which applies the internal metadata
//! request authority without write-mutator configuration. Callers that only
//! need the deterministic storage format can combine [`key`] and [`value`]
//! directly:
//!
//! ```
//! use tidb_meta::{key, value};
//!
//! // Read m + EncodeBytes("DBs") + EncodeUint('h') + EncodeBytes("DB:3") from
//! // the snapshot, then decode the stored DBInfo JSON. The type flag is an
//! // EIGHT-BYTE big-endian uint, not the single byte it reads as -- Go writes
//! // it with codec.EncodeUint (pkg/structure/type.go:89-95), and a one-byte
//! // flag here would build a key no TiDB node can find.
//! let raw_key = key::database_kv_key(3);
//! let stored = br#"{"id":3,"db_name":{"O":"test","L":"test"},"state":5}"#;
//! let db = value::parse_db_info(stored).unwrap();
//! assert_eq!(db.id, 3);
//! assert!(!raw_key.is_empty());
//! ```

pub mod element;
pub mod error;
pub mod key;
pub mod structure;
pub mod transaction;
pub mod value;

pub use error::{MetaError, Result};
