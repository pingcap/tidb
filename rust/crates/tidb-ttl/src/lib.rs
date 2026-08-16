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

//! TiDB's time-to-live subsystem: the SQL a TTL job issues, the session it
//! issues it through, and the cached view of which tables have TTL at all.
//!
//! Packages covered, each with its own header stating what did and did not come
//! across:
//! - [`sql_builder`] <- Go `pkg/ttl/sqlbuilder` — complete package.
//! - [`session`] <- Go `pkg/ttl/session` — complete package.
//! - [`cache`] <- Go `pkg/ttl/cache` — SEED.
//!
//! # One crate-wide constraint
//!
//! `rust/Cargo.lock` pins this crate's dependencies to `tidb-ast`,
//! `tidb-datatype`, `tidb-model`, `tidb-mysql` and `tidb-util`; the validation
//! gates run `--locked`, and both `rust/Cargo.toml` and `rust/Cargo.lock` are
//! owned outside this crate, so no dependency edge may be added here. Several
//! packages these two Go packages import are already transcreated —
//! `tidb-chunk` (`pkg/util/chunk`), `tidb-codec` (`pkg/util/codec`),
//! `tidb-tablecodec`, `tidb-txnkv` (`pkg/kv`), `tidb-expr` (`pkg/expression`
//! and `pkg/expression/exprstatic`) and `tidb-exec`
//! (`pkg/infoschema/context`) — but are unreachable from here for that reason.
//! Every place that constraint bites is named with a `// boundary:` comment at
//! its own definition site, and the crate-wide consequences are spelled out in
//! [`cache`]'s header. Resolving the ownership of `Cargo.toml`/`Cargo.lock`
//! would let `cache::table`'s expiry evaluation, `cache::task`'s range
//! encoding, and the private `cache::table::keycodec` module be replaced by the
//! real transcreations.

pub mod cache;
pub mod session;
pub mod sql_builder;

pub use cache::table::PhysicalTable;
pub use sql_builder::{
    build_delete_sql, format_sql_datum, Result, ScanQueryGenerator, SqlBuilder, SqlBuilderError,
};
