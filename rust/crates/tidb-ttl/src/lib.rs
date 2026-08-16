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
//! # Dependencies
//!
//! The Go packages transcreated here import `pkg/util/codec`, `pkg/tablecodec`,
//! `pkg/kv` and `pkg/expression`; this crate depends on their transcreations
//! (`tidb-codec`, `tidb-tablecodec`, `tidb-txnkv`, `tidb-expr`) directly, so no
//! encoding or expression behaviour is reproduced locally. `pkg/infoschema` has
//! no transcreation, and that is the one import still expressed as a trait
//! boundary — see [`cache`]'s header. Every place a narrowing remains is named
//! with a `// boundary:` comment at its own definition site.

pub mod cache;
pub mod session;
pub mod sql_builder;

pub use cache::table::{eval_expire_time, PhysicalTable, TimeUnitType};
pub use sql_builder::{
    build_delete_sql, format_sql_datum, Result, ScanQueryGenerator, SqlBuilder, SqlBuilderError,
};
