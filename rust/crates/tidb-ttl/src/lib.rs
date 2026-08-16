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

//! Go `pkg/ttl/sqlbuilder` lands as a complete package: every SQL statement
//! TiDB's TTL worker issues to scan expired rows and delete them.
//!
//! File mapping (one Rust module per Go file):
//! - `sql_builder.rs` <- `sql.go`
//!
//! Narrowings, each named at its own definition site:
//! - [`PhysicalTable`] redeclares `pkg/ttl/cache.PhysicalTable` locally. Go
//!   imports it from a sibling package that pulls in the whole TTL cache,
//!   info-schema and statistics stack; `sqlbuilder` reads only `Schema`, the
//!   embedded `TableInfo.Name`, `PartitionDef`, `KeyColumns`, `TimeColumn`, and
//!   the `ValidateKeyPrefix` method, so exactly those come across.
//! - Every `expire time.Time` parameter becomes an `expire_unix: i64`. Go calls
//!   `expire.Unix()` and nothing else on that value, so the Unix second is the
//!   whole of the contract and this crate needs no calendar dependency.
//! - `writeDatum`'s fallback restores an `ast.NewValueExpr(...)` in Go. This
//!   workspace's `tidb-ast` has no `ValueExpr` node, so the restore body is
//!   transcreated directly from `pkg/types/parser_driver/value_expr.go`.
//! - `writeTblName` restores an `ast.TableName` in Go; with no such node here,
//!   the schema and table names are written directly, which is what
//!   `TableName.restoreName` does for a node carrying no index hints or
//!   partition names.
//! - Go's `strings.Builder` accepts arbitrary bytes while Rust's `RestoreCtx`
//!   writes through `std::fmt::Write`. A string datum that is not valid UTF-8
//!   after escaping is therefore reported as an error rather than emitted
//!   verbatim; binary-flagged and blob-typed columns already take the hex
//!   branch, so no key column the scan/delete paths build reaches that case.
//!
//! Test boundary: Go's `TestFormatSQLDatum` uses `testkit` to create a table,
//! round-trip every value through a live session, and re-query with the
//! formatted literal. That oracle needs a running TiDB, which this workspace
//! has no counterpart for, so `format_sql_datum` is covered here by direct
//! assertions over the same field types and values instead.

pub mod sql_builder;

pub use sql_builder::{
    build_delete_sql, format_sql_datum, PhysicalTable, Result, ScanQueryGenerator, SqlBuilder,
    SqlBuilderError,
};
