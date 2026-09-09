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

//! Go `pkg/timer/tablestore` lands as a complete package: the timer store that
//! keeps its records in a TiDB table, with SQL/session plumbing and the etcd
//! watch notifier.
//!
//! File mapping:
//! - [`sql`] <- `sql.go` (complete)
//! - [`store`] <- `store.go` (complete)
//! - [`crate::notifier`] <- `notifier.go` (complete), using the shared
//!   [`tidb_pd_client::EtcdClient`] for prefix watches and leased event puts.
//!
//! [`json`] is a further boundary module standing in for Go's
//! `encoding/json`; its own header explains what it does and does not cover.
//!
//! Narrowings, each named at its own definition site in [`store`]:
//! - [`store::SqlExecutor`] for `pkg/util/sqlexec`'s `SQLExecutor` /
//!   `RestrictedSQLExecutor` plus `DrainRecordSet`.
//! - [`store::Row`] / [`store::Datum`] for `pkg/util/chunk.Row`.
//! - [`store::SysSession`] / [`store::SessionPool`] are the package-owned
//!   `tidb-syssession` types used directly, as Go imports
//!   `pkg/session/syssession`.
//! - [`store::SessionContext`] for `pkg/sessionctx.Context`'s session
//!   variables, and [`store::VARDEF_TIME_ZONE`] for `sessionctx/vardef`.
//! - [`store::SqlContext`] for the `client-go`
//!   `WithInternalSourceType(ctx, kv.InternalTimer)` tag.
//! - `terror.Log` becomes a private logger call in [`store`].
//! - [`sql::SqlArg`] for Go's `[]any` argument lists.
//!
//! Everything Go keeps unexported is `pub` here so the upstream in-package
//! tests port as integration tests against the crate's public surface.

pub mod json;
pub mod sql;
pub mod store;
