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

//! Go `pkg/timer/tablestore` lands as a SEED: the timer store that keeps its
//! records in a TiDB table, complete for the SQL layer and the session
//! plumbing, with the etcd watch notifier named and deferred.
//!
//! File mapping:
//! - [`sql`] <- `sql.go` (complete)
//! - [`store`] <- `store.go` (complete)
//! - `notifier.go` is **not** ported. It is 321 lines of
//!   `go.etcd.io/etcd/client/v3` watch plumbing — an etcd client, a watch
//!   channel per store, and a key namespace derived from the cluster id — and
//!   this workspace has no etcd client crate. Go's `NewTableTimerStore` picks
//!   `NewEtcdNotifier` only when an etcd client is passed and otherwise falls
//!   back to `api.NewMemTimerWatchEventNotifier`; the Rust
//!   [`store::new_table_timer_store`] is that fallback branch exactly, and
//!   [`store::TableTimerStoreCore::with_notifier`] leaves the injection point
//!   open. This is why the package is labelled SEED rather than complete.
//!
//! [`json`] is a further boundary module standing in for Go's
//! `encoding/json`; its own header explains what it does and does not cover.
//!
//! Narrowings, each named at its own definition site in [`store`]:
//! - [`store::SqlExecutor`] for `pkg/util/sqlexec`'s `SQLExecutor` /
//!   `RestrictedSQLExecutor` plus `DrainRecordSet`.
//! - [`store::Row`] / [`store::Datum`] for `pkg/util/chunk.Row`.
//! - [`store::SysSession`] / [`store::SessionPool`] for
//!   `pkg/session/syssession`, which is ported in `tidb-exec` — a crate
//!   `tidb-timer` cannot depend on without inverting the dependency direction.
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
