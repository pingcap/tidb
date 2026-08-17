// Copyright 2025 PingCAP, Inc.
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

//! A SEED of Go `pkg/domain`. This crate is NOT the package; it is the
//! first four leaf files of it, landed so that the 3,070-line `domain.go`
//! has somewhere to arrive.
//!
//! `pkg/domain` is what `cmd/tidb-server` bootstraps: the schema reloader,
//! the stats handle, DDL ownership, the sysvar cache. Nothing of that
//! machinery is here yet. What is here are the files that do not need the
//! `Domain` struct to say what they mean — each one landed against explicit
//! `// boundary:` traits standing in for `Domain`, `infoschema`, and
//! `sessionctx`. Those traits are the contract `domain.go` will implement.
//!
//! ## In this crate
//!
//! | Go file | Rust module | State |
//! | --- | --- | --- |
//! | `sysvar_cache.go` | [`sysvar_cache`] | complete |
//! | `schema_checker.go` | [`schema_checker`] | complete |
//! | `optimize_trace.go` | [`optimize_trace`] | complete |
//! | `domain_sysvars.go` | [`domain_sysvars`] | partial — `initDomainSysVars` absent |
//! | `historical_stats.go` | [`historical_stats`] | complete |
//!
//! ## Not in this crate
//!
//! Every other file of `pkg/domain`, including `domain.go` itself,
//! `plan_replayer*.go`, `extract.go`, `runaway.go`, `ru_stats.go`,
//! `infosync/`, and `domainctx.go`.
//!
//! `runaway.go` was screened and declined. Its only symbol,
//! `(*Domain).initResourceGroupsController`, is nothing but wiring between
//! things that do not exist in Rust: the blocking Go symbols are
//! `rmclient.NewResourceGroupController` (PD's
//! `resource_group/controller`), `runaway.NewRunawayManager`
//! (`pkg/resourcegroup/runaway`), `infosync.GetServerInfo`, and
//! `tikv.SetResourceControlInterceptor`. Every line either constructs or
//! hands over one of those; there is no decision left to port, so nothing
//! was written.
//!
//! The `Domain` struct itself was screened and declined for this batch.
//! `domain.go:150-238` is roughly fifty fields, and each one is a distinct
//! unported package rather than a value: `infoschema.InfoCache`,
//! `privileges.Handle`, `statistics/handle.Handle`, `ddl.DDL`,
//! `ddl.Executor`, `notifier.DDLNotifier`, `infosync.InfoSyncer`,
//! `issyncer.Syncer`, `globalconfigsync.GlobalConfigSyncer`,
//! `syssession.AdvancedSessionPool`, `clientv3.Client` (etcd),
//! `autoid.ClientDiscover`, `owner.Manager` (three times),
//! `ttlworker.JobManager`, `runaway.Manager`,
//! `rmclient.ResourceGroupsController`, `concurrency.Session`,
//! `globalconn.Allocator`, `systable.MinJobIDRefresher`,
//! `crossks.Manager`, and `sessionctx.InstancePlanCache`. The struct also
//! asserts `var _ sqlsvrapi.Server = (*Domain)(nil)` (`domain.go:238`), so
//! landing the type honestly means landing that interface too. A `Domain`
//! built now would be fifty boundary traits with no bodies behind them —
//! an inert stub by any reading, and the accessors it would unblock
//! (`domainctx.go`) are one-liners not worth it. The real wall is that
//! `Domain` is a *composition root*: it has almost no logic of its own,
//! so it cannot land before its parts. The next batch should attack the
//! parts with genuine behavior — `plan_replayer.go`'s task status and
//! dump-file GC, or `ru_stats.go` — rather than the struct.
//!
//! `domainctx.go` was screened and deliberately declined. Its only symbol,
//! `GetDomain`, is a two-line downcast: `v, ok :=
//! ctx.GetDomain().(*Domain); if ok { return v }; return nil`. The blocking
//! Go symbol is `*domain.Domain` itself — with no `Domain` type there is no
//! downcast to perform, and the surrounding
//! `util/context.ValueStoreContext.GetDomain() any` is a Go
//! `interface{}`-shaped hole that Rust has no reason to reproduce. Anything
//! written here would be an inert stub, so nothing was written. It belongs
//! in the `domain.go` batch, where it is a one-liner.
//!
//! `topn_slow_query.go` is also absent, and deliberately: it is already
//! ported in full as `tidb_exec::topn_slow_query`. That was verified
//! symbol-by-symbol rather than assumed.

pub mod domain_sysvars;
pub mod historical_stats;
pub mod optimize_trace;
pub mod schema_checker;
pub mod sysvar_cache;
