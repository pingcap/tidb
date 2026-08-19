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
//! leaf files of it that carry behavior, landed so that the 3,070-line
//! `domain.go` has somewhere to arrive.
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
//! | `ru_stats.go` | [`ru_stats`] | complete except `NewRUStatsWriter` and `requestUnitsWriterLoop`, both `*Domain` wiring — see the module doc |
//! | `plan_replayer.go` | [`plan_replayer`] | complete — every production symbol, against named boundaries for `extstore`, `sessionctx`, `infosync` and `DumpPlanReplayerInfo` |
//!
//! ## Not in this crate
//!
//! Every other file of `pkg/domain`, including `domain.go` itself,
//! `plan_replayer_dump.go`, `extract.go`, `runaway.go`, `test_helper.go`,
//! `infosync/`, and `domainctx.go`.
//!
//! `extract.go` was screened and declined (2026-08-18). Its material
//! behavior is orchestration over four absent packages: `collectRecords`
//! executes SQL against `information_schema.statements_summary_history`
//! through internal `sessionctx.Context` sessions (no statements-summary
//! storage and no internal SQL session pool exist here);
//! `decodeBinaryPlan` needs `plancodec.DecodePlan`; `handleIsView`
//! re-parses through `ast` visitors over the info schema; and the dump
//! tail writes through `extstore`/`replayer` zip paths. The self-contained
//! remainder (task-type names, dir naming, meta JSON) is a few dozen lines
//! that would be an inert stub without the rest. Its three upstream tests
//! are testkit-bound (`CreateMockStoreAndDomain`). Like `runaway.go`,
//! there is no decision left to port until those packages exist.
//!
//! `domainctx.go` was screened and declined (2026-08-18): a typed
//! downcast accessor over Go's context-value idiom (`GetDomain(ctx)`
//! returning nil for a cross-keyspace session), whose only referent is
//! the unbuilt `Domain` composition root. There is no decision to carry
//! until that root exists; its test binds a mock context, another
//! Go-runtime shape.
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
//! dump-file GC, or `ru_stats.go` — rather than the struct. Both of those
//! have since landed ([`plan_replayer`], [`ru_stats`]); the advice stands
//! for whatever comes next.
//!
//! `plan_replayer_dump.go` (1,004 lines) was screened and NOT started: it is
//! several batches, not one, and starting it now would leave a half-file.
//! Its 40 functions fall into five groups, each blocked on a different
//! absent package:
//!
//! 1. *Zip layout and the self-contained dumpers* — the eleven
//!    `PlanReplayer*File` constants plus `dumpSQLMeta`, `dumpConfig`,
//!    `dumpMeta`, `dumpSQLs`, `dumpErrorMsgs`, `dumpDebugTrace`,
//!    `dumpOneDebugTrace`. Blocking symbols: `archive/zip.Writer`,
//!    `github.com/BurntSushi/toml.NewEncoder`, `config.GetGlobalConfig`,
//!    `printer.GetTiDBInfo`. This is the one group that could land on its
//!    own, behind a zip-writer boundary trait — roughly one batch.
//! 2. *Table-name extraction* — `tableNamePair`, `tableNameExtractor` with
//!    its `Enter`/`Leave` visitor, `getTablesAndViews`, `handleIsView`,
//!    `findFK`, `extractTableNames`. Blocking symbols:
//!    `infoschema.InfoSchema` (unported), `ast.Visitor` over whole statement
//!    trees, and re-parsing a view's `SelectStmt`.
//! 3. *Statistics* — `dumpStatsMemStatus`, `dumpStats`, `getStatsForTable`.
//!    Blocking symbols: `Domain.StatsHandle()`, `statistics/util.JSONTable`,
//!    `statistics.Table`, historical-stats reads.
//! 4. *Bindings and session state* — `dumpSessionBindRecords`,
//!    `dumpSessionBindings`, `dumpGlobalBindings`, `dumpVariables`.
//!    Blocking symbols: `bindinfo.Binding`, `variable.SessionVars`.
//! 5. *Live execution* — `dumpExplain`, `dumpEncodedPlan`,
//!    `dumpPlanReplayerExplain`, `getShowCreateTable`,
//!    `resultSetToStringSlice`, `getRows`, `dumpTiFlashReplica`,
//!    `dumpSchemas`, `dumpSchemaMeta`, plus the 180-line orchestrator
//!    `DumpPlanReplayerInfo` and `setTaskPresignedURL`/`getPresignedURL`.
//!    Blocking symbols: `sqlexec.RecordSet`, `sessionctx.Context` executing
//!    `explain`/`show create table`, and `objstore` presigned URLs.
//!
//! The orchestrator sits on top of all four other groups, so it is last.
//! [`plan_replayer::PlanReplayerDumper`] is the boundary it will implement.
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

pub mod domainutil;
pub mod topn_slow_query;
pub mod domain_sysvars;
pub mod historical_stats;
pub mod optimize_trace;
pub mod plan_replayer;
pub mod ru_stats;
pub mod schema_checker;
pub mod sysvar_cache;
