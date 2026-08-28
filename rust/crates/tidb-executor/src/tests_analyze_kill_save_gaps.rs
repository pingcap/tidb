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

//! Gap tests for Go `pkg/executor/analyze_test.go:237/298`. Both tests
//! coordinate live analyze goroutines through `testfailpoint.EnableCall`
//! hooks (`analyzeSaveWorkerBeforeHandleSignal`,
//! `pkg/executor/analyze_worker.go:76`; `analyzeBeforeSendToSaveResults`,
//! `pkg/executor/analyze.go:724`; the collector-memory release pair,
//! `pkg/executor/analyze_col_sampling.go:863/867`). This tier's analyzer is
//! synchronous (`analyze/panic_recovery.rs` documents the collapsed
//! goroutine boundaries) and has no failpoint machinery, so both Go
//! contracts are recorded as gaps rather than approximated.

/// Go `pkg/executor/analyze_test.go:237::TestAnalyzeKillDuringSaveDoesNotHang`:
/// pausing a save worker at its first kill-check point and the third
/// send-to-save-results, then sending the SQL killer's QueryInterrupted
/// signal, must unblock `analyze table t` with `ErrQueryInterrupted`, and
/// `mysql.analyze_jobs` must record exactly one failed row whose fail_reason
/// mentions the interruption (and none mentioning "context canceled").
#[test]
#[ignore = "go-parity-gap: failpoint-coordinated goroutine pause plus SQLKiller plus mysql.analyze_jobs audit; the synchronous tier has none of the three"]
fn analyze_kill_during_save_does_not_hang() {}

/// Go `pkg/executor/analyze_test.go:298::TestAnalyzeV2ReleaseColumnCollectorMemoryImmediately`:
/// analyze v2 sampling (`tidb_analyze_version=2`, samplerate 1.0) must drop
/// the collector's retained sample bytes between build and save --
/// observed through the before/after
/// analyzeSamplingBuild*ReleaseCollectorMemory hooks, where the collector
/// memory delta equals the consumed bytes. Sample values stay under
/// `statistics.MaxSampleValueLength` (`pkg/statistics/sample.go:131`,
/// `mysql.MaxFieldVarCharLength / 2`) so truncation cannot mask the release.
#[test]
#[ignore = "go-parity-gap: memory-release assertions are read through testfailpoint hook arguments; the tier has no failpoints and no collector memory tracker seam"]
fn analyze_v2_release_column_collector_memory_immediately() {}
