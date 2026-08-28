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

//! Gap tests for Go `pkg/executor/test/loadremotetest` (batch items
//! 1041–1042). The enumeration names two items: the package `TestMain`
//! (bootstrap only) and `TestLoadRemote`, the testify suite runner whose
//! `mockGCSSuite` methods stand up a fake GCS server (`fakestorage.Server`)
//! and run `LOAD DATA ... REMOTE ...` CSV/error/multi-file scenarios
//! (`error_test.go`, `multi_file_test.go`, `one_csv_test.go`). LOAD DATA
//! execution — let alone the REMOTE source over cloud storage — is unported
//! (see `tests_loaddatatest_source`), so both items are gaps here.

/// Go `pkg/executor/test/loadremotetest/util_test.go:44::TestLoadRemote`:
/// the mock-GCS suite runner. Its scenarios pin `LOAD DATA ... REMOTE`
/// over `gs://` endpoints: CSV loading (`one_csv_test.go:25 TestLoadCSV`),
/// error/column-count/eval/data failures and issue 43555
/// (`error_test.go:36/82/149/192/266`), filename asterisk expansion, LAST_INSERT_ID,
/// multi-batch with IGNORE lines, and mixed compression
/// (`multi_file_test.go:28/94/127/165`). The remote-source reader
/// (`pkg/executor/load_data.go`'s remote URL path over
/// `pkg/objstore`) and the fake-GCS seam are both unported.
#[test]
#[ignore = "go-parity-gap: LOAD DATA REMOTE over cloud object storage (fake-GCS suite in pkg/executor/test/loadremotetest) has no Rust executor to drive"]
fn load_remote_suite_runs_csv_scenarios_against_a_mock_gcs() {}

/// Go `pkg/executor/test/loadremotetest/main_test.go:23::TestMain`: goleak
/// bootstrap only.
#[test]
#[ignore = "go-parity-gap: loadremotetest TestMain is goleak suite bootstrap; no statement behavior"]
fn loadremotetest_main_is_bootstrap_only() {}
