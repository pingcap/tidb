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

//! Port ledger for `pkg/ddl/executor_nokit_test.go:62
//! TestIsRetryableDDLCancelErr` (`pkg/ddl.part6` batch b105, item 309 of the
//! pkg/ddl enumeration).
//!
//! The Go test classifies the errors `DoDDLJobWrapper`'s admin-cancel retry
//! loop (pkg/ddl/executor.go:7387) consults through `isRetryableDDLCancelErr`
//! (executor.go:7230-7244). The cancel loop itself -- which re-delivers
//! `ADMIN CANCEL DDL JOBS` against a job table behind etcd -- is not
//! transcreated, and neither are the three dbterror sentinels it
//! distinguishes.

/// GO PORT of `pkg/ddl/executor_nokit_test.go:62 TestIsRetryableDDLCancelErr`.
///
/// Re-derived contract (executor.go:7230-7244): a wrapped or bare
/// `dbterror.ErrCancelFinishedDDLJob` is NOT retryable (the job already
/// finished; retrying cannot help); the same for
/// `dbterror.ErrCannotCancelDDLJob` and `dbterror.ErrDDLJobNotFound`,
/// wrapped or not (`errors.Is` semantics over `GenWithStackByArgs` wraps).
/// Anything ELSE -- the test's `errors.New("mock failed admin command on ddl
/// jobs")` stands in for the transient write-conflict the cancel command can
/// hit -- IS retryable.
#[test]
#[ignore = "go-parity-gap: isRetryableDDLCancelErr (pkg/ddl/executor.go:7230-7244), its DoDDLJobWrapper cancel loop (executor.go:7387), and the ErrCancelFinishedDDLJob/ErrCannotCancelDDLJob/ErrDDLJobNotFound sentinels are not transcreated"]
fn is_retryable_ddl_cancel_err_classifies_the_three_sentinels() {}
