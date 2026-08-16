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

//! Source-contract tests for the stale-read TSO provider.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tidb_exec::stale_tso::StaleTsoProvider;

#[test]
fn get_stale_tso_evaluates_once_and_caches_the_value() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1353-1369. A missing
    // evaluator yields (0, nil); a present one runs once and its result is
    // pinned for the statement.
    let provider: StaleTsoProvider<&str> = StaleTsoProvider::new();
    assert_eq!(provider.get_stale_tso(), Ok(0));

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in_eval = Arc::clone(&calls);
    provider.set_if_not_exist(Box::new(move || {
        calls_in_eval.fetch_add(1, Ordering::SeqCst);
        Ok(440_000_000_000_000_001)
    }));

    assert_eq!(provider.get_stale_tso(), Ok(440_000_000_000_000_001));
    assert_eq!(provider.get_stale_tso(), Ok(440_000_000_000_000_001));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn set_if_not_exist_keeps_the_first_evaluator() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1342-1351. A second install
    // is ignored while an evaluator is present.
    let provider: StaleTsoProvider<&str> = StaleTsoProvider::new();
    provider.set_if_not_exist(Box::new(|| Ok(100)));
    provider.set_if_not_exist(Box::new(|| Ok(200)));
    assert_eq!(provider.get_stale_tso(), Ok(100));
}

#[test]
fn evaluator_errors_are_returned_and_not_cached() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:1363-1366. An eval error
    // returns (0, err) without writing the value, so the next call
    // re-evaluates.
    let provider: StaleTsoProvider<&str> = StaleTsoProvider::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_in_eval = Arc::clone(&calls);
    provider.set_if_not_exist(Box::new(move || {
        if calls_in_eval.fetch_add(1, Ordering::SeqCst) == 0 {
            Err("pd not ready")
        } else {
            Ok(7)
        }
    }));

    assert_eq!(provider.get_stale_tso(), Err("pd not ready"));
    assert_eq!(provider.get_stale_tso(), Ok(7));
    assert_eq!(provider.get_stale_tso(), Ok(7));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn reset_drops_the_evaluator_and_the_cached_value() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:202-209. After reset the
    // provider behaves as never-installed, and a new evaluator can land.
    let provider: StaleTsoProvider<&str> = StaleTsoProvider::new();
    provider.set_if_not_exist(Box::new(|| Ok(11)));
    assert_eq!(provider.get_stale_tso(), Ok(11));

    provider.reset();
    assert_eq!(provider.get_stale_tso(), Ok(0));

    provider.set_if_not_exist(Box::new(|| Ok(22)));
    assert_eq!(provider.get_stale_tso(), Ok(22));
}
