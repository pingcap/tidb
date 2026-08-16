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

//! Source-contract tests for the per-statement value cache.

use std::time::{Duration, SystemTime};

use tidb_exec::stmt_cache::{StmtCache, StmtCacheKey, StmtCacheValue};

#[test]
fn get_or_store_keeps_the_first_value() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:824-835. The map insert is
    // guarded by a presence check, so the first stored value wins for the
    // rest of the statement; pkg/sessionctx/variable/sysvar.go:139 relies on
    // this for a stable `now()` per statement.
    let cache = StmtCache::new();
    let first = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let second = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_007);

    let stored = cache.get_or_store(StmtCacheKey::NowTs, StmtCacheValue::NowTs(first));
    assert_eq!(stored, StmtCacheValue::NowTs(first));
    let stored = cache.get_or_store(StmtCacheKey::NowTs, StmtCacheValue::NowTs(second));
    assert_eq!(stored, StmtCacheValue::NowTs(first));

    // Keys are independent slots, as in the source map.
    let safe = cache.get_or_store(StmtCacheKey::SafeTs, StmtCacheValue::SafeTs(425));
    assert_eq!(safe, StmtCacheValue::SafeTs(425));
    assert_eq!(
        cache.get_or_store(StmtCacheKey::NowTs, StmtCacheValue::NowTs(second)),
        StmtCacheValue::NowTs(first)
    );
}

#[test]
fn get_or_evaluate_runs_the_evaluator_once_and_never_caches_errors() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:837-852. The evaluator only
    // runs on a miss, and an evaluator error leaves the slot empty so a
    // later call re-evaluates (pkg/sessiontxn/staleread/util.go:154-164).
    let cache = StmtCache::new();
    let mut calls = 0;

    let value = cache.get_or_evaluate::<&str>(StmtCacheKey::ExternalTs, || {
        calls += 1;
        Ok(StmtCacheValue::ExternalTs(77))
    });
    assert_eq!(value, Ok(StmtCacheValue::ExternalTs(77)));
    assert_eq!(calls, 1);

    let value = cache.get_or_evaluate::<&str>(StmtCacheKey::ExternalTs, || {
        calls += 1;
        Ok(StmtCacheValue::ExternalTs(99))
    });
    assert_eq!(value, Ok(StmtCacheValue::ExternalTs(77)));
    assert_eq!(calls, 1);

    let failed = cache.get_or_evaluate(StmtCacheKey::SafeTs, || Err("pd unavailable"));
    assert_eq!(failed, Err("pd unavailable"));
    let value = cache.get_or_evaluate::<&str>(StmtCacheKey::SafeTs, || {
        Ok(StmtCacheValue::SafeTs(1234))
    });
    assert_eq!(value, Ok(StmtCacheValue::SafeTs(1234)));
}

#[test]
fn reset_in_clears_one_key_and_reset_clears_all() {
    // Source: pkg/sessionctx/stmtctx/stmtctx.go:854-866 and :216-222.
    let cache = StmtCache::new();
    cache.get_or_store(StmtCacheKey::SafeTs, StmtCacheValue::SafeTs(1));
    cache.get_or_store(StmtCacheKey::ExternalTs, StmtCacheValue::ExternalTs(2));

    cache.reset_in(StmtCacheKey::SafeTs);
    assert_eq!(
        cache.get_or_store(StmtCacheKey::SafeTs, StmtCacheValue::SafeTs(10)),
        StmtCacheValue::SafeTs(10)
    );
    assert_eq!(
        cache.get_or_store(StmtCacheKey::ExternalTs, StmtCacheValue::ExternalTs(20)),
        StmtCacheValue::ExternalTs(2)
    );

    cache.reset();
    assert_eq!(
        cache.get_or_store(StmtCacheKey::ExternalTs, StmtCacheValue::ExternalTs(30)),
        StmtCacheValue::ExternalTs(30)
    );

    // Resetting an absent key is a no-op, like Go's delete on the map.
    cache.reset_in(StmtCacheKey::NowTs);
}

#[test]
fn value_accessors_mirror_the_source_type_assertions() {
    // Source narrowing sites: pkg/sessionctx/variable/sysvar.go:139
    // (`.(time.Time)`), pkg/expression/builtin_time.go:7165 (`.(uint64)`),
    // pkg/sessiontxn/staleread/util.go:163 (`.(uint64)`).
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(42);
    assert_eq!(StmtCacheValue::NowTs(now).as_now_ts(), Some(now));
    assert_eq!(StmtCacheValue::NowTs(now).as_safe_ts(), None);
    assert_eq!(StmtCacheValue::SafeTs(7).as_safe_ts(), Some(7));
    assert_eq!(StmtCacheValue::SafeTs(7).as_external_ts(), None);
    assert_eq!(StmtCacheValue::ExternalTs(9).as_external_ts(), Some(9));
    assert_eq!(StmtCacheValue::ExternalTs(9).as_now_ts(), None);
}
