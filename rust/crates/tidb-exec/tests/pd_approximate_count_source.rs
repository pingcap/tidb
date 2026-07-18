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

//! Source-backed tests for the approximate table-count cache boundary.

use std::time::Duration;

use tidb_exec::pd_approximate_count::{approximate_table_count_key, ApproximateTableCountCache};

fn load_count(
    cache: &mut ApproximateTableCountCache,
    now: Duration,
    table_id: i64,
    db_name: &str,
    table_name: &str,
    partition_name: &str,
    miss_count: &mut usize,
) -> (f64, bool) {
    cache.get_or_load_table(now, table_id, db_name, table_name, partition_name, || {
        *miss_count += 1;
        (1.0, true)
    })
}

#[test]
fn ttl_cache_hit_miss_capacity_and_expiry_match_source() {
    // Source: pkg/executor/internal/pdhelper/pd.go:69-85.
    // Direct Go coverage: pkg/executor/internal/pdhelper/pd_test.go:42
    // (TestTTLCache), capacity=2 and TTL=100ms.
    let mut cache = ApproximateTableCountCache::new(2, Duration::from_millis(100));
    let mut miss_count = 0;

    let now = Duration::ZERO;
    assert_eq!(
        load_count(
            &mut cache,
            now,
            1,
            "db",
            "table",
            "partition",
            &mut miss_count,
        ),
        (1.0, true)
    );
    assert_eq!(miss_count, 1);
    assert_eq!(
        load_count(
            &mut cache,
            now,
            1,
            "db",
            "table",
            "partition",
            &mut miss_count,
        ),
        (1.0, true)
    );
    assert_eq!(miss_count, 1);

    assert_eq!(
        load_count(
            &mut cache,
            now,
            2,
            "db1",
            "table1",
            "partition",
            &mut miss_count,
        ),
        (1.0, true)
    );
    assert_eq!(
        load_count(
            &mut cache,
            now,
            3,
            "db2",
            "table2",
            "partition",
            &mut miss_count,
        ),
        (1.0, true)
    );
    // Capacity eviction is LRU: the first key was touched before key 2 and
    // key 3 arrived, so looking it up again is a miss.
    assert_eq!(
        load_count(
            &mut cache,
            now,
            1,
            "db",
            "table",
            "partition",
            &mut miss_count,
        ),
        (1.0, true)
    );
    assert_eq!(
        load_count(
            &mut cache,
            now,
            3,
            "db2",
            "table2",
            "partition",
            &mut miss_count,
        ),
        (1.0, true)
    );
    assert_eq!(miss_count, 4);

    let expired = Duration::from_millis(200);
    for (id, db, table) in [
        (1, "db", "table"),
        (2, "db1", "table1"),
        (3, "db2", "table2"),
    ] {
        assert_eq!(
            load_count(
                &mut cache,
                expired,
                id,
                db,
                table,
                "partition",
                &mut miss_count,
            ),
            (1.0, true)
        );
    }
    assert_eq!(miss_count, 7);
}

#[test]
fn approximate_table_count_key_preserves_source_join() {
    // Source: pkg/executor/internal/pdhelper/pd.go:69-70.
    // Direct Go coverage: pkg/executor/internal/pdhelper/pd_test.go:42
    // (TestTTLCache), whose distinct table IDs exercise cache identity.
    assert_eq!(
        approximate_table_count_key(42, "db", "table", "p0"),
        "42_db_table_p0"
    );
    assert_eq!(
        approximate_table_count_key(-1, "db_with_under", "table", ""),
        "-1_db_with_under_table_"
    );
}
