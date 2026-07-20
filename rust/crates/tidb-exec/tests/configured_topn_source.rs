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

//! Source-backed bounded TopN and lazy LIMIT-only tests.

use std::collections::VecDeque;

use tidb_datatype::{Datum, DatumKind};
use tidb_exec::{
    configured_topn::{
        ConfiguredLimitStream, ConfiguredRowSource, ConfiguredTopN, ConfiguredTopNError,
    },
    order::ConfiguredOrderError,
    Row,
};
use tidb_planner::configured_order_limit_contract::{
    ConfiguredLimitWindow, ConfiguredOrderDirection, ConfiguredOrderKey, ConfiguredOrderLimitSpec,
};

fn topn_spec(
    offset: usize,
    count: usize,
    keys: &[(usize, ConfiguredOrderDirection)],
) -> ConfiguredOrderLimitSpec {
    ConfiguredOrderLimitSpec::new(
        keys.iter()
            .map(|&(offset, direction)| ConfiguredOrderKey::new(offset, direction))
            .collect(),
        ConfiguredLimitWindow::new(offset, count).expect("test window"),
    )
    .expect("at least one test key")
}

#[test]
fn configured_topn_uses_bounded_max_heap_multi_key_order_and_stable_ties() {
    // Source: pkg/executor/sortexec/topn.go:60-68, 364-395, 548-569 and
    // pkg/executor/executor_required_rows_test.go:321-430 (TestTopNRequiredRows).
    let spec = topn_spec(
        1,
        5,
        &[
            (2, ConfiguredOrderDirection::Ascending),
            (1, ConfiguredOrderDirection::Descending),
        ],
    );
    let mut topn = ConfiguredTopN::new(spec, 3, 6).expect("checked capacity");
    for row in [
        vec![Datum::Int(100), Datum::Int(2), Datum::Int(2)],
        vec![Datum::Int(200), Datum::Int(9), Datum::Int(1)],
        vec![Datum::Int(300), Datum::Int(9), Datum::Int(2)],
        vec![Datum::Int(400), Datum::Int(9), Datum::Int(2)],
        vec![Datum::Int(500), Datum::Int(8), Datum::Int(1)],
        vec![Datum::Int(600), Datum::Int(7), Datum::Int(0)],
        vec![Datum::Int(700), Datum::Int(6), Datum::Int(0)],
    ] {
        topn.push(row).expect("valid configured row");
        assert!(topn.retained_len() <= 6, "heap never exceeds LIMIT end");
    }

    let result = topn.finish();
    assert_eq!(
        result.rows,
        vec![
            vec![Datum::Int(700), Datum::Int(6), Datum::Int(0)],
            vec![Datum::Int(200), Datum::Int(9), Datum::Int(1)],
            vec![Datum::Int(500), Datum::Int(8), Datum::Int(1)],
            vec![Datum::Int(300), Datum::Int(9), Datum::Int(2)],
            vec![Datum::Int(400), Datum::Int(9), Datum::Int(2)],
        ],
        "offset skips the globally best row; equal complete keys retained source order"
    );
    assert_eq!(result.evidence.capacity(), 6);
    assert_eq!(result.evidence.high_water_candidates(), 6);
    assert_eq!(result.evidence.rows_consumed(), 7);
    assert_eq!(result.evidence.rows_emitted(), 5);
}

#[test]
fn configured_topn_rejects_capacity_and_bad_rows_before_heap_admission() {
    // Source: pkg/executor/sortexec/topn.go:303-367 plus
    // pkg/executor/sortexec/topn_spill_test.go:454-513 (TestTopNSpillDiskFailpoint).
    let spec = topn_spec(2, 3, &[(1, ConfiguredOrderDirection::Ascending)]);
    assert!(
        matches!(
            ConfiguredTopN::new(spec.clone(), 2, 4),
            Err(ConfiguredTopNError::CapacityExceeded {
                end_exclusive: 5,
                capacity: 4,
            })
        ),
        "capacity fails before any source is consumed"
    );
    let invalid_key_spec = topn_spec(0, 1, &[(2, ConfiguredOrderDirection::Ascending)]);
    assert!(matches!(
        ConfiguredTopN::new(invalid_key_spec, 2, 1),
        Err(ConfiguredTopNError::Order(
            ConfiguredOrderError::FullSchemaOffset {
                offset: 2,
                width: 2,
            }
        ))
    ));

    let mut topn = ConfiguredTopN::new(spec, 2, 5).expect("exact capacity");
    assert_eq!(
        topn.push(vec![Datum::Int(1)]),
        Err(ConfiguredTopNError::Order(ConfiguredOrderError::RowWidth {
            row_index: 0,
            expected: 2,
            actual: 1,
        }))
    );
    assert_eq!(topn.retained_len(), 0, "bad row never reaches the heap");
    assert_eq!(
        topn.push(vec![Datum::Int(1), Datum::UInt(2)]),
        Err(ConfiguredTopNError::Order(ConfiguredOrderError::KeyDatum {
            row_index: 0,
            offset: 1,
            kind: DatumKind::UInt,
        }))
    );
    assert_eq!(topn.retained_len(), 0);
}

#[test]
fn configured_topn_limit_zero_bypasses_capacity_ordering_and_source_consumption() {
    // Source: pkg/executor/sortexec/topn.go:303-306. The Campaign 26
    // contract makes this a typed empty fast path before PD/TiKV work.
    let empty_spec = topn_spec(
        usize::MAX,
        0,
        &[(usize::MAX, ConfiguredOrderDirection::Descending)],
    );
    let mut topn = ConfiguredTopN::new(empty_spec, 0, 1).expect("LIMIT 0 is always local");
    assert!(topn.is_empty());
    topn.push(vec![Datum::UInt(1)])
        .expect("empty TopN does not validate or consume a row");
    assert_eq!(topn.retained_len(), 0);
    let result = topn.finish();
    assert!(result.rows.is_empty());
    assert_eq!(result.evidence.capacity(), 1);
    assert_eq!(result.evidence.high_water_candidates(), 0);
    assert_eq!(result.evidence.rows_consumed(), 0);
    assert_eq!(result.evidence.rows_emitted(), 0);
}

#[derive(Default)]
struct CountingRowSource {
    rows: VecDeque<Row>,
    next_calls: usize,
    close_calls: usize,
}

impl CountingRowSource {
    fn new(rows: impl IntoIterator<Item = Row>) -> Self {
        Self {
            rows: rows.into_iter().collect(),
            ..Self::default()
        }
    }
}

impl ConfiguredRowSource for CountingRowSource {
    type Error = &'static str;

    fn next_row(&mut self) -> Result<Option<Row>, Self::Error> {
        self.next_calls += 1;
        Ok(self.rows.pop_front())
    }

    fn close(&mut self) {
        self.close_calls += 1;
        self.rows.clear();
    }
}

#[test]
fn configured_limit_stream_skips_lazily_and_closes_at_the_exact_window_end() {
    // Source: pkg/executor/executor_required_rows_test.go:131-210
    // (TestLimitRequiredRows) and pkg/executor/chunk_size_control_test.go:99-157.
    let mut source = CountingRowSource::new((0..10).map(|value| vec![Datum::Int(value)]));
    let mut limit = ConfiguredLimitStream::new(ConfiguredLimitWindow::new(2, 3).unwrap());
    assert_eq!(limit.next(&mut source).unwrap(), Some(vec![Datum::Int(2)]));
    assert_eq!(limit.next(&mut source).unwrap(), Some(vec![Datum::Int(3)]));
    assert_eq!(limit.next(&mut source).unwrap(), Some(vec![Datum::Int(4)]));
    assert_eq!(limit.next(&mut source).unwrap(), None);
    assert_eq!(source.next_calls, 5, "offset plus count only");
    assert_eq!(source.close_calls, 1, "one early source close");
    assert_eq!(
        limit.evidence().rows_requested(),
        5,
        "required-row propagation never drains hidden input"
    );
    assert_eq!(limit.evidence().rows_skipped(), 2);
    assert_eq!(limit.evidence().rows_emitted(), 3);
    assert!(limit.evidence().source_closed());
}

#[test]
fn configured_limit_zero_never_requests_an_upstream_row() {
    // Source: pkg/executor/sortexec/topn.go:303-306 and
    // pkg/executor/executor_required_rows_test.go:131-210 (TestLimitRequiredRows).
    let mut source = CountingRowSource::new([vec![Datum::Int(1)]]);
    let mut limit = ConfiguredLimitStream::new(ConfiguredLimitWindow::new(99, 0).unwrap());
    assert_eq!(limit.next(&mut source).unwrap(), None);
    assert_eq!(source.next_calls, 0);
    assert_eq!(source.close_calls, 1);
    assert_eq!(limit.evidence().rows_requested(), 0);
    assert!(limit.evidence().source_closed());
}
