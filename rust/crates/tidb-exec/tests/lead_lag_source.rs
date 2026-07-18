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

//! Source-backed tests for `LAG`/`LEAD` row-offset selection.

use tidb_exec::lead_lag::{
    LeadLag, LeadLagDefault, LeadLagDirection, LeadLagPartialState, LeadLagSelection,
};

fn drain(window: &mut LeadLag, rows: usize) -> Vec<Option<LeadLagSelection>> {
    (0..rows).map(|_| window.next_selection()).collect()
}

fn source_rows(offset: u64, direction: LeadLagDirection) -> Vec<Option<LeadLagSelection>> {
    let mut window = LeadLag::new(direction, offset, LeadLagDefault::Null);
    window.update(3);
    drain(&mut window, 3)
}

#[test]
fn lead_lag_partial_state_and_rust_handle_memory_gap_are_explicit() {
    // Source: pkg/executor/aggfuncs/func_lead_lag.go:36-43.
    // Direct Go coverage: pkg/executor/aggfuncs/func_lead_lag_test.go:119
    // (TestMemLeadLag).
    assert_eq!(
        LeadLag::partial_state_size(),
        std::mem::size_of::<LeadLagPartialState>()
    );
    assert_eq!(
        LeadLag::partial_state_size(),
        std::mem::size_of::<Vec<usize>>() + std::mem::size_of::<u64>()
    );
    // This partial Rust seam stores an 8-byte physical-position handle. The
    // source stores a 16-byte chunk.Row, so TestMemLeadLag byte parity remains
    // deliberately open rather than hidden behind this logical accounting.
    assert_eq!(LeadLag::buffered_row_size(), 8);

    let mut window = LeadLag::new(LeadLagDirection::Lag, 1_000_000, LeadLagDefault::Null);
    assert_eq!(window.update(1), LeadLag::buffered_row_size());
    assert_eq!(
        window.update(2),
        2usize.saturating_mul(LeadLag::buffered_row_size())
    );
    assert_eq!(
        window.buffered_row_memory(),
        3usize.saturating_mul(LeadLag::buffered_row_size())
    );
    assert_eq!(
        drain(&mut window, 3),
        vec![
            Some(LeadLagSelection::Null),
            Some(LeadLagSelection::Null),
            Some(LeadLagSelection::Null),
        ]
    );
    window.reset();
    assert_eq!(window.buffered_row_memory(), 0);
}

#[test]
fn lead_lag_cursor_preserves_executor_physical_row_handles() {
    let mut window = LeadLag::new(LeadLagDirection::Lead, 1, LeadLagDefault::CurrentRow);
    assert_eq!(
        window.update_rows([7, 3, 11]),
        3usize.saturating_mul(LeadLag::buffered_row_size())
    );
    assert_eq!(
        drain(&mut window, 3),
        vec![
            Some(LeadLagSelection::Source(3)),
            Some(LeadLagSelection::Source(11)),
            Some(LeadLagSelection::Default(11)),
        ]
    );
}

#[test]
fn lag_vectors_match_source_test_lead_lag() {
    // Source: pkg/executor/aggfuncs/func_lead_lag.go:63-75.
    // Direct Go coverage: pkg/executor/aggfuncs/func_lead_lag_test.go:27
    // (TestLeadLag), including offsets 0, 1, 2, 3, and 1,000,000.
    assert_eq!(
        source_rows(0, LeadLagDirection::Lag),
        vec![
            Some(LeadLagSelection::Source(0)),
            Some(LeadLagSelection::Source(1)),
            Some(LeadLagSelection::Source(2)),
        ]
    );
    assert_eq!(
        source_rows(1, LeadLagDirection::Lag),
        vec![
            Some(LeadLagSelection::Null),
            Some(LeadLagSelection::Source(0)),
            Some(LeadLagSelection::Source(1)),
        ]
    );
    assert_eq!(
        source_rows(2, LeadLagDirection::Lag),
        vec![
            Some(LeadLagSelection::Null),
            Some(LeadLagSelection::Null),
            Some(LeadLagSelection::Source(0)),
        ]
    );
    for offset in [3, 1_000_000] {
        assert_eq!(
            source_rows(offset, LeadLagDirection::Lag),
            vec![
                Some(LeadLagSelection::Null),
                Some(LeadLagSelection::Null),
                Some(LeadLagSelection::Null),
            ]
        );
    }
}

#[test]
fn lead_vectors_match_source_test_lead_lag() {
    // Source: pkg/executor/aggfuncs/func_lead_lag.go:45-60.
    // Direct Go coverage: pkg/executor/aggfuncs/func_lead_lag_test.go:27
    // (TestLeadLag), including offsets 0, 1, 2, 3, and 1,000,000.
    assert_eq!(
        source_rows(0, LeadLagDirection::Lead),
        vec![
            Some(LeadLagSelection::Source(0)),
            Some(LeadLagSelection::Source(1)),
            Some(LeadLagSelection::Source(2)),
        ]
    );
    assert_eq!(
        source_rows(1, LeadLagDirection::Lead),
        vec![
            Some(LeadLagSelection::Source(1)),
            Some(LeadLagSelection::Source(2)),
            Some(LeadLagSelection::Null),
        ]
    );
    assert_eq!(
        source_rows(2, LeadLagDirection::Lead),
        vec![
            Some(LeadLagSelection::Source(2)),
            Some(LeadLagSelection::Null),
            Some(LeadLagSelection::Null),
        ]
    );
    for offset in [3, 1_000_000] {
        assert_eq!(
            source_rows(offset, LeadLagDirection::Lead),
            vec![
                Some(LeadLagSelection::Null),
                Some(LeadLagSelection::Null),
                Some(LeadLagSelection::Null),
            ]
        );
    }

    // Go performs `curIdx + offset` as uint64 before its bounds check.
    // Preserve the resulting wrap for the two largest offsets.
    assert_eq!(
        source_rows(u64::MAX, LeadLagDirection::Lead),
        vec![
            Some(LeadLagSelection::Null),
            Some(LeadLagSelection::Source(0)),
            Some(LeadLagSelection::Source(1)),
        ]
    );
    assert_eq!(
        source_rows(u64::MAX - 1, LeadLagDirection::Lead),
        vec![
            Some(LeadLagSelection::Null),
            Some(LeadLagSelection::Null),
            Some(LeadLagSelection::Source(0)),
        ]
    );
}

#[test]
fn lead_lag_default_argument_is_evaluated_at_current_row() {
    // Source: pkg/executor/aggfuncs/func_lead_lag.go:63-75.
    // The Go default expression receives p.rows[p.curIdx], so this leaf
    // returns the current row index rather than a fixed default value.
    let mut lag = LeadLag::new(LeadLagDirection::Lag, 1, LeadLagDefault::CurrentRow);
    lag.update(3);
    assert_eq!(
        drain(&mut lag, 3),
        vec![
            Some(LeadLagSelection::Default(0)),
            Some(LeadLagSelection::Source(0)),
            Some(LeadLagSelection::Source(1)),
        ]
    );

    let mut lead = LeadLag::new(LeadLagDirection::Lead, 1, LeadLagDefault::CurrentRow);
    lead.update(3);
    assert_eq!(
        drain(&mut lead, 3),
        vec![
            Some(LeadLagSelection::Source(1)),
            Some(LeadLagSelection::Source(2)),
            Some(LeadLagSelection::Default(2)),
        ]
    );
}

#[test]
fn lead_lag_batches_and_reset_match_source_state() {
    let mut window = LeadLag::new(LeadLagDirection::Lead, 1, LeadLagDefault::Null);
    window.update(1);
    window.update(2);
    assert_eq!(
        drain(&mut window, 3),
        vec![
            Some(LeadLagSelection::Source(1)),
            Some(LeadLagSelection::Source(2)),
            Some(LeadLagSelection::Null),
        ]
    );

    window.reset();
    window.update(3);
    assert_eq!(
        drain(&mut window, 3),
        vec![
            Some(LeadLagSelection::Source(1)),
            Some(LeadLagSelection::Source(2)),
            Some(LeadLagSelection::Null),
        ]
    );
}
