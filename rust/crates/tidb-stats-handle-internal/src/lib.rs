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

//! Go `pkg/statistics/handle/internal`.

use std::io::Write;

use tidb_stats::{Histogram, Table, TopN};

fn histogram_text(histogram: &Histogram) -> Vec<u8> {
    let mut text = format!(
        "column:{} ndv:{} totColSize:{}",
        histogram.id, histogram.ndv, histogram.tot_col_size
    )
    .into_bytes();
    let mut previous_count = 0_i64;
    for bucket in &histogram.buckets {
        text.push(b'\n');
        write!(
            text,
            "num: {} lower_bound: ",
            bucket.count.wrapping_sub(previous_count)
        )
        .expect("writing to a byte vector cannot fail");
        text.extend_from_slice(&bucket.lower_bound.sql_bytes().unwrap_or_default());
        text.extend_from_slice(b" upper_bound: ");
        text.extend_from_slice(&bucket.upper_bound.sql_bytes().unwrap_or_default());
        write!(text, " repeats: {} ndv: {}", bucket.repeat, bucket.ndv)
            .expect("writing to a byte vector cannot fail");
        previous_count = bucket.count;
    }
    text
}

fn histogram_equal(left: &Histogram, right: &Histogram) -> bool {
    histogram_text(left) == histogram_text(right)
}

fn top_n_equal(left: Option<&TopN>, right: Option<&TopN>) -> bool {
    let left_count = left.map_or(0, TopN::total_count);
    let right_count = right.map_or(0, TopN::total_count);
    if left_count == 0 && right_count == 0 {
        return true;
    }
    if left_count != right_count {
        return false;
    }
    match (left, right) {
        (Some(left), Some(right)) => left.resolved_entries() == right.resolved_entries(),
        _ => false,
    }
}

/// Go `AssertTableEqual`.
pub fn assert_table_equal(left: &Table, right: &Table) {
    assert_eq!(
        left.hist_coll.realtime_count,
        right.hist_coll.realtime_count
    );
    assert_eq!(left.hist_coll.modify_count, right.hist_coll.modify_count);
    assert_eq!(
        left.hist_coll.column_count(),
        right.hist_coll.column_count()
    );
    left.hist_coll.for_each_column(|id, left_column| {
        let right_column = right
            .hist_coll
            .get_column(id)
            .expect("right table is missing a column");
        let right_column = right_column
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(histogram_equal(
            &left_column.histogram,
            &right_column.histogram
        ));
        assert_eq!(left_column.cmsketch, right_column.cmsketch);
        assert!(top_n_equal(
            left_column.top_n.as_ref(),
            right_column.top_n.as_ref()
        ));
        false
    });

    assert_eq!(left.hist_coll.index_count(), right.hist_coll.index_count());
    left.hist_coll.for_each_index(|id, left_index| {
        let right_index = right
            .hist_coll
            .get_index(id)
            .expect("right table is missing an index");
        let right_index = right_index
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(histogram_equal(
            &left_index.histogram,
            &right_index.histogram
        ));
        assert_eq!(left_index.cmsketch, right_index.cmsketch);
        assert!(top_n_equal(
            left_index.top_n.as_ref(),
            right_index.top_n.as_ref()
        ));
        false
    });

    let left_existence = left
        .existence_map
        .as_ref()
        .expect("left table has no column/index existence map")
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let right_existence = right
        .existence_map
        .as_ref()
        .expect("right table has no column/index existence map")
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(left_existence.is_equal(&right_existence));
}
