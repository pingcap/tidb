// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/planner/core/rule/rule_partition_pruning_test.go` range-columns
//! pruning benchmarks.

use std::hint::black_box;
use std::time::Instant;

use tidb_datatype::{Collation, Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_executor::partition_pruning::pruned_ids_from_ranger;
use tidb_executor::partition_routing::{
    PartitionDef, PartitionKind, PartitionSpec, RangeColumnBound,
};
use tidb_expr::{column::Column, expression::Expression, NoColumns};
use tidb_planner::ranger::types::Range;

const PARTITION_COUNTS: [usize; 5] = [2, 10, 100, 1_000, 8_000];
const ITERATIONS: usize = 10_000;

fn benchmark_spec(parts: usize) -> PartitionSpec {
    let field_type = FieldType::new(FieldTypeCode::LongLong)
        .with_flags(FieldTypeFlags::UNSIGNED)
        .with_collation(Collation::Binary);
    let mut column = Column::new(1, field_type.clone());
    column.index = 0;
    let mut less_than = (0..parts - 1)
        .map(|ordinal| {
            vec![RangeColumnBound::Value(Datum::UInt(
                (ordinal * 10_000) as u64,
            ))]
        })
        .collect::<Vec<_>>();
    less_than.push(vec![RangeColumnBound::MaxValue]);
    PartitionSpec {
        overlapping_dropping_partition_indices: Vec::new(),
        is_empty_columns: false,
        kind: PartitionKind::RangeColumns {
            less_than,
            field_types: vec![field_type],
        },
        expr_text: "`a`".to_owned(),
        expr: Expression::Column(column),
        dependencies: vec!["a".to_owned()],
        definitions: (0..parts)
            .map(|ordinal| PartitionDef {
                id: ordinal as i64,
                name: format!("p{ordinal}"),
                less_than: Vec::new(),
                in_values: Vec::new(),
                comment: String::new(),
                placement_policy: None,
            })
            .collect(),
    }
}

fn benchmark_range() -> Range {
    Range {
        low_val: vec![Datum::UInt(11_000)],
        high_val: vec![Datum::MaxValue],
        collators: vec![Collation::Binary],
        low_exclude: true,
        high_exclude: false,
    }
}

fn main() {
    let range = benchmark_range();
    for parts in PARTITION_COUNTS {
        let spec = benchmark_spec(parts);
        let started = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(
                pruned_ids_from_ranger(&spec, std::slice::from_ref(&range), &NoColumns)
                    .expect("range-columns pruning succeeds"),
            );
        }
        println!(
            "BenchmarkRangeColumnsPruner{parts}: {:?}",
            started.elapsed()
        );
    }
}
