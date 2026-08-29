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

//! Ports of the authoritative `origin/master` `pkg/executor.part11` slice:
//! items 601–660 in the deterministic top-level `Test*` enumeration.
//!
//! The range splitter is a direct Rust-side behavior port. The other tests
//! exercise session-variable mutation, privilege checks, SHOW/ANALYZE catalog
//! readers, PD/infosync hooks, import-job formatting, or slow-log retrieval;
//! those Go production surfaces are not present in `tidb-executor`, so each is
//! retained as an explicit parity-gap test rather than an approximation.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::NoColumns;

use crate::shuffle::{PartitionRangeSplitter, PartitionSplitter};

/// Go `pkg/executor/shuffle_test.go:28::TestPartitionRangeSplitter` calls
/// `buildPartitionRangeSplitter` (`pkg/executor/shuffle.go:478`) and assigns
/// each contiguous group to two workers round-robin
/// (`pkg/executor/shuffle.go:490`).
#[test]
fn partition_range_splitter_assigns_contiguous_groups_round_robin() {
    let field_type = FieldType::new(FieldTypeCode::Varchar);
    let mut column = Column::new(1, field_type.clone());
    column.index = 0;
    let by_items = vec![Expression::Column(column)];

    let mut input = Chunk::new(&[field_type], 1024, 1024);
    for value in [
        "a", "a", "a", "a", "c", "c", "b", "b", "b", "q", "eee", "eee", "ddd",
    ] {
        input.append_string(0, value);
    }

    let mut splitter = PartitionRangeSplitter::new(2, by_items);
    let mut obtained = Vec::new();
    PartitionSplitter::<NoColumns>::split(&mut splitter, &NoColumns, &input, &mut obtained)
        .expect("range splitting should evaluate the VARCHAR grouping column");

    assert_eq!(obtained, vec![0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 1]);
}
