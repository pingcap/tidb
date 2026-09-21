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

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::executor::{Executor, ExecutorMeta};
use tidb_executor::expand::ExpandExec;
use tidb_executor::projection::ProjectionExec;
use tidb_executor::table_dual::TableDualExec;
use tidb_expr::NoColumns;
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::Long)
}

fn constant(value: Datum) -> Expression {
    Expression::Constant(Constant::new(value, long()))
}

#[test]
fn cached_child_chunk_is_evaluated_once_per_expand_level() {
    let dual = TableDualExec::new(ExecutorMeta::new(Schema::new(vec![]), 1, 1, 1024), 1);
    let input_column = Column::new(10, long());
    let child = ProjectionExec::new(
        ExecutorMeta::new(Schema::new(vec![input_column.clone()]), 2, 1, 1024),
        vec![constant(Datum::Int(7))],
        Box::new(dual),
        NoColumns,
    );

    let mut direct = input_column;
    direct.index = 0;
    let output_schema = Schema::new(vec![Column::new(20, long())]);
    let mut expand = ExpandExec::new(
        ExecutorMeta::new(output_schema, 3, 1, 1024),
        vec![
            vec![Expression::Column(direct)],
            vec![constant(Datum::Null)],
        ],
        Box::new(child),
        NoColumns,
        tidb_executor::StmtContext::for_query().statement_memory(),
    );

    expand.open().unwrap();
    let mut result = expand.new_chunk();
    expand.next(&mut result).unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.get_row(0).get_int64(0), 7);

    expand.next(&mut result).unwrap();
    assert_eq!(result.num_rows(), 1);
    assert!(result.get_row(0).is_null(0));

    expand.next(&mut result).unwrap();
    assert_eq!(result.num_rows(), 0);
    expand.close().unwrap();
}
