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

//! Source-backed string predicate-to-TiPB vectors.

use tidb_datatype::FieldTypeCode;
use tidb_expr::pb_predicate::string_in_to_pb;
use tidb_proto::tipb::{Expr, ExprType, FieldType, ScalarFuncSig};

#[test]
fn string_in_deduplicates_large_lists_without_changing_survivor_order() {
    let tested = Expr {
        tp: Some(ExprType::ColumnRef as i32),
        val: Some(vec![0x80]),
        children: Vec::new(),
        sig: Some(0),
        field_type: Some(FieldType {
            tp: Some(FieldTypeCode::Varchar.mysql_type().into()),
            flag: Some(0),
            flen: Some(64),
            decimal: Some(-1),
            collate: Some(tidb_datatype::collation_to_proto("ascii_bin")),
            charset: Some("ascii".to_owned()),
            elems: Vec::new(),
            array: Some(false),
        }),
        has_distinct: Some(false),
    };
    let mut literals = (0..2_000)
        .map(|value| format!("0x{value:040X}").into_bytes())
        .collect::<Vec<_>>();
    literals.push(literals[17].clone());
    literals.push(literals[1_999].clone());

    let expression = string_in_to_pb(tested, literals, "ascii_bin").unwrap();
    assert_eq!(expression.sig, Some(ScalarFuncSig::InString as i32));
    assert_eq!(expression.children.len(), 2_001);
    assert_eq!(expression.children[1].tp, Some(ExprType::String as i32));
    assert_eq!(
        expression.children[1].val.as_deref(),
        Some(b"0x0000000000000000000000000000000000000000".as_slice())
    );
    assert_eq!(
        expression.children[2_000].val.as_deref(),
        Some(b"0x00000000000000000000000000000000000007CF".as_slice())
    );
}
