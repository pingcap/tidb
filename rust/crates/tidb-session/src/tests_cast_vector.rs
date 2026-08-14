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

use super::{Session, StmtResult};
use crate::tests_support::row_text;
use tidb_datatype::Datum;

#[test]
fn cast_as_vector_uses_the_vector_value_domain() {
    let mut session = Session::new();

    let result = session
        .run("SELECT CAST('[1,2.5]' AS VECTOR)")
        .expect("valid vector cast");
    let StmtResult::Rows(rows) = &result else {
        panic!("expected rows, got {result:?}");
    };
    assert!(matches!(rows[0][0], Datum::VectorFloat32(_)));
    assert_eq!(row_text(Ok(result)), [["[1,2.5]"]]);

    assert_eq!(
        row_text(session.run("SELECT CAST('[1,2]' AS VECTOR(2))")),
        [["[1,2]"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST('[1,2]' AS VECTOR<FLOAT>(2))")),
        [["[1,2]"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CONVERT('[1,2]', VECTOR(2))")),
        [["[1,2]"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST(CAST('[1,2]' AS VECTOR(2)) AS VECTOR(2))")),
        [["[1,2]"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST(VEC_FROM_TEXT('[1,2]') AS VECTOR(2))")),
        [["[1,2]"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST('[]' AS VECTOR)")),
        [["[]"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST(NULL AS VECTOR(2)) IS NULL")),
        [["1"]]
    );

    assert_eq!(
        session
            .run("SELECT CAST('[1,2]' AS VECTOR(3))")
            .unwrap_err()
            .to_string(),
        "vector has 2 dimensions, does not fit VECTOR(3)"
    );
    assert_eq!(
        session
            .run("SELECT CAST('not-a-vector' AS VECTOR)")
            .unwrap_err()
            .to_string(),
        "Invalid vector text: not-a-vector"
    );
    assert_eq!(
        session
            .run("SELECT CAST(1 AS VECTOR)")
            .unwrap_err()
            .to_string(),
        "cannot cast from bigint to vector"
    );
}
