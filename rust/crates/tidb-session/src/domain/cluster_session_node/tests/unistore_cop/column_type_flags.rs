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


//! YEAR, BIT and ZEROFILL columns carry Go's `UnsignedFlag`
//! (`processColumnFlags`), visible in arithmetic and in the catalog's column
//! text, over the cop-backed TestKit stack (X11).

use super::sql_kit::*;
use super::*;

fn flag_table(tk: &mut Session) {
    exec_all(tk, &[
        "CREATE TABLE y (a YEAR, b BIT(8), c INT UNSIGNED, d INT ZEROFILL, e VARCHAR(5))",
        "INSERT INTO y VALUES (1990, 1, 1, 1, 'x')",
    ]);
}

#[test]
fn year_and_bit_columns_are_unsigned() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 620);
    flag_table(&mut tk);
    for column in ["a", "b", "c", "d"] {
        let sql = format!("SELECT {column} - 2000 FROM y");
        let refused = error(&mut tk, &sql);
        assert_eq!(
            (refused.code, refused.message.as_str()),
            (1690, format!("BIGINT UNSIGNED value is out of range in '(test.y.{column} - 2000)'").as_str()),
            "{sql}"
        );
    }
    assert_eq!(rows(&mut tk, "SELECT a + 0 FROM y"), vec![vec![Datum::UInt(1990)]]);
    assert_eq!(rows(&mut tk, "SELECT a - 1000 FROM y"), vec![vec![Datum::UInt(990)]]);
    assert_eq!(rows(&mut tk, "SELECT b + 0 FROM y"), vec![vec![Datum::UInt(1)]]);
    assert_eq!(rows(&mut tk, "SELECT LENGTH(e) - 2000 FROM y"), vec![vec![Datum::Int(-1999)]]);
}

/// The flags change no catalog text: `COLUMN_TYPE` is Go's `InfoSchemaStr`,
/// which drops ZEROFILL and, under the TestKit's
/// `deprecate-integer-display-length`, the default integer width.
#[test]
fn the_new_flags_change_no_catalog_text() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 621);
    flag_table(&mut tk);
    assert_eq!(
        q(&mut tk, "SELECT column_name, column_type FROM information_schema.columns WHERE table_schema = 'test' AND table_name = 'y' ORDER BY ordinal_position"),
        [["a", "year(4)"], ["b", "bit(8)"], ["c", "int unsigned"], ["d", "int(10) unsigned"], ["e", "varchar(5)"]]
    );
}

