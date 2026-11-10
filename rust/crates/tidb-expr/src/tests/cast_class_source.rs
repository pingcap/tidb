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

//! Everything Go's `BuildCastFunctionWithCheck` decides, against the node
//! [`build_cast_function_with_check`] builds.
//!
//! `testdata/cast_class_matrix.txt` is Go's own answer under
//! `mock.NewContext()`, with the `T` and `A` lines of
//! `function_class_matrix.txt` (`v` a vector, `E` an enum, `S` a set and
//! `B` a bit(12) column besides). Each `cast` line is a target type, a mode
//! (`0` plain, `1` in a UNION, `2` to a charset the statement names) and an
//! argument, then either
//!
//! - `cast:<PbCode>`, the return type, `coercibility,repertoire` and the
//!   argument the signature keeps: `=` as given or `c` replaced, its type
//!   and, when replaced, the function and signature or the constant value
//!   that replaced it; or
//! - `const`, the type and the value the cast folded to.

use std::collections::BTreeMap;

use tidb_datatype::FieldType;

use super::function_class_source::{argument, type_from_text, type_text, value_text, MockContext};
use crate::builtin::ColumnsBuildContext;
use crate::builtin_cast::build_cast_function_with_check;
use crate::expression::Expression;
use crate::pushdown_catalog::pb_code;
use crate::wrap_cast::type_of;

const MATRIX: &str = include_str!("../../testdata/cast_class_matrix.txt");

#[test]
fn build_cast_function_matches_go_for_every_target_and_argument() {
    let mut types: BTreeMap<&str, &str> = BTreeMap::new();
    let mut letters: BTreeMap<&str, FieldType> = BTreeMap::new();
    let mut wrong = Vec::new();
    let mut checked = 0;
    for line in MATRIX.lines() {
        let cells: Vec<&str> = line.split('\t').collect();
        match cells[0] {
            "T" => {
                types.insert(cells[1], cells[2]);
                continue;
            }
            "A" => {
                letters.insert(cells[1], type_from_text(types[cells[2]]));
                continue;
            }
            _ => {}
        }
        let (target, mode, label) = (cells[1], cells[2], cells[3]);
        let arg = argument(0, label.chars().next().expect("label"), letters[label].clone());
        let given = format!("{arg:?}");
        let built = build_cast_function_with_check(
            &ColumnsBuildContext(&MockContext),
            arg,
            type_from_text(types[target]),
            mode == "1",
            mode == "2",
        );
        let got = match &built {
            Ok(Expression::Constant(constant)) => format!(
                "const\t{}\t{}",
                type_text(&type_of(built.as_ref().expect("built"))),
                value_text(&constant.value)
            ),
            Ok(Expression::ScalarFunction(f)) => {
                let arg = &f.args[0];
                let kept = format!("{arg:?}") == given;
                let mut slot = format!("{}{}", if kept { '=' } else { 'c' }, type_text(&type_of(arg)));
                match arg {
                    Expression::ScalarFunction(inner) if !kept => {
                        slot += &format!(
                            ":{}:{}",
                            inner.func_name.lowercase(),
                            pb_code(arg).as_str_name()
                        );
                    }
                    Expression::Constant(constant) if !kept => {
                        slot += &format!(":{}", value_text(&constant.value));
                    }
                    _ => {}
                }
                format!(
                    "{}:{}\t{}\t{},{}\t{slot}",
                    f.func_name.lowercase(),
                    pb_code(built.as_ref().expect("built")).as_str_name(),
                    type_text(f.ret_type.as_ref().expect("typed")),
                    f.collation.coercibility().0,
                    f.collation.repertoire().0,
                )
            }
            other => format!("{other:?}"),
        };
        let want = match cells[4..] {
            ["const", tp, value] => format!("const\t{}\t{value}", types[tp]),
            [sig, tp, collation, slot] => {
                let (mark, rest) = slot.split_at(1);
                let (number, tail) = rest.split_once(':').map_or((rest, None), |(n, t)| (n, Some(t)));
                let tail = tail.map(|tail| format!(":{tail}")).unwrap_or_default();
                format!("{sig}\t{}\t{collation}\t{mark}{}{tail}", types[tp], types[number])
            }
            _ => panic!("row {line}"),
        };
        checked += 1;
        if got != want {
            wrong.push(format!("{} mode {mode} of {label}\n  go   {want}\n  rust {got}", types[target]));
        }
    }
    assert!(checked > 1300, "{checked} rows");
    assert!(wrong.is_empty(), "{} of {checked} differ:\n{}", wrong.len(), wrong.join("\n"));
}
