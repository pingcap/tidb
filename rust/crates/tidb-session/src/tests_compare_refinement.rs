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

//! `int column <cmp> non-int constant`: the constant is folded against the
//! column's type ONCE, at build time, not re-coerced on every row.
//!
//! # The signature that makes this a semantic gap, not a diagnostics nit
//!
//! `SELECT * FROM <t> WHERE a > '10ab'` over an INT column raised
//! `Truncated incorrect DOUBLE value: '10ab'` ONCE PER SCANNED ROW here --
//! 11 for `t`, 5 for `trange`, 11 for `thash` -- while TiDB raises it TWICE
//! for all three. TiDB's count does not move with the table because the
//! coercion never reaches a row; ours did, because it happened inside the
//! evaluation loop. A warning multiplicity that tracks row count is the
//! observable shadow of a per-row cost TiDB does not pay.
//!
//! # The Go that decides it
//!
//! `compareFunctionClass.refineArgs` (`pkg/expression/builtin_compare.go`
//! :1778) rewrites the arguments before the signature is chosen. Its
//! `int non-constant [cmp] non-int constant` arm (:1811-1813) calls
//! `RefineComparedConstant(ctx, *arg0Type, arg1, c.op)` (:1574); the mirrored
//! arm (:1838-1840) does the same with `symmetricOp[c.op]` when the constant
//! is on the left. `refineArgs` itself is called from
//! `compareFunctionClass.getFunction` (:1984), so EVERY `lt/le/gt/ge/eq/ne/
//! nulleq` built through the function class goes through it.
//!
//! `RefineComparedConstant` first converts the constant to the column's type
//! (:1585-1598). That conversion is warning one. It then compares the
//! converted value against the original (:1600): when they are equal the int
//! is exact and is returned as-is. When they differ -- '10ab' converts to 10
//! but 10 != '10ab' -- the operator decides the rounding direction:
//!
//! ```text
//! case opcode.LT, opcode.GE:   ast.Ceil    builtin_compare.go:1613-1614
//! case opcode.LE, opcode.GT:   ast.Floor   builtin_compare.go:1618-1619
//! ```
//!
//! `a > '10ab'` is GT, so it takes the `Floor` fold, and the fold's own
//! string->double coercion is warning two. `tryToConvertConstantInt`
//! (:1516-1564) then turns the folded constant into the column's int type.
//! Two conversions, both at build time, and the comparison that survives is
//! `gt(a, 10)` -- int to int, so no row ever coerces a string.
//!
//! TiDB's own recording shows exactly that plan
//! (`tests/integrationtest/r/executor/partition/partition_with_expression.result`
//! :1239): `gt(executor__partition__partition_with_expression.trange.a, 10)`.

use super::Session;
use crate::tests_support::row_text;

/// The three tables of `TestDynamicPruneModeWithExpression`
/// (`tests/integrationtest/t/executor/partition/partition_with_expression.test`
/// :137-143), verbatim.
fn partition_session() -> Session {
    let mut session = Session::new();
    for sql in [
        "create table trange(a int, b int) partition by range(a) (partition p0 values less than(3), partition p1 values less than (5), partition p2 values less than(11))",
        "create table thash(a int, b int) partition by hash(a) partitions 4",
        "create table t(a int, b int)",
        "insert into trange values(1, NULL), (1, NULL), (1, 1), (2, 1), (3, 2), (4, 3), (5, 5), (6, 7), (7, 7), (7, 7), (10, NULL), (NULL, NULL), (NULL, 1)",
        "insert into thash values(1, NULL), (1, NULL), (1, 1), (2, 1), (3, 2), (4, 3), (5, 5), (6, 7), (7, 7), (7, 7), (10, NULL), (NULL, NULL), (NULL, 1)",
        "insert into t values(1, NULL), (1, NULL), (1, 1), (2, 1), (3, 2), (4, 3), (5, 5), (6, 7), (7, 7), (7, 7), (10, NULL), (NULL, NULL), (NULL, 1)",
    ] {
        session
            .run(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    session
}

fn warning_texts(session: &Session) -> Vec<String> {
    session
        .warnings()
        .iter()
        .map(|w| format!("{} {}", w.code, w.message))
        .collect()
}

/// The reported unit: the warning count is the SAME for all three tables,
/// because the coercion happens once at build time and never in the scan.
///
/// Asserting table-independence is the point. A per-row coercion cannot
/// produce equal counts over a 13-row heap, a range-partitioned table and a
/// hash-partitioned one; only a build-time fold can.
#[test]
fn int_column_gt_string_constant_warns_twice_regardless_of_table() {
    let mut session = partition_session();
    let mut counts = Vec::new();
    for table in ["t", "trange", "thash"] {
        let sql = format!("SELECT * from {table} where a > '10ab'");
        // TiDB's recording: no row is greater than 10 in any of the three.
        assert_eq!(row_text(session.run(&sql)), Vec::<Vec<String>>::new(), "{sql}");
        let texts = warning_texts(&session);
        assert!(
            texts
                .iter()
                .all(|t| t.starts_with("1292 Truncated incorrect DOUBLE value")),
            "{sql} -> {texts:?}"
        );
        // Both channels, which have been proven independent here before.
        assert_eq!(
            session.wire_warning_count(),
            u16::try_from(texts.len()).unwrap(),
            "wire count disagrees with the buffer for {sql}"
        );
        counts.push((table, texts.len()));
    }
    assert_eq!(
        counts,
        vec![("t", 2), ("trange", 2), ("thash", 2)],
        "TiDB raises the truncation twice per statement for all three tables; a count that \
         moves with the table means the string is being coerced inside the scan"
    );
}

/// The comparison that survives the rewrite is int-to-int. This is the
/// same fact the warning count measures, read off the plan instead of the
/// diagnostic, so a fix that only silenced the warning would not pass both.
#[test]
fn the_refined_comparison_is_int_to_int() {
    let mut session = partition_session();
    let plan = row_text(session.run("explain select * from t where a > '10ab'"))
        .iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("gt(") && plan.contains(", 10)"),
        "the predicate should have been refined to `gt(<col>, 10)`; got:\n{plan}"
    );
    assert!(
        !plan.contains("10ab"),
        "the string constant survived into the executed predicate:\n{plan}"
    );
}
