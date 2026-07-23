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

//! Complete source cases from `pkg/parser/hintparser_test.go::TestParseHint`.

use tidb_ast::{Hint, HintKind, HintTable, LeadingElement};
use tidb_parser::{parse_hint, parse_with_warnings};

fn table(table: &HintTable) -> String {
    let mut result = String::new();
    if let Some(database) = &table.db_name {
        result.push_str(database);
        result.push('.');
    }
    result.push_str(&table.name);
    if let Some(qb_name) = &table.qb_name {
        result.push('@');
        result.push_str(qb_name);
    }
    if !table.partitions.is_empty() {
        result.push_str(" partition(");
        result.push_str(&table.partitions.join(","));
        result.push(')');
    }
    result
}

fn leading(element: &LeadingElement) -> String {
    match element {
        LeadingElement::Table(value) => table(value),
        LeadingElement::Group(values) => {
            format!(
                "({})",
                values.iter().map(leading).collect::<Vec<_>>().join(",")
            )
        }
    }
}

fn signature(hint: &Hint) -> String {
    let payload = match &hint.kind {
        HintKind::Nullary { qb_name } => format!("nullary|{}", qb_name.as_deref().unwrap_or("")),
        HintKind::Tables { qb_name, tables } => format!(
            "tables|{}|{}",
            qb_name.as_deref().unwrap_or(""),
            tables.iter().map(table).collect::<Vec<_>>().join(",")
        ),
        HintKind::Leading { qb_name, elements } => format!(
            "leading|{}|{}",
            qb_name.as_deref().unwrap_or(""),
            elements.iter().map(leading).collect::<Vec<_>>().join(",")
        ),
        HintKind::Index {
            qb_name,
            table: hint_table,
            indexes,
        } => format!(
            "index|{}|{}|{}",
            qb_name.as_deref().unwrap_or(""),
            table(hint_table),
            indexes.join(",")
        ),
        HintKind::SetVar { var_name, value } => format!("set|{var_name}|{value}"),
        HintKind::Bool { qb_name, value } => {
            format!("bool|{}|{value}", qb_name.as_deref().unwrap_or(""))
        }
        HintKind::Name { qb_name, name } => {
            format!("name|{}|{name}", qb_name.as_deref().unwrap_or(""))
        }
        HintKind::Keyword { qb_name, value } => {
            format!("keyword|{}|{value}", qb_name.as_deref().unwrap_or(""))
        }
        HintKind::MemoryQuota { qb_name, bytes } => {
            format!("memory|{}|{bytes}", qb_name.as_deref().unwrap_or(""))
        }
        HintKind::TimeRange { from, to } => format!("time|{from}|{to}"),
        HintKind::Number { qb_name, value } => {
            format!("number|{}|{value}", qb_name.as_deref().unwrap_or(""))
        }
        HintKind::QbName { qb_name, views } => format!(
            "qb|{qb_name}|{}",
            views.iter().map(table).collect::<Vec<_>>().join(".")
        ),
        HintKind::ReadFromStorage { qb_name, groups } => format!(
            "storage|{}|{}",
            qb_name.as_deref().unwrap_or(""),
            groups
                .iter()
                .map(|(store, tables)| format!(
                    "{store}[{}]",
                    tables.iter().map(table).collect::<Vec<_>>().join(",")
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
    };
    format!("{}|{payload}", hint.name)
}

fn assert_hints(input: &str, ansi_quotes: bool, expected: &[&str]) {
    let result = parse_hint(&format!("/*+{input}*/"), ansi_quotes, 1);
    assert!(
        result.diagnostics.is_empty(),
        "{input}: diagnostics={:?}, hints={:?}",
        result.diagnostics,
        result.hints.iter().map(signature).collect::<Vec<_>>()
    );
    assert_eq!(
        result.hints.iter().map(signature).collect::<Vec<_>>(),
        expected,
        "{input}"
    );
}

fn assert_errors(input: &str, expected: &[&str]) {
    let result = parse_hint(&format!("/*+{input}*/"), false, 1);
    assert!(result.hints.is_empty(), "{input}: {:?}", result.hints);
    assert_eq!(result.diagnostics.len(), expected.len(), "{input}");
    for (actual, expected) in result.diagnostics.iter().zip(expected) {
        assert!(
            actual.message.contains(expected),
            "{input}: {:?}",
            result.diagnostics
        );
    }
}

#[test]
fn test_parse_hint() {
    assert_errors("", &["Optimizer hint syntax error at line 1 "]);
    assert_hints(
        "MEMORY_QUOTA(8 MB) MEMORY_QUOTA(6 GB)",
        false,
        &[
            "MEMORY_QUOTA|memory||8388608",
            "MEMORY_QUOTA|memory||6442450944",
        ],
    );
    assert_hints(
        "QB_NAME(qb1) QB_NAME(`qb2`), QB_NAME(TRUE) QB_NAME(\"ANSI quoted\") QB_NAME(_utf8), QB_NAME(0b10) QB_NAME(0x1a)",
        true,
        &[
            "QB_NAME|qb|qb1|",
            "QB_NAME|qb|qb2|",
            "QB_NAME|qb|TRUE|",
            "QB_NAME|qb|ANSI quoted|",
            "QB_NAME|qb|_utf8|",
            "QB_NAME|qb|0b10|",
            "QB_NAME|qb|0x1a|",
        ],
    );
    assert_errors("QB_NAME(1)", &["Optimizer hint syntax error at line 1 "]);
    assert_errors(
        "QB_NAME(1.5)",
        &[
            "Cannot use decimal number",
            "Optimizer hint syntax error at line 1 ",
        ],
    );
    assert_errors(
        "QB_NAME('string literal')",
        &["Optimizer hint syntax error at line 1 "],
    );
    assert_errors(
        "QB_NAME(many identifiers)",
        &["Optimizer hint syntax error at line 1 "],
    );
    assert_errors("QB_NAME(@qb1)", &["Optimizer hint syntax error at line 1 "]);
    assert_errors(
        "QB_NAME(b'10')",
        &[
            "Cannot use bit-value literal",
            "Optimizer hint syntax error at line 1 ",
        ],
    );
    assert_errors(
        "QB_NAME(x'1a')",
        &[
            "Cannot use hexadecimal literal",
            "Optimizer hint syntax error at line 1 ",
        ],
    );
    assert_errors(
        "JOIN_FIXED_ORDER() BKA()",
        &[
            "Optimizer hint JOIN_FIXED_ORDER is not supported",
            "Optimizer hint BKA is not supported",
        ],
    );
    assert_hints(
        "HASH_JOIN() TIDB_HJ(@qb1) INL_JOIN(x, `y y`.z) MERGE_JOIN(w@`First QB`)",
        false,
        &[
            "HASH_JOIN|tables||",
            "TIDB_HJ|tables|qb1|",
            "INL_JOIN|tables||x,y y.z",
            "MERGE_JOIN|tables||w@First QB",
        ],
    );
    assert_hints(
        "USE_INDEX_MERGE(@qb1 tbl1 x, y, z) IGNORE_INDEX(tbl2@qb2) USE_INDEX(tbl3 PRIMARY) FORCE_INDEX(tbl4@qb3 c1) INDEX_LOOKUP_PUSHDOWN(tbl5@qb6 c3)",
        false,
        &[
            "USE_INDEX_MERGE|index|qb1|tbl1|x,y,z",
            "IGNORE_INDEX|index||tbl2@qb2|",
            "USE_INDEX|index||tbl3|PRIMARY",
            "FORCE_INDEX|index||tbl4@qb3|c1",
            "INDEX_LOOKUP_PUSHDOWN|index||tbl5@qb6|c3",
        ],
    );
    assert_hints(
        "USE_INDEX(@qb1 tbl1 partition(p0) x) USE_INDEX_MERGE(@qb2 tbl2@qb2 partition(p0, p1) x, y, z)",
        false,
        &[
            "USE_INDEX|index|qb1|tbl1 partition(p0)|x",
            "USE_INDEX_MERGE|index|qb2|tbl2@qb2 partition(p0,p1)|x,y,z",
        ],
    );
    assert_hints(
        r#"SET_VAR(sbs = 16M) SET_VAR(fkc=OFF) SET_VAR(os="mcb=off") set_var(abc=1) set_var(os2='mcb2=off') set_var(sel=0.3) set_var(sel_plus=+0.3) set_var(sel_minus=-0.3)"#,
        false,
        &[
            "SET_VAR|set|sbs|16M",
            "SET_VAR|set|fkc|OFF",
            "SET_VAR|set|os|mcb=off",
            "SET_VAR|set|abc|1",
            "SET_VAR|set|os2|mcb2=off",
            "SET_VAR|set|sel|0.3",
            "SET_VAR|set|sel_plus|0.3",
            "SET_VAR|set|sel_minus|-0.3",
        ],
    );
    assert_hints(
        "USE_TOJA(TRUE) IGNORE_PLAN_CACHE() USE_CASCADES(TRUE) QUERY_TYPE(@qb1 OLAP) QUERY_TYPE(OLTP) NO_INDEX_MERGE() RESOURCE_GROUP(rg1)",
        false,
        &[
            "USE_TOJA|bool||true",
            "IGNORE_PLAN_CACHE|nullary|",
            "USE_CASCADES|bool||true",
            "QUERY_TYPE|keyword|qb1|OLAP",
            "QUERY_TYPE|keyword||OLTP",
            "NO_INDEX_MERGE|nullary|",
            "RESOURCE_GROUP|name||rg1",
        ],
    );
    assert_hints(
        "READ_FROM_STORAGE(@foo TIKV[a, b], TIFLASH[c, d]) HASH_AGG() SEMI_JOIN_REWRITE() READ_FROM_STORAGE(TIKV[e])",
        false,
        &[
            "READ_FROM_STORAGE|storage|foo|TIKV[a,b]",
            "READ_FROM_STORAGE|storage|foo|TIFLASH[c,d]",
            "HASH_AGG|nullary|",
            "SEMI_JOIN_REWRITE|nullary|",
            "READ_FROM_STORAGE|storage||TIKV[e]",
        ],
    );
    assert_hints(
        "WRITE_SLOW_LOG, WRITE_SLOW_LOG",
        false,
        &["WRITE_SLOW_LOG|nullary|", "WRITE_SLOW_LOG|nullary|"],
    );
    assert_errors(
        "WRITE_SLOW_LOG()",
        &["Optimizer hint syntax error at line 1 "],
    );
    assert_errors(
        "unknown_hint()",
        &["Optimizer hint syntax error at line 1 "],
    );
    assert_hints(
        "set_var(timestamp = 1.5)",
        false,
        &["SET_VAR|set|timestamp|1.5"],
    );
    assert_errors(
        "set_var(timestamp = _utf8mb4'1234')",
        &["Optimizer hint syntax error at line 1 "],
    );
    assert_errors(
        "set_var(timestamp = 9999999999999999999999999999999999999)",
        &[
            "integer value is out of range",
            "Optimizer hint syntax error at line 1 ",
        ],
    );
    assert_errors(
        "time_range('2020-02-20 12:12:12',456)",
        &["Optimizer hint syntax error at line 1 "],
    );
    assert_errors(
        "time_range(456,'2020-02-20 12:12:12')",
        &["Optimizer hint syntax error at line 1 "],
    );
    assert_hints(
        "TIME_RANGE('2020-02-20 12:12:12','2020-02-20 13:12:12')",
        false,
        &["TIME_RANGE|time|2020-02-20 12:12:12|2020-02-20 13:12:12"],
    );
    assert_hints(
        "LEADING(a,(b,(c,d)))",
        false,
        &["LEADING|leading||a,(b,(c,d))"],
    );
    assert_hints("LEADING(a,b,c)", false, &["LEADING|leading||a,b,c"]);
    assert_hints(
        "LEADING((a,b),(c,d))",
        false,
        &["LEADING|leading||(a,b),(c,d)"],
    );
    assert_hints("LEADING(x,(y,z),w)", false, &["LEADING|leading||x,(y,z),w"]);
}

#[test]
fn test_hint_error() {
    let parsed = parse_with_warnings(
        "select /*+ tidb_unknown(T1,t2) */ c1, c2 from t1, t2 where t1.c1 = t2.c1",
    )
    .unwrap();
    assert_eq!(parsed.statement.restore(), "SELECT `c1`,`c2` FROM (`t1`) JOIN `t2` WHERE `t1`.`c1`=`t2`.`c1`");
    assert_eq!(
        parsed.warnings.iter().map(|warning| warning.message.as_str()).collect::<Vec<_>>(),
        ["[parser:8061]Optimizer hint tidb_unknown is not supported by TiDB and is ignored"]
    );

    let parsed = parse_with_warnings(
        "select /*+ TIDB_INLJ(t1, T2) tidb_unknown(T1,t2, 1) */ c1, c2 from t1, t2 where t1.c1 = t2.c1",
    )
    .unwrap();
    assert!(parsed.statement.restore().contains("TIDB_INLJ"));
    assert_eq!(parsed.warnings.len(), 1);

    let parsed = parse_with_warnings(
        "select c1, c2 from /*+ tidb_unknow(T1,t2) */ t1, t2 where t1.c1 = t2.c1",
    )
    .unwrap();
    assert_eq!(
        parsed.statement.restore(),
        "SELECT `c1`,`c2` FROM (`t1`) JOIN `t2` WHERE `t1`.`c1`=`t2`.`c1`"
    );

    let parsed = parse_with_warnings("insert into t select /*+ memory_quota(1 MB) */ * from t;")
        .unwrap();
    assert_eq!(parsed.statement.restore().matches("MEMORY_QUOTA").count(), 1);
    assert!(parsed.warnings.is_empty());

    let parsed = parse_with_warnings("insert /*+ memory_quota(1 MB) */ into t select * from t;")
        .unwrap();
    assert_eq!(parsed.statement.restore().matches("MEMORY_QUOTA").count(), 1);
    assert!(parsed.warnings.is_empty());

    let parsed =
        parse_with_warnings("SELECT id FROM tbl WHERE id = 0 FOR UPDATE /*+ xyz */").unwrap();
    assert_eq!(parsed.warnings.len(), 1);
    assert!(parsed.warnings[0].message.ends_with("near '/*+' at line 1"));

    let parsed = parse_with_warnings(
        "create global binding for select /*+ max_execution_time(1) */ 1 using select /*+ max_execution_time(1) */ 1;\n",
    )
    .unwrap();
    assert!(parsed.warnings.is_empty());
}
