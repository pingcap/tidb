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

//! Behavior contracts for the allocation and repeated-scan optimizations.
//! Timing comparisons live in the benchmark harness, not flaky unit assertions.

use super::*;

#[test]
fn advancing_preserves_tokens_for_lookback_and_eof() {
    let mut parser = Parser::new("SELECT `col`, 'value' FROM t WHERE id = 1");
    let tokens = parser.toks.clone();
    for token in &tokens {
        assert_eq!(parser.peek(), token);
        parser.advance();
    }
    parser.advance();
    assert!(parser.at_eof());
    assert_eq!(parser.toks, tokens);
    parser.pos = 0;
    assert_eq!(parser.bump(), tokens[0]);
    assert_eq!(parser.peek(), &tokens[1]);
}

#[test]
fn batch_source_text_preserves_comment_and_literal_boundaries() {
    let sql = ";;  /*! SELECT 1; SELECT 2 */;  SELECT '/*!';   SELECT 4; SELECT '*/';  SELECT 6";
    let statements = parse_multi(sql).unwrap();
    let expected = [
        "/*! SELECT 1;",
        " SELECT 2 */;",
        "SELECT '/*!';",
        "   SELECT 4;",
        " SELECT '*/';",
        "SELECT 6",
    ];
    assert_eq!(statements.len(), expected.len());
    for (statement, text) in statements.iter().zip(expected) {
        assert_eq!(statement.original_text(), text.as_bytes());
    }
    assert_eq!(statements[0].restore(), "SELECT 1");
    assert_eq!(statements[1].restore(), "SELECT 2");
    assert_eq!(statements[5].restore(), "SELECT 6");
}

#[test]
fn batch_depth_threshold_preserves_small_and_large_statements() {
    for count in [1_365, 1_366, 1_400] {
        let sql = "SELECT 1;".repeat(count);
        let statements = parse_multi(&sql).unwrap();
        assert_eq!(statements.len(), count);
        for statement in &statements {
            assert_eq!(statement.restore(), "SELECT 1");
            assert_eq!(statement.original_text(), b"SELECT 1;");
        }
    }

    let wide = format!("SELECT {}", vec!["1"; 3_000].join(","));
    let expected = parse(&wide).unwrap().restore();
    let sql = format!("SELECT 0; {wide}; SELECT 2");
    let statements = parse_multi(&sql).unwrap();
    assert_eq!(statements.len(), 3);
    assert_eq!(statements[0].restore(), "SELECT 0");
    assert_eq!(statements[1].restore(), expected);
    assert_eq!(statements[2].restore(), "SELECT 2");

    let invalid = format!("{}SELECT (", "SELECT 1;".repeat(1_366));
    assert!(parse_multi(&invalid).is_err());
}

#[test]
fn nested_lookahead_preserves_queries_joins_and_errors() {
    for depth in [1, 2, 32, 128, 512] {
        let query = format!("{}SELECT 1{}", "(".repeat(depth), ")".repeat(depth));
        let statement = parse(&query).unwrap();
        assert!(statement.try_restore().unwrap().contains("SELECT 1"));

        let joins = format!("SELECT * FROM {}t{}", "(".repeat(depth), ")".repeat(depth));
        assert_eq!(r(&joins), "SELECT * FROM `t`");
        assert!(parse(&joins[..joins.len() - 1]).is_err());
    }
    for sql in [
        "SELECT * FROM ((SELECT 1) a JOIN t ON a.x=t.x)",
        "SELECT * FROM ((SELECT '(') a JOIN t ON a.x=t.x)",
        "SELECT * FROM ((SELECT ')') a JOIN t ON a.x=t.x)",
        "SELECT * FROM (((SELECT 1))) a, (((SELECT 2))) b",
    ] {
        let stmt = parse(sql).unwrap();
        assert_eq!(parse(&stmt.restore()).unwrap().restore(), stmt.restore());
    }
}

#[test]
fn fallible_restore_checks_nested_nodes_without_changing_error_boundary() {
    let error = "Incorrect parameter count in the call to native function 'json_memberof'";
    for sql in [
        "SELECT (((json_memberof())))",
        "SELECT 1 + json_memberof(1)",
        "SELECT * FROM (SELECT json_memberof()) AS t",
        "SELECT CASE WHEN 1 THEN json_memberof() ELSE 0 END",
    ] {
        let statement = parse(sql).expect("parse accepts the source restore error");
        assert_eq!(statement.validate_restore().unwrap_err(), error);
        assert_eq!(statement.try_restore().unwrap_err(), error);
        assert_eq!(statement.try_restore_bytes().unwrap_err(), error);
    }
    let sql = format!("SELECT {}1{}", "(".repeat(512), ")".repeat(512));
    let statement = parse(&sql).unwrap();
    assert_eq!(statement.try_restore().unwrap(), statement.restore());
    assert_eq!(
        statement.try_restore_bytes().unwrap(),
        statement.restore_bytes()
    );
    assert_eq!(
        parse("PREPARE stmt FROM ''")
            .unwrap()
            .try_restore()
            .unwrap_err(),
        "An error occurred while restore PrepareStmt"
    );
}
