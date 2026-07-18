#![allow(dead_code, missing_docs)]

use std::collections::BTreeSet;

use difftest::parser_oracle::{shared_golden, GoOutcome};

const EXECUTION_READY_FIXTURES: [(&str, usize); 41] = [
    ("tests/integrationtest/t/bindinfo/bind.test", 143),
    ("tests/integrationtest/t/collation_agg_func.test", 4),
    ("tests/integrationtest/t/ddl/db_partition.test", 1910),
    ("tests/integrationtest/t/ddl/db_partition.test", 1944),
    ("tests/integrationtest/t/ddl/db_partition.test", 1957),
    ("tests/integrationtest/t/executor/aggregate.test", 471),
    ("tests/integrationtest/t/executor/delete.test", 84),
    ("tests/integrationtest/t/executor/delete.test", 88),
    ("tests/integrationtest/t/executor/delete.test", 92),
    ("tests/integrationtest/t/executor/executor.test", 1931),
    ("tests/integrationtest/t/executor/expand.test", 1),
    (
        "tests/integrationtest/t/executor/infoschema_reader.test",
        50,
    ),
    ("tests/integrationtest/t/executor/insert.test", 1149),
    ("tests/integrationtest/t/executor/insert.test", 1716),
    ("tests/integrationtest/t/executor/jointest/join.test", 470),
    ("tests/integrationtest/t/executor/prepared.test", 70),
    ("tests/integrationtest/t/executor/prepared.test", 78),
    ("tests/integrationtest/t/executor/prepared.test", 95),
    ("tests/integrationtest/t/executor/prepared.test", 103),
    ("tests/integrationtest/t/executor/split_table.test", 147),
    ("tests/integrationtest/t/executor/split_table.test", 175),
    ("tests/integrationtest/t/executor/split_table.test", 294),
    ("tests/integrationtest/t/executor/stale_txn.test", 45),
    ("tests/integrationtest/t/executor/update.test", 254),
    ("tests/integrationtest/t/executor/update.test", 557),
    ("tests/integrationtest/t/explain_complex.test", 136),
    ("tests/integrationtest/t/explain_complex.test", 152),
    ("tests/integrationtest/t/explain_complex.test", 167),
    ("tests/integrationtest/t/explain_complex.test", 179),
    ("tests/integrationtest/t/expression/issues.test", 342),
    ("tests/integrationtest/t/expression/issues.test", 665),
    ("tests/integrationtest/t/expression/issues.test", 773),
    (
        "tests/integrationtest/t/planner/core/casetest/integration.test",
        161,
    ),
    (
        "tests/integrationtest/t/planner/core/casetest/integration.test",
        473,
    ),
    ("tests/integrationtest/t/planner/core/indexjoin.test", 205),
    (
        "tests/integrationtest/t/planner/core/integration.test",
        1299,
    ),
    (
        "tests/integrationtest/t/planner/core/issuetest/planner_issue.test",
        217,
    ),
    (
        "tests/integrationtest/t/planner/core/partial_order_topn.test",
        46,
    ),
    ("tests/integrationtest/t/planner/core/plan_cache.test", 3),
    (
        "tests/integrationtest/t/planner/core/rule_constant_propagation.test",
        46,
    ),
    (
        "tests/integrationtest/t/planner/core/rule_constant_propagation.test",
        47,
    ),
];

#[test]
fn create_table_nonreserved_column_names_match_go() {
    let expected: BTreeSet<_> = EXECUTION_READY_FIXTURES.into_iter().collect();
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(selected.len(), 41, "source-backed selector drifted");

    let mut failures = Vec::new();
    for record in selected {
        assert_eq!(record.outcome, GoOutcome::Accepted, "{}", record.input.sql);
        assert_eq!(record.statement_count, 1, "{}", record.input.sql);
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(statement) => failures.push(format!(
                "{}:{}\n  sql: {}\n   go: {}\n rust: {}",
                record.input.path,
                record.input.start_line,
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => failures.push(format!(
                "{}:{}\n  sql: {}\n  parse error: {error:?}",
                record.input.path, record.input.start_line, record.input.sql
            )),
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
