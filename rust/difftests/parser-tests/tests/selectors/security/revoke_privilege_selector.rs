#![allow(dead_code, missing_docs)]

use std::collections::BTreeSet;

use difftest::parser_oracle::{read_golden, repo_root, GoOutcome};

/// Exact static instances of Go's standard `REVOKE PrivElemList ON ... FROM`
/// branch. Dynamic privileges and role/special no-`ON` forms are deliberately
/// omitted because they take different Go conversion paths.
const STANDARD_REVOKE_FIXTURES: [(&str, usize); 32] = [
    ("tests/integrationtest/t/executor/executor.test", 2674),
    ("tests/integrationtest/t/executor/explain.test", 139),
    ("tests/integrationtest/t/executor/grant.test", 61),
    ("tests/integrationtest/t/executor/grant.test", 68),
    ("tests/integrationtest/t/executor/grant.test", 75),
    ("tests/integrationtest/t/executor/grant.test", 92),
    ("tests/integrationtest/t/executor/revoke.test", 8),
    ("tests/integrationtest/t/executor/revoke.test", 17),
    ("tests/integrationtest/t/executor/revoke.test", 74),
    ("tests/integrationtest/t/executor/revoke.test", 88),
    ("tests/integrationtest/t/executor/revoke.test", 89),
    ("tests/integrationtest/t/executor/revoke.test", 90),
    ("tests/integrationtest/t/executor/revoke.test", 99),
    ("tests/integrationtest/t/executor/revoke.test", 103),
    ("tests/integrationtest/t/executor/revoke.test", 107),
    ("tests/integrationtest/t/executor/simple.test", 24),
    ("tests/integrationtest/t/executor/simple.test", 323),
    ("tests/integrationtest/t/executor/simple.test", 352),
    ("tests/integrationtest/t/executor/simple.test", 473),
    (
        "tests/integrationtest/t/planner/core/integration.test",
        2040,
    ),
    ("tests/integrationtest/t/privilege/privileges.test", 14),
    ("tests/integrationtest/t/privilege/privileges.test", 139),
    ("tests/integrationtest/t/privilege/privileges.test", 140),
    ("tests/integrationtest/t/privilege/privileges.test", 199),
    ("tests/integrationtest/t/privilege/privileges.test", 205),
    ("tests/integrationtest/t/privilege/privileges.test", 207),
    ("tests/integrationtest/t/privilege/privileges.test", 208),
    ("tests/integrationtest/t/privilege/privileges.test", 209),
    ("tests/integrationtest/t/privilege/privileges.test", 297),
    ("tests/integrationtest/t/privilege/privileges.test", 303),
    ("tests/integrationtest/t/privilege/privileges.test", 304),
    ("tests/integrationtest/t/privilege/privileges.test", 312),
];

#[test]
fn revoke_standard_privileges_match_go() {
    let expected: BTreeSet<_> = STANDARD_REVOKE_FIXTURES.into_iter().collect();
    let records = read_golden(&repo_root()).expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(selected.len(), expected.len(), "source fixture drifted");

    for record in selected {
        assert_eq!(record.outcome, GoOutcome::Accepted, "{}", record.input.sql);
        assert_eq!(record.statement_count, 1, "{}", record.input.sql);
        let statement = tidb_parser::parse(&record.input.sql)
            .unwrap_or_else(|error| panic!("{}: {error:?}", record.input.sql));
        assert_eq!(
            statement.restore().as_bytes(),
            record.restores[0].as_slice(),
            "{}",
            record.input.sql
        );
    }
}
