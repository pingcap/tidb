#![allow(dead_code, missing_docs)]

use std::collections::BTreeSet;

use difftest::parser_oracle::{read_golden, repo_root, GoOutcome};

/// Exact Go integration rows for identifier-only dynamic privilege revocation.
/// The neighboring `BOGUS` row remains an explicit negative queue case.
const REVOKE_DYNAMIC_PRIVILEGE_ROWS: [(&str, usize); 8] = [
    ("tests/integrationtest/t/executor/revoke.test", 38),
    ("tests/integrationtest/t/executor/revoke.test", 29),
    ("tests/integrationtest/t/executor/revoke.test", 34),
    ("tests/integrationtest/t/executor/revoke.test", 44),
    ("tests/integrationtest/t/executor/revoke.test", 49),
    ("tests/integrationtest/t/executor/revoke.test", 54),
    ("tests/integrationtest/t/privilege/privileges.test", 124),
    ("tests/integrationtest/t/privilege/privileges.test", 125),
];

#[test]
fn revoke_dynamic_privilege_rows_match_go() {
    let expected: BTreeSet<_> = REVOKE_DYNAMIC_PRIVILEGE_ROWS.into_iter().collect();
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
