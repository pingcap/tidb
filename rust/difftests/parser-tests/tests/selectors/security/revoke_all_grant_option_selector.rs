#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{read_golden, repo_root, GoOutcome};

/// The checked integration instance of Go's special no-`ON` REVOKE form.
const REVOKE_ALL_GRANT_OPTION_ROWS: [(&str, usize); 1] =
    [("tests/integrationtest/t/privilege/privileges.test", 320)];

#[test]
fn revoke_all_grant_option_rows_match_go() {
    let expected = REVOKE_ALL_GRANT_OPTION_ROWS
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
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
