#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn take_keyword<'a>(sql: &'a str, keyword: &str) -> Option<&'a str> {
    let sql = sql.trim_start();
    let prefix = sql.get(..keyword.len())?;
    if !prefix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = sql.get(keyword.len()..)?;
    (rest.is_empty() || rest.starts_with(char::is_whitespace)).then_some(rest.trim_start())
}

fn standalone_split_table(sql: &str) -> bool {
    let Some(mut rest) = take_keyword(sql, "SPLIT") else {
        return false;
    };
    if let Some(after_region) = take_keyword(rest, "REGION") {
        let Some(after_for) = take_keyword(after_region, "FOR") else {
            return false;
        };
        rest = after_for;
    }
    if let Some(after_partition) = take_keyword(rest, "PARTITION") {
        rest = after_partition;
    }
    take_keyword(rest, "TABLE").is_some()
}

fn after_table_name(sql: &str) -> Option<&str> {
    let mut rest = take_keyword(sql, "ALTER")?;
    rest = take_keyword(rest, "TABLE")?;
    if rest.starts_with('`') {
        let mut index = 1;
        while index < rest.len() {
            let tail = &rest[index..];
            if tail.starts_with("``") {
                index += 2;
            } else if let Some(after_quote) = tail.strip_prefix('`') {
                return Some(after_quote.trim_start());
            } else {
                index += tail.chars().next()?.len_utf8();
            }
        }
        return None;
    }
    let index = rest.find(char::is_whitespace)?;
    Some(rest[index..].trim_start())
}

fn alter_split_region(sql: &str) -> bool {
    let Some(rest) = after_table_name(sql) else {
        return false;
    };
    let Some(rest) = take_keyword(rest, "SPLIT") else {
        return false;
    };
    ["TABLE", "PRIMARY", "INDEX", "REGION", "BY", "BETWEEN"]
        .iter()
        .any(|target| take_keyword(rest, target).is_some())
}

#[test]
fn split_table_and_index_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && (standalone_split_table(&record.input.sql)
                    || alter_split_region(&record.input.sql))
        })
        .collect();
    assert_eq!(selected.len(), 73, "source-backed selector drifted");

    let mut failures = Vec::new();
    for record in selected {
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(statement) => failures.push(format!(
                "{}\n  go: {}\n rust: {}",
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => failures.push(format!("{}\n  parse error: {error:?}", record.input.sql)),
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
