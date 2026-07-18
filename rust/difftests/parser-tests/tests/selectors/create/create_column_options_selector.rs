#![allow(dead_code, missing_docs)]

//! Static-oracle slice owned by Go `HandParser.parseColumnOptions`.
//!
//! The selector deliberately tokenizes around quotes and identifiers: a table
//! called `auto_random_t` or a column called `serial_id` is not evidence for
//! this grammar leaf. It covers the exact source options that gained a typed
//! Rust representation here, while leaving `AUTO_RANDOM_BASE` to the table
//! option owner.

use difftest::parser_oracle::{shared_golden, GoOutcome};

#[derive(Debug, Default, Eq, PartialEq)]
struct Counts {
    serial_matched: usize,
    serial_parse_failure: usize,
    serial_restore_mismatch: usize,
    auto_random_matched: usize,
    auto_random_parse_failure: usize,
    auto_random_restore_mismatch: usize,
}

fn has_unquoted_keyword(sql: &str, keyword: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut quote = None;
    let mut index = 0usize;
    while index < bytes.len() {
        match quote {
            Some(_) if bytes[index] == b'\\' && index + 1 < bytes.len() => index += 2,
            Some(delimiter) if bytes[index] == delimiter => {
                if index + 1 < bytes.len() && bytes[index + 1] == delimiter {
                    index += 2;
                } else {
                    quote = None;
                    index += 1;
                }
            }
            Some(_) => index += 1,
            None if matches!(bytes[index], b'\'' | b'"' | b'`') => {
                quote = Some(bytes[index]);
                index += 1;
            }
            None if bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                if sql[start..index].eq_ignore_ascii_case(keyword) {
                    return true;
                }
            }
            None => index += 1,
        }
    }
    false
}

#[test]
fn column_option_static_go_rows_have_reviewed_outcomes() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && (has_unquoted_keyword(&record.input.sql, "SERIAL")
                    || has_unquoted_keyword(&record.input.sql, "AUTO_RANDOM"))
        })
        .collect();
    assert_eq!(selected.len(), 53, "source-backed selector drifted");

    let mut counts = Counts::default();
    for record in selected {
        let target = if has_unquoted_keyword(&record.input.sql, "SERIAL") {
            (
                &mut counts.serial_matched,
                &mut counts.serial_parse_failure,
                &mut counts.serial_restore_mismatch,
            )
        } else {
            (
                &mut counts.auto_random_matched,
                &mut counts.auto_random_parse_failure,
                &mut counts.auto_random_restore_mismatch,
            )
        };
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                *target.0 += 1
            }
            Ok(_) => *target.2 += 1,
            Err(_) => *target.1 += 1,
        }
    }
    assert_eq!(
        counts,
        Counts {
            serial_matched: 4,
            serial_parse_failure: 0,
            serial_restore_mismatch: 0,
            auto_random_matched: 49,
            auto_random_parse_failure: 0,
            auto_random_restore_mismatch: 0,
        },
        "column-option source slice changed; inspect every static Go outcome before updating this snapshot"
    );
}
