#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn is_alter_user_dual_password(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    upper.starts_with("ALTER USER ")
        && (upper.contains(" RETAIN CURRENT PASSWORD") || upper.contains(" DISCARD OLD PASSWORD"))
}

/// Splits the named-user list on commas outside strings, identifiers, and the
/// `USER()` current-user form. Every selected spec must carry a typed auth or
/// dual-password action; Go also accepts actionless siblings, but retaining
/// them would lose semantics in this deliberately narrow AST slice.
fn all_user_specs_have_typed_action(sql: &str) -> bool {
    let Some(rest) = sql.trim_start().get("alter user".len()..) else {
        return false;
    };
    let mut specs = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    let mut quote = None;
    let mut chars = rest.char_indices().peekable();
    while let Some((index, character)) = chars.next() {
        if let Some(delimiter) = quote {
            if character == '\\' && delimiter != '`' {
                chars.next();
            } else if character == delimiter {
                if delimiter == '`' && chars.peek().is_some_and(|(_, next)| *next == '`') {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match character {
            '\'' | '\"' | '`' => quote = Some(character),
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                specs.push(&rest[start..index]);
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    specs.push(&rest[start..]);
    specs.into_iter().all(|spec| {
        let upper = spec.to_ascii_uppercase();
        upper.contains(" IDENTIFIED ")
            || upper.contains(" RETAIN CURRENT PASSWORD")
            || upper.contains(" DISCARD OLD PASSWORD")
    })
}

/// COMMENT is a statement-level ALTER USER option in the Go AST. It is not
/// modelled by the existing Rust account AST, so retain it for an option slice
/// rather than accept and erase it while adding dual-password grammar.
fn has_unported_comment_option(sql: &str) -> bool {
    sql.to_ascii_uppercase().contains(" COMMENT ")
}

#[test]
fn alter_user_dual_password_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_alter_user_dual_password(&record.input.sql)
                && all_user_specs_have_typed_action(&record.input.sql)
                && !has_unported_comment_option(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 18, "source-backed selector drifted");

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
