#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome};

fn starts_action(rest: &str, action: &str) -> bool {
    let rest = rest.trim_start();
    rest.get(..action.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(action))
        && rest
            .get(action.len()..)
            .unwrap_or_default()
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_whitespace() || character == '(')
}

fn after_table_name(sql: &str) -> Option<&str> {
    let sql = sql.trim_start();
    let prefix = "alter table";
    if !sql
        .get(..prefix.len())
        .is_some_and(|value| value.eq_ignore_ascii_case(prefix))
    {
        return None;
    }
    let mut rest = sql.get(prefix.len()..)?;
    if !rest.starts_with(char::is_whitespace) {
        return None;
    }
    rest = rest.trim_start();
    if rest.starts_with('`') {
        let mut index = 1;
        while index < rest.len() {
            let tail = &rest[index..];
            if tail.starts_with("``") {
                index += 2;
            } else if let Some(after_quote) = tail.strip_prefix('`') {
                rest = after_quote;
                break;
            } else {
                index += tail.chars().next()?.len_utf8();
            }
        }
    } else {
        let index = rest.find(char::is_whitespace)?;
        rest = &rest[index..];
    }
    Some(rest.trim_start())
}

/// True when the action contains no comma outside quoted text or parentheses.
fn has_one_alter_action(sql: &str) -> bool {
    let Some(rest) = after_table_name(sql) else {
        return false;
    };
    let mut depth = 0usize;
    let mut quote = None;
    let mut chars = rest.chars().peekable();
    while let Some(character) = chars.next() {
        if let Some(delimiter) = quote {
            if character == '\\' && delimiter != '`' {
                chars.next();
            } else if character == delimiter {
                if delimiter == '`' && chars.peek() == Some(&'`') {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match character {
            '\'' | '"' | '`' => quote = Some(character),
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => return false,
            _ => {}
        }
    }
    true
}

fn is_one_action_ordinary_add_index(sql: &str) -> bool {
    let Some(action) = after_table_name(sql) else {
        return false;
    };
    (starts_action(action, "add index") || starts_action(action, "add key"))
        && has_one_alter_action(sql)
}

#[test]
fn alter_add_index_lexical_one_statement_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_one_action_ordinary_add_index(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 182, "source-backed selector drifted");

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
