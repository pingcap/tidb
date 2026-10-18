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

//! Every served `information_schema` table declares exactly the columns Go's
//! `pkg/infoschema/tables.go` registers for it in `tableNameToColumns`: the
//! same names in the same order, with the same type, size, decimal, flags,
//! default and comment. The test reads this branch's Go source, so a column
//! ported from anywhere else fails here.

use std::collections::HashMap;
use std::path::Path;

use super::{InfoColumn, SERVED_TABLES, UNSPECIFIED_LENGTH};
use tidb_datatype::FieldTypeCode;

fn go_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

/// The string and integer constants of the Go packages `tables.go` names
/// its columns and sizes with.
fn go_constants() -> HashMap<String, String> {
    let mut constants = HashMap::new();
    let packages = [
        "pkg/infoschema",
        "pkg/session/txninfo",
        "pkg/util/deadlockhistory",
        "pkg/sessionctx/variable",
        "pkg/sessionctx/vardef",
        "pkg/util/stmtsummary",
        "pkg/ddl/resourcegroup",
        "pkg/util/execdetails",
    ];
    for package in packages {
        let mut files = std::fs::read_dir(go_root().join(package))
            .unwrap_or_else(|error| panic!("read {package}: {error}"))
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension().is_some_and(|ext| ext == "go")
                    && !path.to_string_lossy().ends_with("_test.go")
            })
            .collect::<Vec<_>>();
        files.sort();
        for file in files {
            let text = std::fs::read_to_string(&file).unwrap();
            for line in text.lines() {
                let line = line.trim().trim_start_matches("const ").trim();
                let Some((name, value)) = line.split_once('=') else {
                    continue;
                };
                let (name, value) = (name.trim(), value.trim());
                if name.is_empty()
                    || !name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                {
                    continue;
                }
                let value = if let Some(quoted) = value.strip_prefix('"') {
                    match quoted.split_once('"') {
                        Some((text, rest))
                            if rest.trim().is_empty() || rest.trim().starts_with("//") =>
                        {
                            text.to_owned()
                        }
                        _ => continue,
                    }
                } else if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
                    value.to_owned()
                } else {
                    continue;
                };
                // A qualified name (`execdetails.CommitTimeStr`) is that
                // package's constant, whatever another package calls the
                // same identifier.
                let package_name = package.rsplit('/').next().unwrap_or(package);
                constants
                    .entry(format!("{package_name}.{name}"))
                    .or_insert_with(|| value.clone());
                constants.entry(name.to_owned()).or_insert(value);
            }
        }
    }
    constants
}

/// One Go `columnInfo` literal, its fields as written.
type GoColumn = HashMap<String, String>;

/// Splits a Go literal's fields at its top-level commas.
fn split_fields(body: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let (mut depth, mut quoted, mut current) = (0_i32, false, String::new());
    let mut previous = ' ';
    for character in body.chars() {
        if character == '"' && previous != '\\' {
            quoted = !quoted;
        }
        if !quoted {
            match character {
                '(' | '{' | '[' => depth += 1,
                ')' | '}' | ']' => depth -= 1,
                ',' if depth == 0 => {
                    fields.push(std::mem::take(&mut current));
                    previous = character;
                    continue;
                }
                _ => {}
            }
        }
        current.push(character);
        previous = character;
    }
    if !current.trim().is_empty() {
        fields.push(current);
    }
    fields
}

/// Go's `var xxx = []columnInfo{...}` blocks, by variable name.
fn go_column_vars(source: &str) -> HashMap<String, Vec<GoColumn>> {
    let mut vars = HashMap::new();
    let mut rest = source;
    while let Some(start) = rest.find("\nvar ") {
        rest = &rest[start + 5..];
        let Some((name, after)) = rest.split_once(" = []columnInfo{") else {
            continue;
        };
        if name.contains(char::is_whitespace) {
            continue;
        }
        let body = &after[..after.find("\n}").expect("a columnInfo block ends")];
        let mut columns = Vec::new();
        for line in body.lines() {
            let line = line.trim();
            let Some(inner) = line.strip_prefix('{') else {
                continue;
            };
            let inner = &inner[..inner.rfind('}').expect("a one-line columnInfo literal")];
            let column = split_fields(inner)
                .into_iter()
                .filter_map(|field| {
                    let (key, value) = field.split_once(':')?;
                    Some((key.trim().to_owned(), value.trim().to_owned()))
                })
                .collect::<GoColumn>();
            columns.push(column);
        }
        vars.insert(name.to_owned(), columns);
    }
    vars
}

/// Go's `tableNameToColumns`: table name (upper-cased) to column variable.
fn go_table_columns(source: &str, constants: &HashMap<String, String>) -> HashMap<String, String> {
    let start = source
        .find("var tableNameToColumns = map[string][]columnInfo{")
        .expect("tableNameToColumns");
    let body = &source[start..];
    let body = &body[..body.find("\n}\n").unwrap()];
    body.lines()
        .skip(1)
        .filter_map(|line| {
            let (table, var) = line.trim().split_once(':')?;
            let table = constants.get(table.trim())?;
            Some((
                table.to_ascii_uppercase(),
                var.trim().trim_end_matches(',').to_owned(),
            ))
        })
        .collect()
}

fn resolve<'a>(expression: &'a str, constants: &'a HashMap<String, String>) -> &'a str {
    let expression = expression.trim();
    if let Some(text) = expression.strip_prefix('"') {
        return text.strip_suffix('"').unwrap_or(text);
    }
    if let Some(text) = constants.get(expression) {
        return text;
    }
    let key = expression.rsplit('.').next().unwrap_or(expression);
    constants.get(key).map_or(expression, String::as_str)
}

fn go_type(expression: &str) -> FieldTypeCode {
    match expression.trim_start_matches("mysql.") {
        "TypeBlob" => FieldTypeCode::Blob,
        "TypeDatetime" => FieldTypeCode::Datetime,
        "TypeDouble" => FieldTypeCode::Double,
        "TypeEnum" => FieldTypeCode::Enum,
        "TypeJSON" => FieldTypeCode::Json,
        "TypeLong" => FieldTypeCode::Long,
        "TypeLongBlob" => FieldTypeCode::LongBlob,
        "TypeLonglong" => FieldTypeCode::LongLong,
        "TypeMediumBlob" => FieldTypeCode::MediumBlob,
        "TypeNewDecimal" => FieldTypeCode::NewDecimal,
        "TypeShort" => FieldTypeCode::Short,
        "TypeTimestamp" => FieldTypeCode::Timestamp,
        "TypeTiny" => FieldTypeCode::Tiny,
        "TypeVarchar" => FieldTypeCode::Varchar,
        other => panic!("unmapped Go column type {other}"),
    }
}

fn go_size(expression: Option<&String>, constants: &HashMap<String, String>) -> i64 {
    let Some(expression) = expression else {
        return 0;
    };
    let key = expression.rsplit('.').next().unwrap();
    match key {
        "UnspecifiedLength" => UNSPECIFIED_LENGTH,
        _ => resolve(expression, constants)
            .parse()
            .unwrap_or_else(|_| panic!("unresolved Go column size {expression}")),
    }
}

fn go_flag(expression: Option<&String>) -> u32 {
    expression.map_or(0, |expression| {
        expression
            .split('|')
            .map(|flag| match flag.trim().trim_start_matches("mysql.") {
                "NotNullFlag" => 1,
                "PriKeyFlag" => 2,
                "UniqueKeyFlag" => 4,
                "MultipleKeyFlag" => 8,
                "BlobFlag" => 16,
                "UnsignedFlag" => 32,
                "BinaryFlag" => 128,
                other => panic!("unmapped Go column flag {other}"),
            })
            .fold(0, |flags, flag| flags | flag)
    })
}

fn go_default(expression: Option<&String>) -> Option<String> {
    let expression = expression?.trim();
    Some(match expression {
        "mysql.DefaultCollationName" => tidb_mysql::charset::DefaultCollationName.to_owned(),
        _ => expression.trim_matches('"').to_owned(),
    })
}

#[test]
fn served_information_schema_columns_are_go_tables_go() {
    let source = std::fs::read_to_string(go_root().join("pkg/infoschema/tables.go"))
        .expect("read pkg/infoschema/tables.go");
    let constants = go_constants();
    let vars = go_column_vars(&source);
    let tables = go_table_columns(&source, &constants);
    let mut mismatches = Vec::new();
    for (table, columns) in SERVED_TABLES {
        let var = tables
            .get(*table)
            .unwrap_or_else(|| panic!("{table} is not in Go's tableNameToColumns"));
        let go_columns = &vars[var];
        let go_names = go_columns
            .iter()
            .map(|column| resolve(&column["name"], &constants).to_owned())
            .collect::<Vec<_>>();
        let names = columns.iter().map(|column| column.name).collect::<Vec<_>>();
        if go_names != names {
            mismatches.push(format!("{table}: columns {names:?}, Go {go_names:?}"));
            continue;
        }
        for (column, go) in columns.iter().zip(go_columns) {
            let InfoColumn {
                name,
                tp,
                size,
                decimal,
                flag,
                deflt,
                comment,
            } = column;
            let go_tp = go_type(&go["tp"]);
            let blob = matches!(
                go_tp,
                FieldTypeCode::Blob | FieldTypeCode::MediumBlob | FieldTypeCode::LongBlob
            );
            let expected = (
                go_tp,
                if blob {
                    *size
                } else {
                    go_size(go.get("size"), &constants)
                },
                go.get("decimal")
                    .map_or(0, |decimal| decimal.parse().unwrap()),
                go_flag(go.get("flag")),
                go_default(go.get("deflt")),
                go.get("comment")
                    .map(|comment| resolve(comment, &constants).to_owned()),
            );
            let actual = (
                *tp,
                *size,
                *decimal,
                *flag,
                deflt.map(str::to_owned),
                comment.map(str::to_owned),
            );
            if expected != actual {
                mismatches.push(format!("{table}.{name}: {actual:?}, Go {expected:?}"));
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}
