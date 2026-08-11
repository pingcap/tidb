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

//! Whole-package semantic contract for `pkg/meta/metadef`.

use tidb_metadef::{is_br_related_db, is_mem_or_sys_db};

const GO_SYSTEM_TABLES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../pkg/meta/metadef/system_tables_def.go"
));
const RUST_SYSTEM_TABLES: &str = include_str!("../src/system_tables_def.rs");

fn go_public_string_constants(source: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut offset = 0;
    while offset < source.len() {
        let line_end = source[offset..]
            .find('\n')
            .map_or(source.len(), |index| offset + index);
        let line = &source[offset..line_end];
        let trimmed = line.trim_start();
        let Some((name, literal)) = trimmed.split_once(" = ") else {
            offset = line_end.saturating_add(1);
            continue;
        };
        if !name.starts_with(char::is_uppercase)
            || !name.chars().all(|ch| ch.is_ascii_alphanumeric())
        {
            offset = line_end.saturating_add(1);
            continue;
        }

        let literal_offset = offset + line.find(literal).expect("literal starts on this line");
        match literal.as_bytes().first() {
            Some(b'`') => {
                let start = literal_offset + 1;
                let end = start
                    + source[start..]
                        .find('`')
                        .expect("Go raw string constant is terminated");
                values.push(source[start..end].to_owned());
                offset = end + 1;
            }
            Some(b'"') => {
                let start = literal_offset + 1;
                let end = start
                    + source[start..]
                        .find('"')
                        .expect("Go quoted string constant is terminated");
                assert!(
                    !source[start..end].contains('\\'),
                    "the compact source reader intentionally accepts only unescaped quoted constants"
                );
                values.push(source[start..end].to_owned());
                offset = end + 1;
            }
            _ => offset = line_end.saturating_add(1),
        }
    }
    values
}

fn rust_public_string_constants(source: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut offset = 0;
    while let Some(relative) = source[offset..].find("pub const ") {
        let declaration = offset + relative;
        let Some(value_relative) = source[declaration..].find(": &str = ") else {
            break;
        };
        let literal = declaration + value_relative + ": &str = ".len();
        if source[literal..].starts_with("r#\"") {
            let start = literal + 3;
            let end = start
                + source[start..]
                    .find("\"#;")
                    .expect("Rust raw string constant is terminated");
            values.push(source[start..end].to_owned());
            offset = end + 3;
        } else if source[literal..].starts_with('"') {
            let start = literal + 1;
            let end = start
                + source[start..]
                    .find('"')
                    .expect("Rust quoted string constant is terminated");
            assert!(
                !source[start..end].contains('\\'),
                "the compact source reader intentionally accepts only unescaped quoted constants"
            );
            values.push(source[start..end].to_owned());
            offset = end + 2;
        } else {
            offset = literal + 1;
        }
    }
    values
}

#[test]
fn every_public_string_constant_matches_the_go_package() {
    let mut go = go_public_string_constants(GO_SYSTEM_TABLES);
    let mut rust = rust_public_string_constants(RUST_SYSTEM_TABLES);
    go.sort();
    rust.sort();
    assert_eq!(rust, go);
}

#[test]
fn composed_and_br_database_predicates_preserve_the_package_contract() {
    assert!(is_mem_or_sys_db("information_schema"));
    assert!(is_mem_or_sys_db("mysql"));
    assert!(!is_mem_or_sys_db("ordinary"));

    assert!(is_br_related_db("__TiDB_BR_Temporary_restore"));
    assert!(!is_br_related_db("__tidb_br_temporary_restore"));
    assert!(!is_br_related_db("ordinary"));
}
