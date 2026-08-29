// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Server identity and fixed-width table rendering from `pkg/util/printer`.

use tidb_log::{Field, Value};

const RUST_VERSION: &str = env!("TIDB_RUST_VERSION");

fn release_versions_for_display() -> (String, Option<String>) {
    let versions = tidb_mysql::runtime_versions();
    if !tidb_config::kerneltype::is_next_gen() {
        return (versions.tidb_release_version, None);
    }
    let component =
        tidb_mysql::normalize_tidb_release_version_for_next_gen(&versions.tidb_release_version);
    match tidb_mysql::build_tidbx_release_version(component) {
        Ok(release) => (release, Some(component.to_owned())),
        Err(_) => (versions.tidb_release_version, None),
    }
}

/// Returns the build and runtime identity shown by `TIDB_VERSION()` and `-V`.
#[must_use]
pub fn get_tidb_info() -> String {
    let (release_version, _) = release_versions_for_display();
    let config = tidb_config::config_tree::config::get_global_config();
    let mut rendered = format!(
        "Release Version: {release_version}\n\
         Edition: {}\n\
         Git Commit Hash: {}\n\
         Git Branch: {}\n\
         UTC Build Time: {}\n\
         GoVersion: {}\n\
         Race Enabled: {}\n\
         Check Table Before Drop: {}\n\
         Store: {}",
        crate::versioninfo::tidb_edition(),
        crate::versioninfo::TIDB_GIT_HASH,
        crate::versioninfo::TIDB_GIT_BRANCH,
        crate::versioninfo::TIDB_BUILD_TS,
        RUST_VERSION,
        crate::israce::RACE_ENABLED,
        tidb_config::config_tree::config::check_table_before_drop(),
        config.store,
    );
    if !crate::versioninfo::TIDB_ENTERPRISE_EXTENSION_GIT_HASH.is_empty() {
        rendered.push_str("\nEnterprise Extension Commit Hash: ");
        rendered.push_str(crate::versioninfo::TIDB_ENTERPRISE_EXTENSION_GIT_HASH);
    }
    rendered.push_str("\nKernel Type: ");
    rendered.push_str(tidb_config::kerneltype::name());
    rendered
}

/// Writes the structured startup identity and effective configuration.
pub fn print_tidb_info() {
    let logger = crate::logutil::bg_logger();
    logger.info("Welcome to TiDB.", &tidb_info_fields());
    let config_json = serde_json::to_vec(&tidb_config::config_tree::config::get_global_config())
        .expect("global config is serializable");
    logger.info(
        "loaded config",
        &[Field::new("config", Value::ByteString(config_json))],
    );
}

fn tidb_info_fields() -> Vec<Field> {
    let (release_version, component_version) = release_versions_for_display();
    let mut fields = vec![
        Field::new("Release Version", Value::Str(release_version)),
        Field::new("Edition", Value::Str(crate::versioninfo::tidb_edition())),
        Field::new(
            "Git Commit Hash",
            Value::Str(crate::versioninfo::TIDB_GIT_HASH.to_owned()),
        ),
        Field::new(
            "Git Branch",
            Value::Str(crate::versioninfo::TIDB_GIT_BRANCH.to_owned()),
        ),
        Field::new(
            "UTC Build Time",
            Value::Str(crate::versioninfo::TIDB_BUILD_TS.to_owned()),
        ),
        Field::new("GoVersion", Value::Str(RUST_VERSION.to_owned())),
        Field::new("Race Enabled", Value::Bool(crate::israce::RACE_ENABLED)),
        Field::new(
            "Check Table Before Drop",
            Value::Bool(tidb_config::config_tree::config::check_table_before_drop()),
        ),
    ];
    if let Some(component_version) = component_version {
        fields.push(Field::new(
            "TiDB Component Version",
            Value::Str(component_version),
        ));
    }
    if tidb_config::kerneltype::is_next_gen() {
        fields.push(Field::new(
            "Deploy Mode",
            Value::Str(tidb_config::deploymode::get().to_string()),
        ));
    }
    fields.push(Field::new(
        "Kernel Type",
        Value::Str(tidb_config::kerneltype::name().to_owned()),
    ));
    if !crate::versioninfo::TIDB_ENTERPRISE_EXTENSION_GIT_HASH.is_empty() {
        fields.push(Field::new(
            "Enterprise Extension Commit Hash",
            Value::Str(crate::versioninfo::TIDB_ENTERPRISE_EXTENSION_GIT_HASH.to_owned()),
        ));
    }
    fields
}

/// Formats byte-preserving Go strings as TiDB's ASCII table.
///
/// Widths are byte counts, and output bytes are copied unchanged. This is the
/// semantic boundary of Go's `string`, which is not restricted to UTF-8.
#[must_use]
pub fn get_print_result_bytes<C, D>(columns: &[C], rows: &[Vec<D>]) -> Option<Vec<u8>>
where
    C: AsRef<[u8]>,
    D: AsRef<[u8]>,
{
    if columns.is_empty() || rows.is_empty() || rows.iter().any(|row| row.len() != columns.len()) {
        return None;
    }

    let mut widths = columns
        .iter()
        .map(|column| column.as_ref().len())
        .collect::<Vec<_>>();
    for row in rows {
        for (width, value) in widths.iter_mut().zip(row) {
            *width = (*width).max(value.as_ref().len());
        }
    }

    let mut divider = Vec::new();
    for width in &widths {
        divider.push(b'+');
        divider.resize(divider.len() + width + 2, b'-');
    }
    divider.extend_from_slice(b"+\n");

    let mut output = Vec::new();
    output.extend_from_slice(&divider);
    render_print_row(columns, &widths, &mut output);
    output.extend_from_slice(&divider);
    for row in rows {
        render_print_row(row, &widths, &mut output);
    }
    output.extend_from_slice(&divider);
    Some(output)
}

fn render_print_row<T: AsRef<[u8]>>(row: &[T], widths: &[usize], output: &mut Vec<u8>) {
    for (value, width) in row.iter().zip(widths) {
        let value = value.as_ref();
        output.extend_from_slice(b"| ");
        output.extend_from_slice(value);
        output.resize(output.len() + width + 1 - value.len(), b' ');
    }
    output.extend_from_slice(b"|\n");
}

/// Formats UTF-8 rows as TiDB's ASCII table and rejects empty or ragged inputs.
#[must_use]
pub fn get_print_result(columns: &[String], rows: &[Vec<String>]) -> Option<String> {
    get_print_result_bytes(columns, rows).map(|output| {
        String::from_utf8(output).expect("UTF-8 table inputs produce UTF-8 table output")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_result() {
        let columns = vec!["col1".to_owned(), "col2".to_owned(), "col3".to_owned()];
        assert_eq!(get_print_result(&columns, &[vec!["11".to_owned()]]), None);

        let rows = vec![
            vec!["11".to_owned(), "12".to_owned(), "13".to_owned()],
            vec!["21".to_owned(), "22".to_owned(), "23".to_owned()],
        ];
        assert_eq!(
            get_print_result(&columns, &rows).as_deref(),
            Some(
                "+------+------+------+\n\
                 | col1 | col2 | col3 |\n\
                 +------+------+------+\n\
                 | 11   | 12   | 13   |\n\
                 | 21   | 22   | 23   |\n\
                +------+------+------+\n"
            )
        );
        assert_eq!(get_print_result(&columns, &[]), None);
        assert_eq!(get_print_result(&[], &[]), None);
    }
}
