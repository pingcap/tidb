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

use crate::versioninfo::VersionInfo;

fn release_versions_for_display(info: &VersionInfo) -> (String, Option<String>) {
    if info.kernel_type != "Next Generation" {
        return (info.release_version.clone(), None);
    }
    let component = tidb_mysql::normalize_tidb_release_version_for_next_gen(&info.release_version);
    match tidb_mysql::build_tidbx_release_version(component) {
        Ok(release) => (release, Some(component.to_owned())),
        Err(_) => (info.release_version.clone(), None),
    }
}

/// Returns the build and runtime identity shown by `TIDB_VERSION()` and `-V`.
#[must_use]
pub fn get_tidb_info(info: &VersionInfo) -> String {
    let (release_version, _) = release_versions_for_display(info);
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
        info.edition,
        info.git_hash,
        info.git_branch,
        info.build_ts,
        info.runtime_version,
        crate::israce::RACE_ENABLED,
        info.check_table_before_drop,
        info.store,
    );
    if !info.enterprise_extension_git_hash.is_empty() {
        rendered.push_str("\nEnterprise Extension Commit Hash: ");
        rendered.push_str(&info.enterprise_extension_git_hash);
    }
    rendered.push_str("\nKernel Type: ");
    rendered.push_str(&info.kernel_type);
    rendered
}

/// Writes the structured startup identity and effective configuration.
pub fn print_tidb_info(info: &VersionInfo, config_json: &[u8]) {
    print_tidb_info_to(&tidb_log::l(), info, config_json);
}

fn print_tidb_info_to(logger: &tidb_log::Logger, info: &VersionInfo, config_json: &[u8]) {
    let (release_version, component_version) = release_versions_for_display(info);
    let mut fields = vec![
        Field::new("Release Version", Value::Str(release_version)),
        Field::new("Edition", Value::Str(info.edition.clone())),
        Field::new("Git Commit Hash", Value::Str(info.git_hash.clone())),
        Field::new("Git Branch", Value::Str(info.git_branch.clone())),
        Field::new("UTC Build Time", Value::Str(info.build_ts.clone())),
        Field::new("GoVersion", Value::Str(info.runtime_version.clone())),
        Field::new("Race Enabled", Value::Bool(crate::israce::RACE_ENABLED)),
        Field::new(
            "Check Table Before Drop",
            Value::Bool(info.check_table_before_drop),
        ),
    ];
    if let Some(component_version) = component_version {
        fields.push(Field::new(
            "TiDB Component Version",
            Value::Str(component_version),
        ));
    }
    if let Some(deploy_mode) = &info.deploy_mode {
        fields.push(Field::new("Deploy Mode", Value::Str(deploy_mode.clone())));
    }
    fields.push(Field::new(
        "Kernel Type",
        Value::Str(info.kernel_type.clone()),
    ));
    if !info.enterprise_extension_git_hash.is_empty() {
        fields.push(Field::new(
            "Enterprise Extension Commit Hash",
            Value::Str(info.enterprise_extension_git_hash.clone()),
        ));
    }
    logger.info("Welcome to TiDB.", &fields);
    logger.info(
        "loaded config",
        &[Field::new(
            "config",
            Value::ByteString(config_json.to_vec()),
        )],
    );
}

/// Formats rows as TiDB's ASCII table and rejects empty or ragged inputs.
#[must_use]
pub fn get_print_result(columns: &[String], rows: &[Vec<String>]) -> Option<String> {
    if columns.is_empty() || rows.is_empty() || rows.iter().any(|row| row.len() != columns.len()) {
        return None;
    }

    let mut widths = columns.iter().map(String::len).collect::<Vec<_>>();
    for row in rows {
        for (width, value) in widths.iter_mut().zip(row) {
            *width = (*width).max(value.len());
        }
    }

    let divider = widths
        .iter()
        .map(|width| format!("+{}", "-".repeat(width + 2)))
        .collect::<String>()
        + "+\n";
    let render_row = |row: &[String]| {
        let mut rendered = String::new();
        for (value, width) in row.iter().zip(&widths) {
            rendered.push('|');
            rendered.push(' ');
            rendered.push_str(value);
            rendered.push_str(&" ".repeat(width + 1 - value.len()));
        }
        rendered.push_str("|\n");
        rendered
    };

    let mut output = String::new();
    output.push_str(&divider);
    output.push_str(&render_row(columns));
    output.push_str(&divider);
    for row in rows {
        output.push_str(&render_row(row));
    }
    output.push_str(&divider);
    Some(output)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn print_result_matches_source_boundaries() {
        let columns = vec!["col1".to_owned(), "col2".to_owned(), "col3".to_owned()];
        assert_eq!(get_print_result(&columns, &[vec!["11".to_owned()]]), None);
        assert_eq!(get_print_result(&columns, &[]), None);
        assert_eq!(get_print_result(&[], &[]), None);

        assert_eq!(
            get_print_result(&["é".to_owned()], &[vec!["x".to_owned()]]).as_deref(),
            Some("+----+\n| é |\n+----+\n| x  |\n+----+\n")
        );

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
    }

    #[test]
    fn tidb_info_and_startup_fields_match_kernel_shape() {
        let classic = VersionInfo::build_default();
        let rendered = get_tidb_info(&classic);
        assert!(rendered.contains(&format!("Release Version: {}", classic.release_version)));
        assert!(rendered.contains("\nKernel Type: Classic"));
        assert!(!rendered.contains("TiDB Component Version:"));

        let next_gen = VersionInfo {
            release_version: "v26.3.0".to_owned(),
            ..classic.clone().with_runtime_environment(
                true,
                "tikv",
                "Next Generation",
                Some("starter".to_owned()),
            )
        };
        assert!(get_tidb_info(&next_gen).contains("Release Version: CLOUD.202603.0"));

        let sink = Arc::new(tidb_log::MemorySink::default());
        let log_config = tidb_log::Config {
            level: "info".to_owned(),
            disable_timestamp: true,
            ..tidb_log::Config::default()
        };
        let (logger, _) =
            tidb_log::init_test_logger(sink.clone(), &log_config).expect("test logger");
        print_tidb_info_to(&logger, &next_gen, br#"{"store":"tikv"}"#);
        let output = sink.string();
        assert!(output.contains("Welcome to TiDB."));
        assert!(output.contains("[\"TiDB Component Version\"=v26.3.0]"));
        assert!(output.contains("[\"Deploy Mode\"=starter]"));
        assert!(output.contains("loaded config"));
        assert!(output.contains(r#"{\"store\":\"tikv\"}"#));
    }
}
