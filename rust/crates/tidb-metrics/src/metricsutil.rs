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

//! Go `pkg/util/metricsutil/db_labels.go`.

/// Go `GetDBNames`: the database labels for SQL metrics, honoring
/// `Status.RecordDBLabel` (`record_db_label`, read by the caller): the
/// distinct databases of the statement's tables (Go `StmtCtx.Tables`), else
/// the lowercased current database; one empty label when the record is off.
#[must_use]
pub fn get_db_names<'a>(
    record_db_label: bool,
    table_dbs: impl IntoIterator<Item = &'a str>,
    current_db: &str,
) -> Vec<String> {
    if !record_db_label {
        return vec![String::new()];
    }
    let mut names: Vec<String> = Vec::new();
    for db in table_dbs {
        if !names.iter().any(|name| name == db) {
            names.push(db.to_owned());
        }
    }
    if names.is_empty() {
        names.push(current_db.to_lowercase());
    }
    names
}

#[cfg(test)]
mod tests {
    use super::get_db_names;

    /// Go `TestGetDBNamesLabels`.
    #[test]
    fn test_get_db_names_labels() {
        let sorted = |mut names: Vec<String>| {
            names.sort();
            names
        };
        for (name, record_db_label, tables, current_db, want) in [
            ("record DB label disabled", false, vec!["databaseb"], "DatabaseA", vec![""]),
            ("empty tables", true, vec![], "DatabaseA", vec!["databasea"]),
            ("empty current DB", true, vec![], "", vec![""]),
            (
                "cross DB deduplication",
                true,
                vec!["databasea", "databaseb", "databasea"],
                "UnusedDB",
                vec!["databasea", "databaseb"],
            ),
        ] {
            assert_eq!(sorted(get_db_names(record_db_label, tables, current_db)), want, "{name}");
        }
    }
}
