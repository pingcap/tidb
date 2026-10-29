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

//! Go `pkg/ddl/multi_schema_change.go`'s admission of the sub-jobs one
//! ALTER TABLE combines.

use std::collections::HashSet;

use tidb_error::mysql::MysqlError;

/// Go `checkOperateSameColAndIdx`: a multi-schema change names one column
/// or index at most once among its actions.
pub fn check_multi_schema_names(info: &tidb_model::MultiSchemaInfo) -> Result<(), MysqlError> {
    fn check_names(
        names: &[tidb_ast::CiString],
        add_to_seen: bool,
        seen: &mut HashSet<String>,
        kind: &str,
    ) -> Result<(), MysqlError> {
        for name in names {
            let canonical = name.lowercase().to_owned();
            if seen.contains(&canonical) {
                // Go `ErrOperateSameColumn` / `ErrOperateSameIndex`.
                let proto = if kind == "index" {
                    &tidb_dbterror::ERR_OPERATE_SAME_INDEX
                } else {
                    &tidb_dbterror::ERR_OPERATE_SAME_COLUMN
                };
                return Err(MysqlError::from_terror(&proto.generate_with_stack(
                    format!("Unsupported operate same {kind} '{canonical}'"),
                )));
            }
            if add_to_seen {
                seen.insert(canonical);
            }
        }
        Ok(())
    }

    let mut columns = HashSet::new();
    check_names(&info.add_columns.snapshot(), true, &mut columns, "column")?;
    check_names(&info.drop_columns.snapshot(), true, &mut columns, "column")?;
    check_names(
        &info.position_columns.snapshot(),
        false,
        &mut columns,
        "column",
    )?;
    check_names(
        &info.modify_columns.snapshot(),
        true,
        &mut columns,
        "column",
    )?;
    check_names(
        &info.relative_columns.snapshot(),
        false,
        &mut columns,
        "column",
    )?;

    let mut indexes = HashSet::new();
    check_names(&info.add_indexes.snapshot(), true, &mut indexes, "index")?;
    check_names(&info.drop_indexes.snapshot(), true, &mut indexes, "index")?;
    check_names(&info.alter_indexes.snapshot(), true, &mut indexes, "index")?;
    Ok(())
}
