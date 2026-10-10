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

//! Region split POLICIES (`pkg/ddl/split_region.go` `normalizeSplitPolicy`):
//! the `SPLIT [PRIMARY KEY | INDEX name] BETWEEN (...) AND (...) REGIONS n`
//! clauses of CREATE TABLE and ALTER TABLE, recorded on the table
//! (`TableInfo.TableSplitPolicy`) or on an index
//! (`IndexInfo.RegionSplitPolicy`) and printed back by SHOW CREATE TABLE.

use crate::kv_table::KvTable;
use crate::DriverError;
use tidb_model::RegionSplitPolicy;

/// Which keyspace a split clause names.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SplitPolicyTarget<'a> {
    /// The table's record keyspace (`TableLevel`).
    Table,
    /// `SPLIT PRIMARY KEY`.
    PrimaryKey,
    /// `SPLIT INDEX name`.
    Index(&'a str),
}

/// Go `normalizeSplitPolicy`: validates one clause against `table` and
/// returns the lower-cased index name it targets (`""` for the table) with
/// the policy to record. Each bound is built and evaluated as Go's
/// `BuildSimpleExpr` + `Eval`, then stored as its restored SQL text.
pub(crate) fn normalize_split_policy(
    target: SplitPolicyTarget<'_>,
    option: &tidb_ast::SplitOption,
    table: &KvTable,
) -> Result<(String, RegionSplitPolicy), DriverError> {
    let primary_name = "primary";
    let (index_name, primary_key) = match target {
        SplitPolicyTarget::Table => (String::new(), false),
        SplitPolicyTarget::PrimaryKey => (primary_name.to_owned(), true),
        SplitPolicyTarget::Index(name) => {
            let name = name.to_lowercase();
            let primary_key = name == primary_name;
            (name, primary_key)
        }
    };
    let common_handle = !table.common_handle_offsets().is_empty();
    let clustered = table.pk_handle_offset().is_some() || common_handle;
    if clustered && primary_key {
        return Err(forbidden("SPLIT PRIMARY is only for non-clustered table"));
    }
    let (lower, upper, regions) = match option {
        tidb_ast::SplitOption::Between {
            lower,
            upper,
            regions,
        } => (lower.as_slice(), upper.as_slice(), *regions),
        // The BY form carries no region count.
        tidb_ast::SplitOption::By(_) => (&[][..], &[][..], 0),
    };
    if regions < 1 {
        return Err(forbidden(
            "SPLIT REGION number must not be zero or negative",
        ));
    }
    let column_count = if common_handle && primary_key {
        table.common_handle_offsets().len()
    } else if index_name.is_empty() {
        1
    } else {
        let Some(index) = table
            .indexes()
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(&index_name))
        else {
            return Err(DriverError::DdlCoded {
                errno: tidb_error::tidb::errcode::ErrWrongNameForIndex,
                message: format!("Incorrect index name '{index_name}'"),
            });
        };
        index.column_offsets.len()
    };
    if column_count != upper.len() || column_count != lower.len() {
        return Err(DriverError::DdlCoded {
            errno: tidb_error::tidb::errcode::ErrInvalidSplitRegionRanges,
            message:
                "Failed to split region ranges: length of index columns and split values differ"
                    .to_owned(),
        });
    }
    let policy = RegionSplitPolicy {
        lower: tidb_model::GoSharedSlice::from_vec(restored_bounds(lower)?),
        upper: tidb_model::GoSharedSlice::from_vec(restored_bounds(upper)?),
        regions,
    };
    Ok((index_name, policy))
}

/// Go `ErrForbiddenDDL` (8267, `%s is forbidden`).
fn forbidden(what: &str) -> DriverError {
    DriverError::DdlCoded {
        errno: tidb_error::tidb::errcode::ErrForbiddenDDL,
        message: format!("{what} is forbidden"),
    }
}

/// Builds and evaluates each bound (`BuildSimpleExpr` + `Eval`), then keeps
/// its `format.DefaultRestoreFlags` text.
fn restored_bounds(bounds: &[tidb_ast::Expr]) -> Result<Vec<String>, DriverError> {
    bounds
        .iter()
        .map(|bound| {
            let built = tidb_expr::simple_expr::build_simple_expr(
                &NoColumns,
                bound,
                &tidb_expr::simple_expr::BuildOptions::default(),
            )
            .map_err(|error| DriverError::DdlCoded {
                errno: tidb_error::tidb::errcode::ErrUnknown,
                message: error.to_string(),
            })?;
            built
                .eval(&tidb_expr::NoColumns, tidb_chunk::row::Row::empty())
                .map_err(crate::ExecError::Eval)?;
            Ok(bound.restore_with_flags(tidb_ast::RestoreFlags::DEFAULT))
        })
        .collect()
}

/// The empty column scope Go's DDL expression context gives a split bound.
struct NoColumns;

impl tidb_expr::rewriter::ColumnResolver for NoColumns {
    fn resolve(&self, _path: &[String]) -> Option<(usize, tidb_datatype::FieldType, i64)> {
        None
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        tidb_datatype::SessionTimeZone::utc()
    }
}

/// Records a normalized policy where Go's DDL puts it.
pub(crate) fn set_split_policy(table: &mut KvTable, index_name: &str, policy: RegionSplitPolicy) {
    if index_name.is_empty() {
        table.set_table_split_policy(Some(policy));
        return;
    }
    if let Some(index_id) = table
        .indexes()
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case(index_name))
        .map(|index| index.id)
    {
        table.set_index_split_policy(index_id, policy);
    }
}

/// Go `checkAndWarnMissingRegionSplitPolicy`: a new index on a table whose
/// other indexes carry split policies draws a 1105 warning.
pub(crate) fn warn_missing_region_split_policy(
    table: &KvTable,
    new_index_name: &str,
    ctx: &crate::StmtContext,
) {
    if table
        .indexes()
        .iter()
        .any(|index| table.index_split_policy(index.id).is_some())
    {
        ctx.append_warning_parts(
            tidb_error::tidb::errcode::ErrUnknown,
            &format!(
                "It is recommended to add a region split strategy to the new index '{new_index_name}' to avoid write hotspots"
            ),
        );
    }
}
