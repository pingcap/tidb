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

/// What Go's `normalizeSplitPolicy` reads off `tbInfo`, so the one port
/// serves both the in-process [`KvTable`] and a stored `TableInfo`.
pub trait SplitPolicyTable {
    /// Go `tbInfo.HasClusteredIndex()`.
    fn has_clustered_index(&self) -> bool;
    /// Go `len(tables.FindPrimaryIndex(tbInfo).Columns)` when
    /// `tbInfo.IsCommonHandle`, else `None`.
    fn common_handle_column_count(&self) -> Option<usize>;
    /// Go `len(tbInfo.FindIndexByName(name).Columns)`, `name` lowercase.
    fn index_column_count(&self, name: &str) -> Option<usize>;
}

impl SplitPolicyTable for KvTable {
    fn has_clustered_index(&self) -> bool {
        self.pk_handle_offset().is_some() || !self.common_handle_offsets().is_empty()
    }

    fn common_handle_column_count(&self) -> Option<usize> {
        let offsets = self.common_handle_offsets();
        (!offsets.is_empty()).then_some(offsets.len())
    }

    fn index_column_count(&self, name: &str) -> Option<usize> {
        self.indexes()
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(name))
            .map(|index| index.column_offsets.len())
    }
}

impl SplitPolicyTable for tidb_model::TableInfo {
    fn has_clustered_index(&self) -> bool {
        self.pk_is_handle || self.is_common_handle
    }

    fn common_handle_column_count(&self) -> Option<usize> {
        if !self.is_common_handle {
            return None;
        }
        self.indices
            .iter_deref()
            .find(|index| index.read().primary)
            .map(|index| index.read().columns.len())
    }

    fn index_column_count(&self, name: &str) -> Option<usize> {
        self.indices
            .iter_deref()
            .find(|index| index.read().name.lowercase() == name)
            .map(|index| index.read().columns.len())
    }
}

/// Which keyspace a split clause names.
#[derive(Clone, Copy, Debug)]
pub enum SplitPolicyTarget<'a> {
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
pub fn normalize_split_policy(
    target: SplitPolicyTarget<'_>,
    option: &tidb_ast::SplitOption,
    table: &impl SplitPolicyTable,
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
    if table.has_clustered_index() && primary_key {
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
    let common_handle_columns = table.common_handle_column_count().filter(|_| primary_key);
    let column_count = if let Some(count) = common_handle_columns {
        count
    } else if index_name.is_empty() {
        1
    } else {
        let Some(count) = table.index_column_count(&index_name) else {
            return Err(DriverError::DdlCoded {
                errno: tidb_error::tidb::errcode::ErrWrongNameForIndex,
                message: format!("Incorrect index name '{index_name}'"),
            });
        };
        count
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
