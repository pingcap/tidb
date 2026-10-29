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

//! Go `pkg/ddl/split_region.go`'s region split POLICIES
//! (`normalizeSplitPolicy`): the `SPLIT [PRIMARY KEY | INDEX name] BETWEEN
//! (...) AND (...) REGIONS n` clauses of CREATE TABLE and ALTER TABLE,
//! recorded on the table (`TableInfo.TableSplitPolicy`) or on an index
//! (`IndexInfo.RegionSplitPolicy`) and printed back by SHOW CREATE TABLE.

use tidb_error::mysql::MysqlError;

use tidb_hack::GoToLower;
use tidb_model::RegionSplitPolicy;

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

/// Go `normalizeSplitPolicy`: validates one clause against `tb_info` and
/// returns the lower-cased index name it targets (`""` for the table) with
/// the policy to record. Each bound is built and evaluated as Go's
/// `BuildSimpleExpr` + `Eval`, then stored as its restored SQL text.
pub fn normalize_split_policy(
    target: SplitPolicyTarget<'_>,
    option: &tidb_ast::SplitOption,
    tb_info: &tidb_model::TableInfo,
) -> Result<(String, RegionSplitPolicy), MysqlError> {
    let primary_name = "primary";
    let (index_name, primary_key) = match target {
        SplitPolicyTarget::Table => (String::new(), false),
        SplitPolicyTarget::PrimaryKey => (primary_name.to_owned(), true),
        SplitPolicyTarget::Index(name) => {
            let name = name.go_to_lower();
            let primary_key = name == primary_name;
            (name, primary_key)
        }
    };
    if tb_info.has_clustered_index() && primary_key {
        return Err(MysqlError::from_terror(
            &tidb_util::dbterror::ERR_FORBIDDEN_DDL.generate_with_stack_by_args(&[
                "SPLIT PRIMARY is only for non-clustered table".into(),
            ]),
        ));
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
        return Err(MysqlError::from_terror(
            &tidb_util::dbterror::ERR_FORBIDDEN_DDL.generate_with_stack_by_args(&[
                "SPLIT REGION number must not be zero or negative".into(),
            ]),
        ));
    }
    // Go: the common handle's primary index for `SPLIT PRIMARY KEY` on a
    // clustered table, one column for the table level, else the named index.
    let common_handle_columns = (primary_key && tb_info.is_common_handle)
        .then(|| {
            tb_info
                .indices
                .iter_deref()
                .find(|index| index.read().primary)
                .map(|index| index.read().columns.len())
        })
        .flatten();
    let column_count = if let Some(count) = common_handle_columns {
        count
    } else if index_name.is_empty() {
        1
    } else {
        let Some(count) = tb_info
            .indices
            .iter_deref()
            .find(|index| index.read().name.lowercase() == index_name)
            .map(|index| index.read().columns.len())
        else {
            return Err(MysqlError::from_terror(
                &tidb_util::dbterror::ERR_WRONG_NAME_FOR_INDEX
                    .generate_with_stack_by_args(&[index_name.into()]),
            ));
        };
        count
    };
    if column_count != upper.len() || column_count != lower.len() {
        return Err(MysqlError::from_terror(
            &tidb_util::dbterror::exeerrors::ERR_INVALID_SPLIT_REGION_RANGES
                .generate_with_stack_by_args(&[
                    "length of index columns and split values differ".into()
                ]),
        ));
    }
    let policy = RegionSplitPolicy {
        lower: tidb_goruntime::go_runtime::GoSharedSlice::from_vec(restored_bounds(lower)?),
        upper: tidb_goruntime::go_runtime::GoSharedSlice::from_vec(restored_bounds(upper)?),
        regions,
    };
    Ok((index_name, policy))
}

/// Builds and evaluates each bound (`BuildSimpleExpr` + `Eval`), then keeps
/// its `format.DefaultRestoreFlags` text.
fn restored_bounds(bounds: &[tidb_ast::Expr]) -> Result<Vec<String>, MysqlError> {
    bounds
        .iter()
        .map(|bound| {
            let built = tidb_expr::simple_expr::build_simple_expr(
                &NoColumns,
                bound,
                &tidb_expr::simple_expr::BuildOptions::default(),
            )
            .map_err(|error| tidb_error::mysql::MysqlError::unknown(error.to_string()))?;
            built
                .eval(&tidb_expr::NoColumns, tidb_chunk::row::Row::empty())
                .map_err(|error| error.to_mysql_error())?;
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
