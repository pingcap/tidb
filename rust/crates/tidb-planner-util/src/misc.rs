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

//! Go `pkg/planner/util/misc.go`.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use tidb_datatype::FieldName;
use tidb_expr::column::Column;

/// Go `util.GetMaxSortPrefix(sortCols, allCols)`
/// (`pkg/planner/util/misc.go`): how many leading `sort_cols` are also in
/// `all_cols`, positionally.
#[must_use]
pub fn max_sort_prefix_len(sort_cols: &[Column], all_cols: &[Column]) -> usize {
    let limit = sort_cols.len().min(all_cols.len());
    let mut length = 0;
    while length < limit {
        if !all_cols
            .iter()
            .any(|column| column.unique_id == sort_cols[length].unique_id)
        {
            break;
        }
        length += 1;
    }
    length
}

/// Go `hint.HintedTable` identity used by join hints.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HintedTable {
    /// Go `HintedTable.DBName.L`, empty when the plan's names carry none.
    pub db_name: String,
    /// Go `HintedTable.TblName.L`.
    pub table_name: String,
    /// Go `HintedTable.SelectOffset`: the plan's own query block, or its
    /// parent's when the block is a named derived table.
    pub select_offset: i32,
}

/// Go `hint.PlanHints`' join half for the current query block.
#[derive(Clone, Debug, Default)]
pub struct JoinHints {
    /// Table alias (lowercase, `db.table` when the hint is qualified and
    /// `table` otherwise) to the [`tidb_hint::join_hint_flags`] bits hinted for it.
    pub tables: BTreeMap<String, u32>,
    /// Go `PlanHints.LeadingList`, retaining the nested AST order.
    pub leading: Option<Vec<tidb_ast::LeadingElement>>,
    /// Go `PlanHints` itself, which the preference checks consult.
    pub canonical: Option<Rc<RefCell<tidb_hint::PlanHints>>>,
    /// The query block these hints were read for.
    pub select_offset: i32,
    /// Go `SessionVars.CurrentDB`, which `ExtractTableAlias` gives an alias
    /// whose names carry no database (a derived table's).
    pub current_db: String,
    /// Go `SessionVars.EnableAdvancedJoinHint`.
    pub advanced_join_hint: bool,
    /// Go `SessionVars.PlannerSelectBlockAsName`: each derived table's
    /// alias by its query block, shared with the builder that records them.
    pub block_as_names: Rc<RefCell<BTreeMap<i32, String>>>,
}

impl JoinHints {
    /// Adapts the canonical Go `PlanHints` join fields to the preference bits
    /// consumed by `LogicalJoin`.
    #[must_use]
    pub fn from_plan_hints(
        canonical: Rc<RefCell<tidb_hint::PlanHints>>,
        select_offset: i32,
        current_db: &str,
        advanced_join_hint: bool,
        block_as_names: Rc<RefCell<BTreeMap<i32, String>>>,
    ) -> Self {
        let plan = canonical.borrow();
        let mut hints = Self::default();
        for (tables, flag) in [
            (&plan.index_join.inlj_tables, tidb_hint::join_hint_flags::INLJ),
            (&plan.index_join.inlhj_tables, tidb_hint::join_hint_flags::INLHJ),
            (&plan.index_join.inlmj_tables, tidb_hint::join_hint_flags::INLMJ),
            (&plan.hash_join, tidb_hint::join_hint_flags::HASH_JOIN),
            (&plan.hash_join_build, tidb_hint::join_hint_flags::HJ_BUILD),
            (&plan.hash_join_probe, tidb_hint::join_hint_flags::HJ_PROBE),
            (&plan.no_hash_join, tidb_hint::join_hint_flags::NO_HASH_JOIN),
            (&plan.sort_merge_join, tidb_hint::join_hint_flags::MERGE_JOIN),
            (&plan.no_merge_join, tidb_hint::join_hint_flags::NO_MERGE_JOIN),
            (
                &plan.no_index_join.inlj_tables,
                tidb_hint::join_hint_flags::NO_INDEX_JOIN,
            ),
            (
                &plan.no_index_join.inlhj_tables,
                tidb_hint::join_hint_flags::NO_INDEX_HASH_JOIN,
            ),
            (
                &plan.no_index_join.inlmj_tables,
                tidb_hint::join_hint_flags::NO_INDEX_MERGE_JOIN,
            ),
            (&plan.broadcast_join, tidb_hint::join_hint_flags::BC_JOIN),
            (&plan.shuffle_join, tidb_hint::join_hint_flags::SHUFFLE_JOIN),
        ] {
            for table in tables
                .iter()
                .filter(|table| table.select_offset == select_offset)
            {
                hints.hint_table(&table.database_name, &table.table_name, flag);
            }
        }
        hints.leading = plan.leading_list.clone();
        drop(plan);
        hints.canonical = Some(canonical);
        hints.select_offset = select_offset;
        hints.current_db = tidb_mysql::to_lowercase(current_db);
        hints.advanced_join_hint = advanced_join_hint;
        hints.block_as_names = block_as_names;
        hints
    }

    /// Go `ExtractTableAlias`'s query-block choice: "For sub-queries like
    /// `(select * from t) t1`, t1 should belong to its surrounding select
    /// block."
    pub fn alias_select_offset(&self, plan_offset: i32) -> i32 {
        if plan_offset != self.select_offset
            && plan_offset >= 0
            && self
                .block_as_names
                .borrow()
                .get(&plan_offset)
                .is_some_and(|name| !name.is_empty())
        {
            self.select_offset
        } else {
            plan_offset
        }
    }

    /// Go `ExtractTableAlias`'s database fallback: an alias whose names carry
    /// no database takes the session's current one.
    pub fn alias_database(&self, alias: &HintedTable) -> String {
        if alias.db_name.is_empty() {
            self.current_db.clone()
        } else {
            alias.db_name.clone()
        }
    }

    /// Go `PlanHints.IfPreferXxx(alias)` for whichever bit `flag` is: the
    /// alias carries the hint, matched qualified-first then bare.
    #[must_use]
    pub fn prefers(&self, alias: Option<&HintedTable>, flag: u32) -> bool {
        let Some(alias) = alias else { return false };
        if let Some(canonical) = &self.canonical {
            // Go `ExtractTableAlias`: "if dbName.L == "" { dbName =
            // CurrentDB }", the database the hint's own table defaulted to.
            let table = tidb_hint::HintedTable {
                database_name: self.alias_database(alias),
                table_name: alias.table_name.clone(),
                select_offset: alias.select_offset,
                ..tidb_hint::HintedTable::default()
            };
            let tables = [Some(&table)];
            let mut canonical = canonical.borrow_mut();
            return match flag {
                tidb_hint::join_hint_flags::INLJ => canonical.prefer_index_join(&tables),
                tidb_hint::join_hint_flags::INLHJ => canonical.prefer_index_hash_join(&tables),
                tidb_hint::join_hint_flags::INLMJ => canonical.prefer_index_merge_join(&tables),
                tidb_hint::join_hint_flags::HJ_BUILD => canonical.prefer_hash_join_build(&tables),
                tidb_hint::join_hint_flags::HJ_PROBE => canonical.prefer_hash_join_probe(&tables),
                tidb_hint::join_hint_flags::HASH_JOIN => canonical.prefer_hash_join(&tables),
                tidb_hint::join_hint_flags::NO_HASH_JOIN => canonical.prefer_no_hash_join(&tables),
                tidb_hint::join_hint_flags::MERGE_JOIN => canonical.prefer_merge_join(&tables),
                tidb_hint::join_hint_flags::NO_MERGE_JOIN => canonical.prefer_no_merge_join(&tables),
                tidb_hint::join_hint_flags::NO_INDEX_JOIN => canonical.prefer_no_index_join(&tables),
                tidb_hint::join_hint_flags::NO_INDEX_HASH_JOIN => canonical.prefer_no_index_hash_join(&tables),
                tidb_hint::join_hint_flags::NO_INDEX_MERGE_JOIN => {
                    canonical.prefer_no_index_merge_join(&tables)
                }
                tidb_hint::join_hint_flags::BC_JOIN => canonical.prefer_broadcast_join(&tables),
                tidb_hint::join_hint_flags::SHUFFLE_JOIN => canonical.prefer_shuffle_join(&tables),
                _ => false,
            };
        }
        let qualified = format!("{}.{}", alias.db_name, alias.table_name);
        let bits = self
            .tables
            .get(&qualified)
            .or_else(|| self.tables.get(&alias.table_name))
            .copied()
            .unwrap_or(0);
        bits & flag != 0
    }

    /// Records `flags` for an (optionally qualified) table alias.
    pub fn hint_table(&mut self, db_name: &str, table_name: &str, flags: u32) {
        let key = if db_name.is_empty() {
            tidb_mysql::to_lowercase(table_name)
        } else {
            format!(
                "{}.{}",
                tidb_mysql::to_lowercase(db_name),
                tidb_mysql::to_lowercase(table_name)
            )
        };
        *self.tables.entry(key).or_insert(0) |= flags;
    }
}

/// Go `util.ExtractTableAlias(p, parentOffset)`
/// (`pkg/planner/util/misc.go:244`), minus the query-block comparison this
/// crate has no offsets for (see this module's boundaries).
///
/// The rejection rules are Go's exactly: a plan whose output names disagree on
/// the table (or carry a database with no table) has no single alias, so no
/// hint can name it.
#[must_use]
pub fn extract_table_alias(names: &[FieldName]) -> Option<HintedTable> {
    if names.is_empty() {
        return None;
    }
    let first = names
        .iter()
        .find(|name| !name.names.table.lower.is_empty())?;
    for name in names {
        if name.names.table.lower.is_empty() {
            // "A valid aliasable column should always carry both
            // DBName/TblName together."
            if !name.names.database.lower.is_empty() {
                return None;
            }
            continue;
        }
        let db_conflicts = !name.names.database.lower.is_empty()
            && !first.names.database.lower.is_empty()
            && name.names.database.lower != first.names.database.lower;
        if name.names.table.lower != first.names.table.lower || db_conflicts {
            return None;
        }
    }
    Some(HintedTable {
        db_name: first.names.database.lower.clone(),
        table_name: first.names.table.lower.clone(),
        select_offset: 0,
    })
}

#[cfg(test)]
mod tests {
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;

    use super::max_sort_prefix_len;

    fn column(unique_id: i64) -> Column {
        Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong))
    }

    /// Go `util.GetMaxSortPrefix`, as `PreparePossibleProperties` uses it.
    #[test]
    fn max_sort_prefix_stops_at_the_first_miss() {
        assert_eq!(
            max_sort_prefix_len(&[column(1), column(9), column(2)], &[column(1), column(2)]),
            1
        );
        assert_eq!(max_sort_prefix_len(&[], &[column(1)]), 0);
    }
}
