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

//! Table-level `USE`/`FORCE`/`IGNORE INDEX` hints, resolved into the set of
//! access paths the optimizer is still allowed to consider.
//!
//! This is Go's `getPossibleAccessPaths`
//! (`pkg/planner/core/planbuilder.go:1440`) with everything this tier has no
//! analogue for -- TiFlash engines, MV/global indexes, index-merge and
//! index-lookup-pushdown hints -- left out. What remains is the whole of the
//! rule for a TiKV single-store table, and it is deliberately structured the
//! way Go structures it, because the surprising parts are all in the
//! structure:
//!
//! * `FORCE` and `USE` are the SAME hint. Go says so in place: "Currently we
//!   don't distinguish between `FORCE` and `USE` because our cost estimation
//!   is not reliable." NEITHER makes the optimizer prefer the named index;
//!   both merely delete every OTHER path from the candidate set, after which
//!   the ordinary cost model runs over what is left. A `FORCE INDEX` that
//!   names two indexes still costs both and picks the cheaper.
//! * A named path is additionally marked forced, which is Go's `path.Forced`,
//!   and that flag survives into skyline pruning: `keepIndex := ... ||
//!   path.Forced || path.IsSingleScan` (`find_best_task.go:1830`). It is the
//!   only reason `FORCE INDEX(idx_c) WHERE b = 2` can plan at all -- that
//!   path neither narrows a range nor covers the read, so without the flag
//!   pruning would drop it and there would be no path left.
//! * `IGNORE INDEX` is collected separately and applied LAST, to whatever
//!   survived. The table path is never removable: `removeIgnoredPaths` keeps
//!   `path.IsTiKVTablePath()` unconditionally, and its ignore-matching is by
//!   `path.Index`, so `IGNORE INDEX(primary)` is a complete no-op.
//! * `USE INDEX ()` -- the empty list, valid MySQL meaning "use no indexes"
//!   -- is not an ignore. It takes the branch that forces the TABLE path, so
//!   it leaves the table scan (and the point get over the handle, which is
//!   the same path) as the only candidate.
//! * A hint naming an index the table does not have is a statement-level
//!   ERROR, [`DriverError::KeyNotExists`] = 1176, raised BEFORE any path is
//!   costed. It is raised whether or not the cost model would ever have
//!   wanted that index, and for `IGNORE` just as much as for `USE`/`FORCE`.
//!   An INVISIBLE index is not in `publicPaths` and so is exactly this error
//!   too. Only the comment spelling `/*+ use_index(t, x) */` downgrades it to
//!   a warning; that spelling does not reach here.
//! * A `FOR JOIN`/`FOR ORDER BY`/`FOR GROUP BY` qualifier takes the hint out
//!   of scan-path selection entirely (`hint.HintScope != ast.HintForScan ->
//!   continue`), INCLUDING its name validation: `USE INDEX FOR JOIN
//!   (no_such_idx)` is silently inert, not 1176. Captured.
//! * `PRIMARY` names the table path on a table whose primary key is the
//!   handle (Go `getPathByIndexName`'s trailing `isPrimaryIndex` arm).
//! * If everything the hints left is empty, Go appends the table path back
//!   rather than failing to plan. `FORCE INDEX(i) IGNORE INDEX(i)` is that
//!   case, and it reads the table.

use crate::driver::DriverError;
use crate::kv_table::KvTable;
use tidb_ast::{IndexHint, IndexHintKind, IndexHintScope};

/// Which access paths over one table its index hints leave available --
/// Go's `available` slice, in the two states it can be in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AvailablePaths {
    /// `Some(ids)` is Go's restricted `available` after a `USE`/`FORCE`: ONLY
    /// these indexes may be enumerated, and each one is `path.Forced`.
    /// `None` is Go's `available = publicPaths`, every visible index.
    forced_indexes: Option<Vec<i64>>,
    /// Whether the table path -- the full scan, the narrowed handle range,
    /// and the point get that is the same path taken to its limit -- is
    /// available. Only a `USE`/`FORCE` that named no index of this table can
    /// take it away.
    table: bool,
    /// The index ids `IGNORE INDEX` named, applied to whatever survived.
    ignored: Vec<i64>,
}

impl AvailablePaths {
    /// Go's `available = publicPaths` with no hints in play: every path.
    pub(crate) const fn unrestricted() -> Self {
        Self {
            forced_indexes: None,
            table: true,
            ignored: Vec::new(),
        }
    }

    /// Whether an index may still become a candidate path.
    pub(crate) fn allows_index(&self, index_id: i64) -> bool {
        if self.ignored.contains(&index_id) {
            return false;
        }
        match &self.forced_indexes {
            Some(forced) => forced.contains(&index_id),
            None => true,
        }
    }

    /// Go `path.Forced`: the hint named this index, so skyline pruning keeps
    /// it even when it neither narrows a range nor covers the read.
    pub(crate) fn forces_index(&self, index_id: i64) -> bool {
        self.forced_indexes
            .as_ref()
            .is_some_and(|forced| forced.contains(&index_id))
    }

    /// Whether the table path -- full scan, handle range, or point get --
    /// survives. `false` is what makes `FORCE INDEX(idx_b) WHERE a = 2` read
    /// the index instead of doing the point get the cost model would rather
    /// have done.
    pub(crate) const fn allows_table(&self) -> bool {
        self.table
    }

    /// Resolves one table's hints, exactly in Go's order: collect, then
    /// restrict, then remove the ignored, then fall back to the table path.
    ///
    /// The error is Go's `plannererrors.ErrKeyDoesNotExist`, raised on the
    /// first unresolvable name.
    pub(crate) fn resolve(table: &KvTable, hints: &[IndexHint]) -> Result<Self, DriverError> {
        // Go's `hasScanHint` / `hasUseOrForce`. Both are needed: a statement
        // with only `IGNORE INDEX` has a scan hint but no forced set, and
        // must fall through to `available = publicPaths`.
        let mut has_scan_hint = false;
        let mut has_use_or_force = false;
        let mut forced_indexes = Vec::new();
        let mut forced_table = false;
        let mut ignored = Vec::new();
        for hint in hints {
            // Go: `if hint.HintScope != ast.HintForScan { continue }`. The
            // name is not even looked up, so a `FOR JOIN` hint naming a
            // missing index is inert rather than 1176. Captured.
            if hint.scope != IndexHintScope::All {
                continue;
            }
            has_scan_hint = true;
            // `USE INDEX ()`: no names and not an ignore, so Go takes
            // `getTablePath` and forces it. `IGNORE INDEX ()` is a syntax
            // error and never reaches here.
            if hint.indexes.is_empty() && hint.kind != IndexHintKind::Ignore {
                has_use_or_force = true;
                forced_table = true;
            }
            for name in &hint.indexes {
                match resolve_index_name(table, name) {
                    Some(HintedPath::Index(id)) => {
                        if hint.kind == IndexHintKind::Ignore {
                            ignored.push(id);
                        } else {
                            has_use_or_force = true;
                            forced_indexes.push(id);
                        }
                    }
                    // `IGNORE INDEX(primary)` names the table path, which
                    // `removeIgnoredPaths` refuses to remove -- so ignoring
                    // it does nothing at all.
                    Some(HintedPath::Table) => {
                        if hint.kind != IndexHintKind::Ignore {
                            has_use_or_force = true;
                            forced_table = true;
                        }
                    }
                    None => {
                        return Err(DriverError::KeyNotExists {
                            key: name.clone(),
                            table: table.name.clone(),
                        });
                    }
                }
            }
        }
        let (mut forced_indexes, mut table) = if has_scan_hint && has_use_or_force {
            (Some(forced_indexes), forced_table)
        } else {
            // Go: `available = publicPaths`. The ignores below still apply.
            (None, true)
        };
        // Go `removeIgnoredPaths`, which never removes the table path.
        if let Some(forced) = forced_indexes.as_mut() {
            forced.retain(|id| !ignored.contains(id));
            // Go: "If we have got FORCE or USE index hint but got no
            // available index, we have to use table scan."
            if forced.is_empty() {
                table = true;
            }
        }
        Ok(Self {
            forced_indexes,
            table,
            ignored,
        })
    }
}

/// What one hinted name resolved to.
enum HintedPath {
    /// A visible secondary index of the table.
    Index(i64),
    /// The table path itself, which is what `PRIMARY` names on a table whose
    /// primary key IS the row handle.
    Table,
}

/// Go `getPathByIndexName`: an index of the table by name, or the table path
/// when the name is `PRIMARY` and the primary key is the handle.
///
/// Index names are case-insensitive, and an INVISIBLE index is not a path at
/// all -- it is absent from `publicPaths`, which is why naming one is 1176
/// rather than a plan that quietly reads it.
fn resolve_index_name(table: &KvTable, name: &str) -> Option<HintedPath> {
    if let Some(index) = table
        .plan_indexes()
        .find(|index| index.name.eq_ignore_ascii_case(name))
    {
        return Some(HintedPath::Index(index.id));
    }
    if name.eq_ignore_ascii_case("primary") && table.pk_handle_offset().is_some() {
        return Some(HintedPath::Table);
    }
    None
}

/// The hints on a plain single-table `FROM`, resolved against that table.
///
/// Every OTHER `FROM` shape -- a join, a derived table, a view -- resolves to
/// [`AvailablePaths::unrestricted`] here and has its names validated by
/// [`validate_join_index_hints`] instead, so a hint naming a missing index is
/// still 1176 on a table this tier's fast path never reaches.
pub(crate) fn table_ref_hints(
    table_ref: &tidb_ast::TableRef,
    table: &KvTable,
) -> Result<AvailablePaths, DriverError> {
    if table_ref.hints.is_empty() {
        return Ok(AvailablePaths::unrestricted());
    }
    AvailablePaths::resolve(table, &table_ref.hints)
}

/// Raises Go's 1176 for any index hint in a `FROM` clause naming an index its
/// table does not have, over EVERY table of a join rather than only the one
/// the fast path costs.
///
/// Go validates per `DataSource`, so both sides of a join report it; without
/// this walk the error would depend on which table the plan happened to
/// narrow, which is not a rule anyone could rely on.
pub(crate) fn validate_join_index_hints(
    join: &tidb_ast::Join,
    catalog: &crate::driver::Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    validate_join_node(&join.left, catalog, current_db)?;
    match &join.right {
        Some(right) => validate_join_node(right, catalog, current_db),
        None => Ok(()),
    }
}

fn validate_join_node(
    node: &tidb_ast::JoinNode,
    catalog: &crate::driver::Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    match node {
        tidb_ast::JoinNode::Table(table_ref) => {
            if table_ref.hints.is_empty() {
                return Ok(());
            }
            let Ok((database, name)) =
                crate::driver::split_table_path_pub(&table_ref.name, current_db)
            else {
                return Ok(());
            };
            // A name that resolves to nothing, or to something that is not a
            // stored table, is a diagnosis for the resolver above to make.
            let Some(crate::driver::TableEntry::Kv(table)) = catalog.get_in(database, name) else {
                return Ok(());
            };
            AvailablePaths::resolve(table, &table_ref.hints).map(|_| ())
        }
        tidb_ast::JoinNode::Join(join) => validate_join_index_hints(join, catalog, current_db),
        // A derived table's own `FROM` is validated when it is planned.
        tidb_ast::JoinNode::Derived { .. } => Ok(()),
    }
}

/// Go's COMMENT-style index hints (`/*+ use_index(t, idx) */`), which are a
/// genuinely different rule from the `FROM`-clause spelling above even though
/// they name the same paths.
///
/// Go appends them to the very same `indexHints` slice
/// (`getPossibleAccessPaths`, `planbuilder.go:1445`) but remembers the
/// original length, and every refusal below that boundary is downgraded:
///
/// * a name no index of the matched table has is `AppendWarning(1176)` and a
///   `continue`, not the statement-level 1176 the `FROM` spelling raises;
/// * a hint whose TABLE matched nothing in the query block is not reported
///   here at all -- it is reported once, at the end of the block, by
///   `popTableHints` -> `SetHintWarning` (`hint.go:1234`), as 1815. That is
///   `ErrInternal.FastGen`, and `FastGen` REPLACES the class message, which
///   is why the wire text carries no `Internal : ` prefix even though
///   `errno.ErrInternal` is `"Internal : %s"`.
///
/// The matching itself is `HintedIndex.Match`: the hint's table name against
/// the query block's ALIAS when one is written (`FROM t t2` is matched by
/// `use_index(t2, ...)` and NOT by `use_index(t, ...)`), and the hint's
/// database -- defaulted to the current one -- against the table's. Both
/// case-insensitively. A DERIVED table is not a `DataSource` and matches
/// nothing, so `use_index(d, ...)` over `FROM (SELECT ...) d` is 1815.
///
/// # Capture (`rust/difftests/gorun`, verbatim)
///
/// ```text
/// select /*+ use_index(zzz, idx_b) */ * from t where b = 2
///   RS:2|2|2
///   show warnings -> RS:Warning|1815|use_index(test.zzz, idx_b) is inapplicable, check whether the table(test.zzz) exists
/// select /*+ use_index(t, no_such_idx) */ * from t where b = 2
///   RS:2|2|2
///   show warnings -> RS:Warning|1176|Key 'no_such_idx' doesn't exist in table 't'
/// select /*+ USE_INDEX(ZZZ, IdX_B) */ * from t where b = 2
///   show warnings -> RS:Warning|1815|use_index(test.ZZZ, idx_b) is inapplicable, check whether the table(test.ZZZ) exists
/// select /*+ use_index(zzz) */ * from t where b = 2
///   show warnings -> RS:Warning|1815|use_index(test.zzz) is inapplicable, check whether the table(test.zzz) exists
/// select /*+ order_index(zzz, idx_b) */ * from t where b = 2
///   show warnings -> RS:Warning|1815|(test.zzz, idx_b) is inapplicable, check whether the table(test.zzz) exists
/// ```
///
/// The last one is not a transcription slip: Go's `HintedIndex.HintTypeString`
/// switches over only `HintUse`/`HintIgnore`/`HintForce` and returns `""` for
/// `order_index`/`no_order_index`, so the rendered warning really does start
/// at the open paren. It is ported as measured rather than repaired.
pub(crate) fn report_comment_index_hints(
    select: &tidb_ast::SelectStmt,
    catalog: &crate::driver::Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) {
    use tidb_expr::Columns as _;

    for hint in &select.hints {
        let tidb_ast::HintKind::Index { table, indexes, .. } = &hint.kind else {
            continue;
        };
        let Some(type_string) = comment_hint_type_string(&hint.name) else {
            continue;
        };
        let db = table.db_name.as_deref().unwrap_or(current_db);
        match matched_hint_table(select, table, db, catalog, current_db) {
            // Go validates the names against the matched `DataSource`, and a
            // miss below `indexHintsLen` warns instead of failing.
            Some(matched) => {
                for name in indexes {
                    if resolve_index_name(&matched, name).is_none() {
                        let reported = DriverError::KeyNotExists {
                            key: name.clone(),
                            table: matched.name.clone(),
                        }
                        .to_mysql_error();
                        ctx.append_warning(reported.code, &reported.message);
                    }
                }
            }
            None => ctx.append_warning(
                1815,
                &unmatched_hint_warning(type_string, db, &table.name, indexes),
            ),
        }
    }
}

/// Go `HintedIndex.HintTypeString`, plus the membership test that decides
/// whether a hint is in `PlanHints.IndexHintList` at all.
///
/// `None` means the hint is not an index hint of this family and has nothing
/// to do with 1815 here (`NO_INDEX_LOOKUP_PUSHDOWN` collects into a separate
/// `HintedTable` list with its own, differently-worded warning).
fn comment_hint_type_string(name: &str) -> Option<&'static str> {
    Some(match name {
        "USE_INDEX" => "use_index",
        "IGNORE_INDEX" => "ignore_index",
        "FORCE_INDEX" => "force_index",
        "USE_INDEX_MERGE" => "use_index_merge",
        "INDEX_LOOKUP_PUSHDOWN" => "index_lookup_pushdown",
        // Go's own switch has no arm for these two, so the format string
        // renders an empty hint name. Captured.
        "ORDER_INDEX" | "NO_ORDER_INDEX" => "",
        _ => return None,
    })
}

/// Go `collectUnmatchedIndexHintWarning`'s format string, including its own
/// asymmetry: the table name keeps the case it was WRITTEN in (`CIStr`'s
/// `%s` is the original), while each index name is lowercased (`.L`).
fn unmatched_hint_warning(type_string: &str, db: &str, table: &str, indexes: &[String]) -> String {
    let mut index_list = String::new();
    for name in indexes {
        index_list.push_str(", ");
        index_list.push_str(&name.to_lowercase());
    }
    format!(
        "{type_string}({db}.{table}{index_list}) is inapplicable, \
         check whether the table({db}.{table}) exists"
    )
}

/// Go `HintedIndex.Match` over the query block's own `DataSource`s: the
/// stored table one comment hint names, or `None` when it names none of them.
fn matched_hint_table(
    select: &tidb_ast::SelectStmt,
    hinted: &tidb_ast::HintTable,
    hinted_db: &str,
    catalog: &crate::driver::Catalog,
    current_db: &str,
) -> Option<KvTable> {
    let join = select.from.as_ref()?;
    let mut found = None;
    visit_join_tables(join, &mut |table_ref| {
        if found.is_some() {
            return;
        }
        // Go matches the hint against the `DataSource`'s reported name, which
        // is the alias whenever one is written.
        let referenced = table_ref
            .alias
            .as_deref()
            .or_else(|| table_ref.name.last().map(String::as_str));
        if !referenced.is_some_and(|name| name.eq_ignore_ascii_case(&hinted.name)) {
            return;
        }
        let Ok((database, name)) = crate::driver::split_table_path_pub(&table_ref.name, current_db)
        else {
            return;
        };
        if !database.eq_ignore_ascii_case(hinted_db) {
            return;
        }
        if let Some(crate::driver::TableEntry::Kv(table)) = catalog.get_in(database, name) {
            found = Some(table.clone());
        }
    });
    found
}

/// Every plain table reference of one `FROM`, in written order. A derived
/// table is deliberately NOT descended into: its own query block pops its own
/// hints, and the derived table itself is not a `DataSource` a hint can name.
fn visit_join_tables(join: &tidb_ast::Join, visit: &mut impl FnMut(&tidb_ast::TableRef)) {
    visit_join_node(&join.left, visit);
    if let Some(right) = &join.right {
        visit_join_node(right, visit);
    }
}

fn visit_join_node(node: &tidb_ast::JoinNode, visit: &mut impl FnMut(&tidb_ast::TableRef)) {
    match node {
        tidb_ast::JoinNode::Table(table_ref) => visit(table_ref),
        tidb_ast::JoinNode::Join(join) => visit_join_tables(join, visit),
        tidb_ast::JoinNode::Derived { .. } => {}
    }
}
