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

//! SEED port of the dependency-closed decisions in `pkg/executor/compiler.go`.
//!
//! Go's `Compiler.Compile` is a session-bound orchestration: it runs
//! `plannercore.Preprocess`, `planner.Optimize`, `plannercore.GetPreparedStmt`,
//! and `sessiontxn.AdviseOptimizeWithPlanAndThenWarmUp` against a live
//! `sessionctx.Context`. None of those seams exist in Rust yet, so the
//! orchestration itself is not portable here.
//!
//! What is dependency-closed and ported below:
//!
//! * `needLowerPriority` / `isPhysicalPlanNeedLowerPriority`
//!   (`pkg/executor/compiler.go:169-224`) — a pure walk over a plan tree
//!   deciding whether the coprocessor priority should be lowered, driven only
//!   by estimated row counts and a threshold.
//! * `CountStmtNode`'s statement-label bucketing decisions
//!   (`pkg/executor/compiler.go:226-596`) — the pure parts: whether the
//!   statement counts as "internal", and which DB-label a statement carries.
//!
//! Everything else is named as a `// boundary:` narrowing.

/// Threshold on the estimated output row count above which a query's
/// coprocessor priority is lowered.
///
/// Go: `pkg/executor/compiler.go` reads
/// `config.GetGlobalConfig().Log.ExpensiveThreshold`; the tree walk compares
/// `p.StatsCount()` against `float64(threshold)`.
pub type ExpensiveThreshold = u32;

/// A minimal plan-tree shape carrying only what `needLowerPriority` reads.
///
/// Go's walk is over `base.Plan` / `base.PhysicalPlan`. The Rust plan layer
/// (`tidb-planner`) is not wired to this crate, so this SEED models the tree by
/// the two facts the decision consumes: the node's estimated row count and its
/// children.
#[derive(Clone, Debug, Default)]
pub struct PriorityPlanNode {
    /// Estimated output row count. Go: `base.PhysicalPlan.StatsCount()`.
    pub stats_count: f64,
    /// Physical children. Go: `p.Children()`.
    pub children: Vec<PriorityPlanNode>,
}

impl PriorityPlanNode {
    /// Builds a leaf node with the given estimated row count.
    #[must_use]
    pub fn leaf(stats_count: f64) -> Self {
        Self {
            stats_count,
            children: Vec::new(),
        }
    }

    /// Builds a node with the given estimated row count and children.
    #[must_use]
    pub fn with_children(stats_count: f64, children: Vec<Self>) -> Self {
        Self {
            stats_count,
            children,
        }
    }
}

/// The plan shapes `needLowerPriority` distinguishes.
///
/// Go: `pkg/executor/compiler.go:169-190`. `Execute` recurses into its inner
/// plan; the DML wrappers descend into `SelectPlan` when it is present; a
/// `base.PhysicalPlan` is walked directly; anything else is `false`.
#[derive(Clone, Debug)]
pub enum PriorityPlan {
    /// Go: `case base.PhysicalPlan`.
    Physical(PriorityPlanNode),
    /// Go: `case *plannercore.Execute` — recurses into `x.Plan`.
    Execute(Box<PriorityPlan>),
    /// Go: `*physicalop.Insert` / `*physicalop.Delete` / `*physicalop.Update`,
    /// each of which checks `x.SelectPlan != nil` first.
    Dml(Option<PriorityPlanNode>),
    /// Any other plan kind; Go falls through to the trailing `return false`.
    Other,
}

/// Whether the query's execution priority should be lowered.
///
/// Go: `needLowerPriority` at `pkg/executor/compiler.go:169`.
#[must_use]
pub fn need_lower_priority(plan: &PriorityPlan, threshold: ExpensiveThreshold) -> bool {
    match plan {
        PriorityPlan::Physical(node) => is_physical_plan_need_lower_priority(node, threshold),
        PriorityPlan::Execute(inner) => need_lower_priority(inner, threshold),
        PriorityPlan::Dml(select_plan) => select_plan
            .as_ref()
            .is_some_and(|node| is_physical_plan_need_lower_priority(node, threshold)),
        PriorityPlan::Other => false,
    }
}

/// Whether any operator in the physical subtree estimates more rows than the
/// expensive-query threshold.
///
/// Go: `isPhysicalPlanNeedLowerPriority` at `pkg/executor/compiler.go:200`.
/// The Go code reads the threshold from
/// `config.GetGlobalConfig().Log.ExpensiveThreshold` inside the function; here
/// it is passed in, since global config is not a seam this crate owns.
///
/// Go compares `int64(p.StatsCount()) > expensiveThreshold`, i.e. the estimate
/// is truncated toward zero *before* the comparison; a plan estimating
/// `threshold + 0.5` rows therefore does not trip it. That truncation is
/// preserved here.
#[must_use]
pub fn is_physical_plan_need_lower_priority(
    plan: &PriorityPlanNode,
    threshold: ExpensiveThreshold,
) -> bool {
    if truncate_stats_count(plan.stats_count) > i64::from(threshold) {
        return true;
    }
    plan.children
        .iter()
        .any(|child| is_physical_plan_need_lower_priority(child, threshold))
}

/// Go's `int64(f)` conversion of a `float64` estimate: truncation toward zero,
/// with out-of-range and NaN inputs left to saturating behaviour rather than
/// Go's implementation-defined result.
fn truncate_stats_count(stats_count: f64) -> i64 {
    stats_count as i64
}

/// A metric row `CountStmtNode` asks to be incremented.
///
/// Go increments Prometheus counters directly
/// (`pkg/executor/compiler.go:210-231`); this crate has no metrics registry
/// seam, so the routing decision is returned as data instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StmtNodeCount {
    /// Go: `metrics.DbStmtNodeCounter.WithLabelValues(dbLabel, typeLabel)`.
    ByDb {
        /// The statement's database label.
        db_label: String,
        /// The statement type label. Go: `stmtctx.GetStmtLabel`.
        type_label: String,
    },
    /// Go: `metrics.StmtNodeCounter.WithLabelValues(typeLabel, dbLabel, resourceGroup)`.
    /// `db_label` is empty in the no-DB-labelling branch.
    ByType {
        /// The statement type label.
        type_label: String,
        /// The statement's database label, empty when DB labelling is off.
        db_label: String,
        /// The statement's resource-group name.
        resource_group: String,
    },
}

/// The two independent config switches `CountStmtNode` consults.
///
/// Go: `config.GetGlobalConfig().Status.RecordQPSbyDB` and
/// `config.GetGlobalConfig().Status.RecordDBLabel`.
#[derive(Clone, Copy, Debug, Default)]
pub struct StmtCountConfig {
    /// Go: `Status.RecordQPSbyDB`. Checked first; wins over `record_db_label`.
    pub record_qps_by_db: bool,
    /// Go: `Status.RecordDBLabel`.
    pub record_db_label: bool,
}

/// Decides which statement-node counters to increment.
///
/// Go: `CountStmtNode` at `pkg/executor/compiler.go:210`.
///
/// Two behaviours worth naming, both preserved:
///
/// * restricted (internal) SQL is not counted at all;
/// * when either DB-labelling switch is on but the statement resolves to no
///   database labels, Go's `for dbLabel := range dbLabels` loop body never
///   runs, so *no* counter is incremented — the plain `ByType` fallback is
///   reached only when both switches are off.
#[must_use]
pub fn count_stmt_node(
    in_restricted_sql: bool,
    type_label: &str,
    db_labels: &[String],
    resource_group: &str,
    config: StmtCountConfig,
) -> Vec<StmtNodeCount> {
    if in_restricted_sql {
        return Vec::new();
    }
    if config.record_qps_by_db {
        return db_labels
            .iter()
            .map(|db_label| StmtNodeCount::ByDb {
                db_label: db_label.clone(),
                type_label: type_label.to_owned(),
            })
            .collect();
    }
    if config.record_db_label {
        return db_labels
            .iter()
            .map(|db_label| StmtNodeCount::ByType {
                type_label: type_label.to_owned(),
                db_label: db_label.clone(),
                resource_group: resource_group.to_owned(),
            })
            .collect();
    }
    vec![StmtNodeCount::ByType {
        type_label: type_label.to_owned(),
        db_label: String::new(),
        resource_group: resource_group.to_owned(),
    }]
}

/// The `ast.ResultSetNode` shapes `getDbFromResultNode` distinguishes.
///
/// Go: `pkg/executor/compiler.go:560-596`. Only four cases carry behaviour;
/// every other result-set node yields no labels.
#[derive(Clone, Debug)]
pub enum ResultSetNode {
    /// Go: `*ast.TableSource` — recurses into `x.Source`.
    TableSource(Box<ResultSetNode>),
    /// Go: `*ast.SelectStmt` — recurses into `x.From.TableRefs` when `From` is
    /// non-nil, and yields nothing otherwise.
    Select(Option<Box<ResultSetNode>>),
    /// Go: `*ast.TableName` — the resolved `DBInfo.Name.L`, or nothing when the
    /// resolve context has no entry for this table name.
    TableName(Option<String>),
    /// Go: `*ast.Join` — left then right, each skipped when nil.
    Join {
        /// Go: `x.Left`.
        left: Option<Box<ResultSetNode>>,
        /// Go: `x.Right`.
        right: Option<Box<ResultSetNode>>,
    },
    /// Any other result-set node; Go falls through the switch.
    Other,
}

/// Collects the database labels reachable from a result-set node.
///
/// Go: `getDbFromResultNode` at `pkg/executor/compiler.go:560`. The Go comment
/// notes the result "may have duplicate db name"; duplicates are preserved
/// here, and de-duplication happens only in the caller's label set.
///
/// Narrowing: Go resolves `*ast.TableName` through
/// `resolveCtx.GetTableName(x)`; that resolution is pre-applied into
/// `ResultSetNode::TableName`, since `resolve.Context` has no Rust seam.
#[must_use]
pub fn get_db_from_result_node(node: Option<&ResultSetNode>) -> Vec<String> {
    let mut labels = Vec::new();
    collect_db_from_result_node(node, &mut labels);
    labels
}

fn collect_db_from_result_node(node: Option<&ResultSetNode>, labels: &mut Vec<String>) {
    let Some(node) = node else {
        return;
    };
    match node {
        ResultSetNode::TableSource(source) => {
            collect_db_from_result_node(Some(source), labels);
        }
        ResultSetNode::Select(from) => {
            collect_db_from_result_node(from.as_deref(), labels);
        }
        ResultSetNode::TableName(db) => {
            if let Some(db) = db {
                labels.push(db.clone());
            }
        }
        ResultSetNode::Join { left, right } => {
            collect_db_from_result_node(left.as_deref(), labels);
            collect_db_from_result_node(right.as_deref(), labels);
        }
        ResultSetNode::Other => {}
    }
}

// boundary: `getStmtDbLabel` (`pkg/executor/compiler.go:233-558`) is a ~30-arm
// type switch over concrete `ast` statement nodes (`*ast.AlterTableStmt`,
// `*ast.CreateIndexStmt`, `*ast.InsertStmt`, `*ast.RenameTableStmt`,
// `*ast.CreateBindingStmt`, ...). Porting it requires the full statement AST,
// which this crate does not own. `get_db_from_result_node` above is the
// dependency-closed half it delegates to.

// boundary: `stmtctx.GetStmtLabel` supplies the `type_label` consumed by
// `count_stmt_node`; it too is an AST type switch and is not ported here.

// boundary: `Compiler.Compile` (`pkg/executor/compiler.go:50`) is not ported.
// It requires `sessionctx.Context`, `plannercore.Preprocess`,
// `resolve.NewNodeW`, `sessiontxn.GetTxnManager`,
// `plannercore.GetPreparedStmt`, `planner.Optimize`,
// `plannercore.IsSafeToReusePointGetExecutor`, and
// `sessiontxn.AdviseOptimizeWithPlanAndThenWarmUp` — none of which exist in
// Rust. Wiring the plan layer to the executor is a separate later batch.

// boundary: the panic-recovery filter in `Compile`'s deferred closure
// (`pkg/executor/compiler.go:54-68`) re-panics unless the recovered error is
// one of `exeerrors.ErrMemoryExceedForQuery`,
// `exeerrors.ErrMemoryExceedForInstance`, `exeerrors.ErrQueryInterrupted`, or
// `exeerrors.ErrMaxExecTimeExceeded`. Rust has no equivalent recover seam here.

// boundary: the plan-digest trace event (`pkg/executor/compiler.go:126-143`)
// needs `traceevent.IsEnabled`, `stmtCtx.GetPlanDigest`,
// `stmtctx.GetStmtLabel`, and `redact.String`.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_plan_below_threshold_keeps_priority() {
        let plan = PriorityPlan::Physical(PriorityPlanNode::leaf(100.0));
        assert!(!need_lower_priority(&plan, 10_000));
    }

    #[test]
    fn threshold_comparison_is_strictly_greater() {
        let plan = PriorityPlanNode::leaf(10_000.0);
        assert!(!is_physical_plan_need_lower_priority(&plan, 10_000));
        let plan = PriorityPlanNode::leaf(10_001.0);
        assert!(is_physical_plan_need_lower_priority(&plan, 10_000));
    }

    #[test]
    fn fractional_estimate_is_truncated_before_comparison() {
        // Go: `int64(p.StatsCount()) > expensiveThreshold`.
        let plan = PriorityPlanNode::leaf(10_000.9);
        assert!(!is_physical_plan_need_lower_priority(&plan, 10_000));
    }

    #[test]
    fn any_descendant_over_threshold_lowers_priority() {
        let plan = PriorityPlanNode::with_children(
            1.0,
            vec![PriorityPlanNode::with_children(
                1.0,
                vec![PriorityPlanNode::leaf(50_000.0)],
            )],
        );
        assert!(is_physical_plan_need_lower_priority(&plan, 10_000));
    }

    #[test]
    fn execute_recurses_into_inner_plan() {
        let plan = PriorityPlan::Execute(Box::new(PriorityPlan::Physical(PriorityPlanNode::leaf(
            50_000.0,
        ))));
        assert!(need_lower_priority(&plan, 10_000));
    }

    #[test]
    fn dml_without_select_plan_never_lowers() {
        assert!(!need_lower_priority(&PriorityPlan::Dml(None), 0));
    }

    #[test]
    fn dml_with_select_plan_walks_it() {
        let plan = PriorityPlan::Dml(Some(PriorityPlanNode::leaf(50_000.0)));
        assert!(need_lower_priority(&plan, 10_000));
    }

    #[test]
    fn unknown_plan_kind_never_lowers() {
        assert!(!need_lower_priority(&PriorityPlan::Other, 0));
    }

    fn db(name: &str) -> Vec<String> {
        vec![name.to_owned()]
    }

    #[test]
    fn restricted_sql_is_never_counted() {
        let counts = count_stmt_node(
            true,
            "Select",
            &db("test"),
            "default",
            StmtCountConfig {
                record_qps_by_db: true,
                record_db_label: true,
            },
        );
        assert!(counts.is_empty());
    }

    #[test]
    fn default_config_counts_by_type_with_empty_db_label() {
        let counts = count_stmt_node(
            false,
            "Select",
            &db("test"),
            "rg1",
            StmtCountConfig::default(),
        );
        assert_eq!(
            counts,
            vec![StmtNodeCount::ByType {
                type_label: "Select".to_owned(),
                db_label: String::new(),
                resource_group: "rg1".to_owned(),
            }]
        );
    }

    #[test]
    fn record_qps_by_db_wins_over_record_db_label() {
        let counts = count_stmt_node(
            false,
            "Insert",
            &db("test"),
            "rg1",
            StmtCountConfig {
                record_qps_by_db: true,
                record_db_label: true,
            },
        );
        assert_eq!(
            counts,
            vec![StmtNodeCount::ByDb {
                db_label: "test".to_owned(),
                type_label: "Insert".to_owned(),
            }]
        );
    }

    #[test]
    fn record_db_label_carries_resource_group() {
        let counts = count_stmt_node(
            false,
            "Update",
            &db("test"),
            "rg1",
            StmtCountConfig {
                record_qps_by_db: false,
                record_db_label: true,
            },
        );
        assert_eq!(
            counts,
            vec![StmtNodeCount::ByType {
                type_label: "Update".to_owned(),
                db_label: "test".to_owned(),
                resource_group: "rg1".to_owned(),
            }]
        );
    }

    #[test]
    fn db_labelling_with_no_labels_emits_nothing() {
        // Go's `for dbLabel := range dbLabels` body never runs, so neither
        // counter is touched -- the plain fallback is not reached.
        for config in [
            StmtCountConfig {
                record_qps_by_db: true,
                record_db_label: false,
            },
            StmtCountConfig {
                record_qps_by_db: false,
                record_db_label: true,
            },
        ] {
            assert!(count_stmt_node(false, "Select", &[], "rg1", config).is_empty());
        }
    }

    #[test]
    fn result_node_walk_descends_table_source_and_select() {
        let node = ResultSetNode::TableSource(Box::new(ResultSetNode::Select(Some(Box::new(
            ResultSetNode::TableName(Some("test".to_owned())),
        )))));
        assert_eq!(get_db_from_result_node(Some(&node)), vec!["test"]);
    }

    #[test]
    fn select_without_from_yields_nothing() {
        assert!(get_db_from_result_node(Some(&ResultSetNode::Select(None))).is_empty());
    }

    #[test]
    fn nil_result_node_yields_nothing() {
        assert!(get_db_from_result_node(None).is_empty());
    }

    #[test]
    fn unresolved_table_name_yields_nothing() {
        assert!(get_db_from_result_node(Some(&ResultSetNode::TableName(None))).is_empty());
    }

    #[test]
    fn join_keeps_left_right_order_and_duplicates() {
        let node = ResultSetNode::Join {
            left: Some(Box::new(ResultSetNode::TableName(Some("a".to_owned())))),
            right: Some(Box::new(ResultSetNode::Join {
                left: Some(Box::new(ResultSetNode::TableName(Some("b".to_owned())))),
                right: Some(Box::new(ResultSetNode::TableName(Some("a".to_owned())))),
            })),
        };
        assert_eq!(get_db_from_result_node(Some(&node)), vec!["a", "b", "a"]);
    }

    #[test]
    fn join_with_missing_side_skips_it() {
        let node = ResultSetNode::Join {
            left: None,
            right: Some(Box::new(ResultSetNode::TableName(Some("b".to_owned())))),
        };
        assert_eq!(get_db_from_result_node(Some(&node)), vec!["b"]);
    }
}
