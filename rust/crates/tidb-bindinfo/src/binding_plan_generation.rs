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

//! Go `pkg/bindinfo/binding_plan_generation.go`: `EXPLAIN EXPLORE`'s new
//! plan candidates, found by a breadth-first search over optimizer
//! variables, fix controls, leading-2 join orders, index hints and
//! `NO_DECORRELATE` hints.

use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use tidb_ast::{
    Expr, Hint, HintKind, HintTable, Join, JoinNode, QueryStmt, SelectStmt,
    SetOprTermBody, Stmt, TableRef, Visitable, Visitor,
};
use tidb_driver_error::DriverError;
use tidb_vardef::defaults as def;
use tidb_vardef::tidb_vars as vars;

use crate::binding::Binding;
use crate::binding_auto::{BindingPlanInfo, PlanExploreSession, PlanExploreSessionPool};

/// Go `Binding.Source` of a generated plan candidate.
pub const SOURCE_GENERATED: &str = "generated";

fn plain_error(message: impl Into<String>) -> DriverError {
    DriverError::Mysql(tidb_terror::MysqlError::unknown(message.into()))
}

/// Go `planGenerator.Generate`: new plan candidates for a SELECT, made by
/// adjusting optimizer variables and fixes.
///
/// # Errors
///
/// A plan generation error.
pub fn generate(
    pool: &dyn PlanExploreSessionPool,
    default_schema: &str,
    sql: &str,
    charset: &str,
    collation: &str,
) -> Result<Vec<BindingPlanInfo>, DriverError> {
    // Only SQL starting with SELECT is supported for now.
    let sql = sql.trim();
    const PREFIX: &str = "SELECT";
    if sql.len() < PREFIX.len() || !sql.as_bytes()[..PREFIX.len()].eq_ignore_ascii_case(PREFIX.as_bytes()) {
        return Ok(Vec::new()); // not a SELECT statement
    }
    let mut plans = Vec::new();
    pool.call_with_sctx(&mut |sctx| {
        let gened_plans = generate_plan_with_sctx(sctx, default_schema, sql, charset, collation)?;
        plans = Vec::with_capacity(gened_plans.len());
        for gened_plan in gened_plans {
            let binding_sql = format!(
                "{} /*+ {} */ {}",
                &sql[..PREFIX.len()],
                gened_plan.plan_hints,
                &sql[PREFIX.len()..]
            );
            let mut binding = Binding {
                original_sql: sql.to_owned(),
                bind_sql: binding_sql,
                db: default_schema.to_owned(),
                source: SOURCE_GENERATED,
                plan_digest: gened_plan.plan_digest.clone(),
                ..Binding::default()
            };
            prepare_hints(sctx, &mut binding)?;
            plans.push(BindingPlanInfo {
                binding,
                plan: gened_plan.plan_text(),
                ..BindingPlanInfo::default()
            });
        }
        Ok(())
    })?;
    Ok(plans)
}

/// Go `prepareHints` (`binding.go`) for a binding built in code: its hints,
/// tables and digests from its bind SQL, which must still plan.
///
/// # Errors
///
/// The bind SQL does not parse, or does not plan.
pub fn prepare_hints(sctx: &mut dyn PlanExploreSession, binding: &mut Binding) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse(&binding.bind_sql).map_err(|error| plain_error(error.message))?;
    let table_names = crate::binding::collect_table_names(&stmt);
    let is_cross_db = table_names.iter().any(|(schema, _)| schema == "*");
    // A cross-database binding uses '*' as its database.
    let db_name = if is_cross_db { "*" } else { binding.db.as_str() };
    let (hints_set, stmt, warns) =
        tidb_hint::parse_hints_set(&binding.bind_sql, &binding.charset, &binding.collation, db_name)
            .map_err(plain_error)?;
    if !is_cross_db && !has_param(&stmt) {
        sctx.check_binding_validation(&binding.bind_sql)?;
    }
    // A totally invalid hint escalates its warning to an error.
    if hints_set.restore().is_empty() {
        if let Some(warn) = warns.into_iter().next() {
            return Err(plain_error(warn));
        }
    }
    binding.hints = hints_set;
    binding.no_db_digest = crate::binding::no_db_digest(&stmt);
    binding.table_names = table_names;
    Ok(())
}

/// Go `hasParam`: whether the statement holds a `?` parameter marker.
#[must_use]
pub fn has_param(stmt: &Stmt) -> bool {
    struct ParamChecker(bool);
    impl Visitor for ParamChecker {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if matches!(node.downcast_ref::<Expr>(), Some(Expr::ParamMarker { .. })) {
                self.0 = true;
                return true;
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }
    let mut checker = ParamChecker(false);
    stmt.clone().accept(&mut checker);
    checker.0
}

/// Go `tableName`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TableName {
    schema: String,
    name: String,
    alias: String,
}

impl TableName {
    /// Go `HintName`.
    fn hint_name(&self) -> &str {
        if self.alias.is_empty() {
            &self.name
        } else {
            &self.alias
        }
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.hint_name())
    }
}

/// Go `indexHint`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct IndexHint {
    table: TableName,
    index: String,
}

impl std::fmt::Display for IndexHint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.table, self.index)
    }
}

/// Go `genedPlan`.
struct GenedPlan {
    plan_digest: String,
    plan_hints: String,
    plan_text: Vec<Vec<String>>,
}

impl GenedPlan {
    /// Go `PlanText`: the rows tab-separated, one per line.
    fn plan_text(&self) -> String {
        self.plan_text.iter().map(|row| row.join("\t")).collect::<Vec<_>>().join("\n")
    }
}

/// A variable's value in a [`State`]: Go keeps them as `any`, a float64
/// or a bool.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OptVarValue {
    /// A float64 variable.
    Float(f64),
    /// A boolean variable.
    Bool(bool),
}

impl std::fmt::Display for OptVarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Only 4 decimal digits, which is enough for optimizer tuning.
            OptVarValue::Float(value) => write!(f, "{value:.4}"),
            OptVarValue::Bool(value) => write!(f, "{value}"),
        }
    }
}

/// Go `state`: one setting of the optimizer variables, fixes and hints.
#[derive(Clone, Debug)]
struct State {
    leading2: [Option<TableName>; 2],
    index_hints: Vec<Option<IndexHint>>,
    no_decorrelate_qb: String,
    var_names: Vec<String>,
    var_values: Vec<OptVarValue>,
    fix_ids: Vec<u64>,
    fix_values: Vec<String>,
}

impl State {
    /// Go `Encode`.
    fn encode(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        parts.extend(self.leading2.iter().flatten().map(ToString::to_string));
        parts.extend(self.index_hints.iter().flatten().map(ToString::to_string));
        if !self.no_decorrelate_qb.is_empty() {
            parts.push(format!("no_decorrelate@{}", self.no_decorrelate_qb));
        }
        parts.extend(self.var_values.iter().map(ToString::to_string));
        parts.extend(self.fix_values.iter().cloned());
        parts.join(",")
    }

    /// Go `newStateWithLeading2`.
    fn with_leading2(&self, leading2: [Option<TableName>; 2]) -> State {
        State {
            leading2,
            ..self.clone()
        }
    }

    /// Go `newStateWithIndexHint`.
    fn with_index_hint(&self, table_idx: usize, hint: Option<IndexHint>) -> State {
        let mut state = self.clone();
        if table_idx < state.index_hints.len() {
            state.index_hints[table_idx] = hint;
        }
        state
    }

    /// Go `newStateWithNoDecorrelateQB`.
    fn with_no_decorrelate_qb(&self, qb_name: &str) -> State {
        State {
            no_decorrelate_qb: qb_name.to_owned(),
            ..self.clone()
        }
    }

    /// Go `newStateWithNewVar`.
    fn with_new_var(&self, var_name: &str, var_val: OptVarValue) -> State {
        let mut state = self.clone();
        if let Some(i) = state.var_names.iter().position(|name| name == var_name) {
            state.var_values[i] = var_val;
        }
        state
    }

    /// Go `newStateWithNewFix`.
    fn with_new_fix(&self, fix_id: u64, fix_val: String) -> State {
        let mut state = self.clone();
        if let Some(i) = state.fix_ids.iter().position(|id| *id == fix_id) {
            state.fix_values[i] = fix_val;
        }
        state
    }
}

/// Go `generatePlanWithSCtx`.
fn generate_plan_with_sctx(
    sctx: &mut dyn PlanExploreSession,
    default_schema: &str,
    sql: &str,
    _charset: &str,
    _collation: &str,
) -> Result<Vec<GenedPlan>, DriverError> {
    let stmt = tidb_parser::parse(sql).map_err(|error| plain_error(error.message))?;
    sctx.set_current_db(default_schema);
    sctx.set_cost_model_version(2); // cost factors only work on cost model v2
    let (vars, fixes) = sctx.record_relevant_opt_vars_and_fixes(&stmt)?;
    let table_names = extract_select_table_names(default_schema, &stmt);
    // Enumerate all possible leading-2 table pairs.
    let mut possible_leading2 = Vec::with_capacity(8);
    for i in 0..table_names.len() {
        for j in 0..table_names.len() {
            if i == j {
                continue;
            }
            possible_leading2.push([Some(table_names[i].clone()), Some(table_names[j].clone())]);
        }
    }
    let index_hint_options = extract_select_index_hints(sctx, default_schema, &stmt);
    let possible_no_decorrelate_qbs = extract_no_decorrelate_qbs(&stmt);
    breadth_first_plan_search(
        sctx,
        sql,
        &vars,
        &fixes,
        &possible_leading2,
        &index_hint_options,
        &possible_no_decorrelate_qbs,
    )
}

/// Go `breadthFirstPlanSearch`.
fn breadth_first_plan_search(
    sctx: &mut dyn PlanExploreSession,
    sql: &str,
    vars: &[String],
    fixes: &[u64],
    possible_leading2: &[[Option<TableName>; 2]],
    index_hint_options: &[Vec<Option<IndexHint>>],
    possible_no_decorrelate_qbs: &[String],
) -> Result<Vec<GenedPlan>, DriverError> {
    let mut visited_states: HashSet<String> = HashSet::new();
    let mut visited_plans: HashMap<String, GenedPlan> = HashMap::new();
    let mut state_list: VecDeque<State> = VecDeque::new();

    // The start state: no leading hint and the default values of every
    // variable and fix control.
    let start_state = get_start_state(vars, fixes, index_hint_options.len())?;
    visited_states.insert(start_state.encode());
    state_list.push_back(start_state);

    let push = |state: State, visited_states: &mut HashSet<String>, state_list: &mut VecDeque<State>| {
        if visited_states.insert(state.encode()) {
            state_list.push_back(state);
        }
    };
    let (max_plans, max_explore_state) = (30, 10000);
    while visited_plans.len() < max_plans && visited_states.len() < max_explore_state {
        let Some(curr_state) = state_list.pop_front() else {
            break;
        };
        let plan = gen_plan_under_state(sctx, sql, &curr_state)?;
        visited_plans.insert(plan.plan_digest.clone(), plan);

        // In each step, adjust one variable, fix or join order.
        for qb_name in possible_no_decorrelate_qbs {
            push(curr_state.with_no_decorrelate_qb(qb_name), &mut visited_states, &mut state_list);
        }
        for leading2 in possible_leading2 {
            push(curr_state.with_leading2(leading2.clone()), &mut visited_states, &mut state_list);
        }
        for (table_idx, options) in index_hint_options.iter().enumerate() {
            for index_hint in options {
                push(curr_state.with_index_hint(table_idx, index_hint.clone()), &mut visited_states, &mut state_list);
            }
        }
        for (i, var_name) in vars.iter().enumerate() {
            let new_var_val = adjust_var(var_name, curr_state.var_values[i])?;
            push(curr_state.with_new_var(var_name, new_var_val), &mut visited_states, &mut state_list);
        }
        for (i, fix_id) in fixes.iter().enumerate() {
            let new_fix_val = adjust_fix(*fix_id, &curr_state.fix_values[i])?;
            push(curr_state.with_new_fix(*fix_id, new_fix_val), &mut visited_states, &mut state_list);
        }
    }

    // Sorted by digest to make the result stable.
    let mut plans: Vec<GenedPlan> = visited_plans.into_values().collect();
    plans.sort_by(|left, right| left.plan_digest.cmp(&right.plan_digest));
    Ok(plans)
}

/// Go `genPlanUnderState`: a plan generated under `state`. The SQL is parsed
/// again for every state, as Go does.
fn gen_plan_under_state(
    sctx: &mut dyn PlanExploreSession,
    sql: &str,
    state: &State,
) -> Result<GenedPlan, DriverError> {
    let mut stmt = tidb_parser::parse(sql).map_err(|error| plain_error(error.message))?;
    for (var_name, value) in state.var_names.iter().zip(&state.var_values) {
        if !GEN_PLAN_VARS.contains(&var_name.as_str()) {
            return Err(plain_error(format!("unsupported variable {var_name} in plan generation")));
        }
        sctx.set_opt_var(var_name, *value)?;
    }

    let fix_control = state
        .fix_ids
        .iter()
        .zip(&state.fix_values)
        .map(|(fix_id, fix_val)| format!("{fix_id}:{fix_val}"))
        .collect::<Vec<_>>()
        .join(",");
    sctx.set_optimizer_fix_control(&fix_control)?;

    if let Stmt::Query(query) = &mut stmt {
        if let QueryStmt::Select(select) = query.as_mut() {
            let has_index_hint = state.index_hints.iter().any(Option::is_some);
            if let [Some(first), Some(second)] = &state.leading2 {
                let table = |table: &TableName| HintTable {
                    db_name: Some(table.schema.clone()),
                    name: table.hint_name().to_owned(),
                    qb_name: None,
                    partitions: Vec::new(),
                };
                // Go builds the hint with `Tables` only and no `HintData`
                // leading list, which this table-list shape is.
                select.hints.push(Hint {
                    name: "LEADING".to_owned(),
                    written_name: None,
                    kind: HintKind::Tables {
                        qb_name: None,
                        tables: vec![table(first), table(second)],
                    },
                });
            }
            if has_index_hint || state.leading2.iter().all(Option::is_some) {
                for index_hint in state.index_hints.iter().flatten() {
                    select.hints.push(Hint {
                        name: "USE_INDEX".to_owned(),
                        written_name: None,
                        kind: HintKind::Index {
                            qb_name: None,
                            table: HintTable {
                                db_name: Some(index_hint.table.schema.clone()),
                                name: index_hint.table.hint_name().to_owned(),
                                qb_name: None,
                                partitions: Vec::new(),
                            },
                            indexes: vec![index_hint.index.clone()],
                        },
                    });
                }
            }
            if !state.no_decorrelate_qb.is_empty() {
                select.hints.push(Hint {
                    name: "NO_DECORRELATE".to_owned(),
                    written_name: None,
                    kind: HintKind::Nullary {
                        qb_name: Some(state.no_decorrelate_qb.clone()),
                    },
                });
            }
        }
    }

    let (plan_digest, plan_hints, plan_text) = sctx.gen_brief_plan(&stmt)?;
    Ok(GenedPlan {
        plan_digest,
        plan_hints,
        plan_text,
    })
}

/// The variables `genPlanUnderState` sets.
const GEN_PLAN_VARS: &[&str] = &[
    vars::TIDB_OPT_INDEX_SCAN_COST_FACTOR,
    vars::TIDB_OPT_INDEX_READER_COST_FACTOR,
    vars::TIDB_OPT_TABLE_READER_COST_FACTOR,
    vars::TIDB_OPT_TABLE_FULL_SCAN_COST_FACTOR,
    vars::TIDB_OPT_TABLE_RANGE_SCAN_COST_FACTOR,
    vars::TIDB_OPT_TABLE_ROW_ID_SCAN_COST_FACTOR,
    vars::TIDB_OPT_TABLE_TIFLASH_SCAN_COST_FACTOR,
    vars::TIDB_OPT_INDEX_LOOKUP_COST_FACTOR,
    vars::TIDB_OPT_INDEX_MERGE_COST_FACTOR,
    vars::TIDB_OPT_SORT_COST_FACTOR,
    vars::TIDB_OPT_TOP_N_COST_FACTOR,
    vars::TIDB_OPT_LIMIT_COST_FACTOR,
    vars::TIDB_OPT_STREAM_AGG_COST_FACTOR,
    vars::TIDB_OPT_HASH_AGG_COST_FACTOR,
    vars::TIDB_OPT_MERGE_JOIN_COST_FACTOR,
    vars::TIDB_OPT_HASH_JOIN_COST_FACTOR,
    vars::TIDB_OPT_INDEX_JOIN_COST_FACTOR,
    vars::TIDB_OPT_ORDERING_IDX_SEL_RATIO,
    vars::TIDB_OPT_RISK_EQ_SKEW_RATIO,
    vars::TIDB_OPT_RISK_GROUP_NDV_SKEW_RATIO,
    vars::TIDB_OPT_RISK_RANGE_SKEW_RATIO,
    vars::TIDB_OPT_PREFER_RANGE_SCAN,
    vars::TIDB_OPT_ENABLE_NO_DECORRELATE_IN_SELECT,
    vars::TIDB_OPT_ENABLE_SEMI_JOIN_REWRITE,
    vars::TIDB_OPT_SELECTIVITY_FACTOR,
    vars::TIDB_OPT_ENABLE_ALTERNATIVE_LOGICAL_PLANS,
];

/// Go `adjustVar`: the variable's next value in plan generation.
fn adjust_var(var_name: &str, var_val: OptVarValue) -> Result<OptVarValue, DriverError> {
    let unsupported = || plain_error(format!("unsupported variable {var_name} in plan generation"));
    match var_name {
        vars::TIDB_OPT_INDEX_SCAN_COST_FACTOR
        | vars::TIDB_OPT_INDEX_READER_COST_FACTOR
        | vars::TIDB_OPT_TABLE_READER_COST_FACTOR
        | vars::TIDB_OPT_TABLE_FULL_SCAN_COST_FACTOR
        | vars::TIDB_OPT_TABLE_RANGE_SCAN_COST_FACTOR
        | vars::TIDB_OPT_TABLE_ROW_ID_SCAN_COST_FACTOR
        | vars::TIDB_OPT_TABLE_TIFLASH_SCAN_COST_FACTOR
        | vars::TIDB_OPT_INDEX_LOOKUP_COST_FACTOR
        | vars::TIDB_OPT_INDEX_MERGE_COST_FACTOR
        | vars::TIDB_OPT_SORT_COST_FACTOR
        | vars::TIDB_OPT_TOP_N_COST_FACTOR
        | vars::TIDB_OPT_LIMIT_COST_FACTOR
        | vars::TIDB_OPT_STREAM_AGG_COST_FACTOR
        | vars::TIDB_OPT_HASH_AGG_COST_FACTOR
        | vars::TIDB_OPT_MERGE_JOIN_COST_FACTOR
        | vars::TIDB_OPT_HASH_JOIN_COST_FACTOR
        | vars::TIDB_OPT_INDEX_JOIN_COST_FACTOR => {
            // Cost factors take a penalty of 5 times their current cost in
            // each step.
            let OptVarValue::Float(v) = var_val else { return Err(unsupported()) };
            if v >= 1e6 {
                return Ok(OptVarValue::Float(v)); // avoid too large a penalty
            }
            Ok(OptVarValue::Float(v * 5.0))
        }
        // Range [0, 1]; "<= 0" means disabled.
        vars::TIDB_OPT_ORDERING_IDX_SEL_RATIO
        | vars::TIDB_OPT_RISK_EQ_SKEW_RATIO
        | vars::TIDB_OPT_RISK_RANGE_SKEW_RATIO
        | vars::TIDB_OPT_RISK_GROUP_NDV_SKEW_RATIO
        | vars::TIDB_OPT_SELECTIVITY_FACTOR => {
            let OptVarValue::Float(v) = var_val else { return Err(unsupported()) };
            if v <= 0.0 {
                return Ok(OptVarValue::Float(0.1));
            } else if v + 0.1 > 1.0 {
                return Ok(OptVarValue::Float(v));
            }
            // Increase 0.1 each step.
            Ok(OptVarValue::Float(v + 0.1))
        }
        // Flip the switch.
        vars::TIDB_OPT_PREFER_RANGE_SCAN
        | vars::TIDB_OPT_ENABLE_NO_DECORRELATE_IN_SELECT
        | vars::TIDB_OPT_ALWAYS_KEEP_JOIN_KEY
        | vars::TIDB_OPT_ENABLE_SEMI_JOIN_REWRITE
        | vars::TIDB_OPT_ENABLE_ALTERNATIVE_LOGICAL_PLANS => {
            let OptVarValue::Bool(v) = var_val else { return Err(unsupported()) };
            Ok(OptVarValue::Bool(!v))
        }
        _ => Err(unsupported()),
    }
}

/// Go `adjustFix`: the fix control's next value in plan generation.
fn adjust_fix(fix_id: u64, fix_val: &str) -> Result<String, DriverError> {
    match fix_id {
        // Flip the switch.
        tidb_fixcontrol::FIX_44855 | tidb_fixcontrol::FIX_52869 => {
            if fix_val.trim().to_uppercase() == tidb_vardef::global_sysvar_initial::OFF {
                Ok(tidb_vardef::global_sysvar_initial::ON.to_owned())
            } else {
                Ok(tidb_vardef::global_sysvar_initial::OFF.to_owned())
            }
        }
        tidb_fixcontrol::FIX_45132 => {
            let num: i64 = fix_val
                .parse()
                .map_err(|_| plain_error(format!("strconv.ParseInt: parsing \"{fix_val}\": invalid syntax")))?;
            if num <= 10 {
                return Ok(fix_val.to_owned());
            }
            // Each time 50% more aggressive.
            Ok((num / 2).to_string())
        }
        _ => Err(plain_error(format!("unsupported fix-control {fix_id} in plan generation"))),
    }
}

/// Go `getStartState`: the default values of the variables and fix controls.
fn get_start_state(vars: &[String], fixes: &[u64], index_hint_count: usize) -> Result<State, DriverError> {
    let mut state = State {
        leading2: [None, None],
        index_hints: vec![None; index_hint_count],
        no_decorrelate_qb: String::new(),
        var_names: vars.to_vec(),
        var_values: Vec::with_capacity(vars.len()),
        fix_ids: fixes.to_vec(),
        fix_values: Vec::with_capacity(fixes.len()),
    };
    for var_name in vars {
        let value = match var_name.as_str() {
            vars::TIDB_OPT_INDEX_SCAN_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_INDEX_SCAN_COST_FACTOR),
            vars::TIDB_OPT_INDEX_READER_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_INDEX_READER_COST_FACTOR),
            vars::TIDB_OPT_TABLE_READER_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_TABLE_READER_COST_FACTOR),
            vars::TIDB_OPT_TABLE_FULL_SCAN_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_TABLE_FULL_SCAN_COST_FACTOR),
            vars::TIDB_OPT_TABLE_RANGE_SCAN_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_TABLE_RANGE_SCAN_COST_FACTOR),
            vars::TIDB_OPT_TABLE_ROW_ID_SCAN_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_TABLE_ROW_ID_SCAN_COST_FACTOR),
            vars::TIDB_OPT_TABLE_TIFLASH_SCAN_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_TABLE_TIFLASH_SCAN_COST_FACTOR),
            vars::TIDB_OPT_INDEX_LOOKUP_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_INDEX_LOOKUP_COST_FACTOR),
            vars::TIDB_OPT_INDEX_MERGE_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_INDEX_MERGE_COST_FACTOR),
            vars::TIDB_OPT_SORT_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_SORT_COST_FACTOR),
            vars::TIDB_OPT_TOP_N_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_TOP_N_COST_FACTOR),
            vars::TIDB_OPT_LIMIT_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_LIMIT_COST_FACTOR),
            vars::TIDB_OPT_STREAM_AGG_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_STREAM_AGG_COST_FACTOR),
            vars::TIDB_OPT_HASH_AGG_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_HASH_AGG_COST_FACTOR),
            vars::TIDB_OPT_MERGE_JOIN_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_MERGE_JOIN_COST_FACTOR),
            vars::TIDB_OPT_HASH_JOIN_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_HASH_JOIN_COST_FACTOR),
            vars::TIDB_OPT_INDEX_JOIN_COST_FACTOR => OptVarValue::Float(def::DEF_OPT_INDEX_JOIN_COST_FACTOR),
            vars::TIDB_OPT_ORDERING_IDX_SEL_RATIO => OptVarValue::Float(def::DEF_TIDB_OPT_ORDERING_IDX_SEL_RATIO),
            vars::TIDB_OPT_RISK_EQ_SKEW_RATIO => OptVarValue::Float(def::DEF_OPT_RISK_EQ_SKEW_RATIO),
            vars::TIDB_OPT_RISK_RANGE_SKEW_RATIO => OptVarValue::Float(def::DEF_OPT_RISK_RANGE_SKEW_RATIO),
            vars::TIDB_OPT_RISK_GROUP_NDV_SKEW_RATIO => OptVarValue::Float(def::DEF_OPT_RISK_GROUP_NDV_SKEW_RATIO),
            vars::TIDB_OPT_PREFER_RANGE_SCAN => OptVarValue::Bool(def::DEF_OPT_PREFER_RANGE_SCAN),
            vars::TIDB_OPT_ENABLE_NO_DECORRELATE_IN_SELECT => OptVarValue::Bool(def::DEF_OPT_ENABLE_NO_DECORRELATE_IN_SELECT),
            vars::TIDB_OPT_ENABLE_SEMI_JOIN_REWRITE => OptVarValue::Bool(def::DEF_OPT_ENABLE_SEMI_JOIN_REWRITE),
            vars::TIDB_OPT_ALWAYS_KEEP_JOIN_KEY => OptVarValue::Bool(def::DEF_OPT_ALWAYS_KEEP_JOIN_KEY),
            vars::TIDB_OPT_SELECTIVITY_FACTOR => OptVarValue::Float(def::DEF_OPT_SELECTIVITY_FACTOR),
            vars::TIDB_OPT_CARTESIAN_JOIN_ORDER_THRESHOLD => OptVarValue::Float(def::DEF_OPT_CARTESIAN_JOIN_ORDER_THRESHOLD),
            vars::TIDB_OPT_ENABLE_ALTERNATIVE_LOGICAL_PLANS => OptVarValue::Bool(def::DEF_OPT_ENABLE_ALTERNATIVE_LOGICAL_PLANS),
            _ => return Err(plain_error(format!("unsupported variable {var_name} in plan generation"))),
        };
        state.var_values.push(value);
    }
    for fix_id in fixes {
        let value = match *fix_id {
            tidb_fixcontrol::FIX_44855 => "OFF",
            tidb_fixcontrol::FIX_45132 => "1000",
            tidb_fixcontrol::FIX_52869 => "OFF",
            _ => return Err(plain_error(format!("unsupported fix-control {fix_id} in plan generation"))),
        };
        state.fix_values.push(value.to_owned());
    }
    Ok(state)
}

fn select_of(stmt: &Stmt) -> Option<&SelectStmt> {
    match stmt {
        Stmt::Query(query) => match query.as_ref() {
            QueryStmt::Select(select) => Some(select),
            _ => None,
        },
        _ => None,
    }
}

/// A table reference's `(schema, name)` lowercased, the schema defaulted.
fn table_ref_name(default_schema: &str, table: &TableRef) -> (String, String) {
    let lower = |name: &str| tidb_hack::go_to_lower(name.to_owned());
    match table.name.as_slice() {
        [schema, name] => {
            let schema = lower(schema);
            (if schema.is_empty() { default_schema.to_owned() } else { schema }, lower(name))
        }
        [.., name] => (default_schema.to_owned(), lower(name)),
        [] => (default_schema.to_owned(), String::new()),
    }
}

/// Go `extractSelectTableNames`: every table name in the SELECT statement,
/// by `schema.name`, sorted.
fn extract_select_table_names(default_schema: &str, stmt: &Stmt) -> Vec<TableName> {
    struct Extractor<'a> {
        default_schema: &'a str,
        table_names: BTreeMap<String, TableName>,
    }
    impl Visitor for Extractor<'_> {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if let Some(table) = node.downcast_ref::<TableRef>() {
                let (schema, name) = table_ref_name(self.default_schema, table);
                let table = TableName {
                    schema,
                    name,
                    alias: String::new(),
                };
                self.table_names.entry(table.to_string()).or_insert(table);
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }
    let Some(select) = select_of(stmt) else {
        return Vec::new(); // only SELECT statements are supported for now
    };
    let mut extractor = Extractor {
        default_schema,
        table_names: BTreeMap::new(),
    };
    select.clone().accept(&mut extractor);
    extractor.table_names.into_values().collect()
}

/// Go `extractNoDecorrelateQBs`: the query-block names of the subqueries
/// below the top SELECT, sorted.
fn extract_no_decorrelate_qbs(stmt: &Stmt) -> Vec<String> {
    let Some(select) = select_of(stmt) else {
        return Vec::new(); // only SELECT statements are supported for now
    };
    let mut select = select.clone();
    // Go `selectOffsetAssigner`.
    struct OffsetAssigner(i32);
    impl Visitor for OffsetAssigner {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if let Some(select) = node.downcast_mut::<SelectStmt>() {
                self.0 += 1;
                select.query_block_offset = self.0;
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }
    select.accept(&mut OffsetAssigner(0));

    // Go `subqueryOffsetExtractor`.
    struct SubqueryOffsets(std::collections::BTreeSet<i32>);
    impl Visitor for SubqueryOffsets {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if let Some(expr) = node.downcast_ref::<Expr>() {
                let query = match expr {
                    Expr::Subquery(query) => Some(query.as_ref()),
                    Expr::Exists { subquery, .. }
                    | Expr::InSubquery { subquery, .. }
                    | Expr::CompareSubquery { subquery, .. } => Some(subquery.as_ref()),
                    _ => None,
                };
                if let Some(query) = query {
                    collect_subquery_offsets(query, &mut self.0);
                }
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }
    let mut extractor = SubqueryOffsets(std::collections::BTreeSet::new());
    select.accept(&mut extractor);
    let top_offset = select.query_block_offset;
    let mut qb_names: Vec<String> = extractor
        .0
        .into_iter()
        .filter(|offset| *offset != top_offset)
        .filter_map(|offset| tidb_hint::generate_qb_name(tidb_hint::NodeType::Select, offset).ok())
        .collect();
    qb_names.sort_by_key(|name| tidb_hack::go_to_lower(name.clone()));
    qb_names
}

/// Go `collectSubqueryOffsets` / `collectSubqueryOffsetsFromSelectList`.
fn collect_subquery_offsets(query: &QueryStmt, offsets: &mut std::collections::BTreeSet<i32>) {
    match query {
        QueryStmt::Select(select) => {
            if select.query_block_offset > 0 {
                offsets.insert(select.query_block_offset);
            }
        }
        QueryStmt::SetOpr(set_opr) => {
            for term in &set_opr.terms {
                collect_term_offsets(&term.body, offsets);
            }
        }
    }
}

fn collect_term_offsets(body: &SetOprTermBody, offsets: &mut std::collections::BTreeSet<i32>) {
    match body {
        SetOprTermBody::Select(select) => {
            if select.query_block_offset > 0 {
                offsets.insert(select.query_block_offset);
            }
        }
        SetOprTermBody::Nested(set_opr) => {
            for term in &set_opr.terms {
                collect_term_offsets(&term.body, offsets);
            }
        }
    }
}

/// Go `predicateColumnExtractor`: the columns of `table` the predicates
/// name, subqueries excluded.
struct PredicateColumnExtractor<'a> {
    table: &'a TableName,
    columns: HashSet<String>,
    allow_any_table: bool,
}

impl Visitor for PredicateColumnExtractor<'_> {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        // Only the current SELECT's predicates; inner queries are skipped.
        if node.is::<QueryStmt>() || node.is::<SelectStmt>() {
            return true;
        }
        let Some(Expr::Column(path)) = node.downcast_ref::<Expr>() else {
            return false;
        };
        let lower = |name: &String| tidb_hack::go_to_lower(name.clone());
        let (schema, table, name) = match path.as_slice() {
            [schema, table, name] => (lower(schema), lower(table), lower(name)),
            [table, name] => (String::new(), lower(table), lower(name)),
            [name] => (String::new(), String::new(), lower(name)),
            _ => return false,
        };
        if !self.allow_any_table {
            if table.is_empty() || !matches_column_table(self.table, &schema, &table) {
                return false;
            }
        } else if !schema.is_empty() && schema != self.table.schema {
            return false;
        }
        self.columns.insert(name);
        false
    }

    fn leave(&mut self, _node: &mut dyn Any) -> bool {
        true
    }
}

/// Go `matchesColumnTable`.
fn matches_column_table(target: &TableName, schema: &str, table: &str) -> bool {
    if !schema.is_empty() && schema != target.schema {
        return false;
    }
    if table.is_empty() {
        return false;
    }
    if table == target.name {
        return true;
    }
    !target.alias.is_empty() && table == target.alias
}

/// Go `extractSelectTableNamesWithAlias`: the FROM clause's tables with
/// their aliases, sorted.
fn extract_select_table_names_with_alias(default_schema: &str, select: &SelectStmt) -> Vec<TableName> {
    fn collect_node(default_schema: &str, node: &JoinNode, tables: &mut BTreeMap<String, TableName>) {
        match node {
            JoinNode::Table(table) => {
                let (schema, name) = table_ref_name(default_schema, table);
                let alias = table.alias.as_deref().map(|alias| tidb_hack::go_to_lower(alias.to_owned())).unwrap_or_default();
                let key = format!("{schema}.{name}:{alias}");
                tables.insert(key, TableName { schema, name, alias });
            }
            JoinNode::Join(join) => collect_join(default_schema, join, tables),
            _ => {}
        }
    }
    fn collect_join(default_schema: &str, join: &Join, tables: &mut BTreeMap<String, TableName>) {
        collect_node(default_schema, &join.left, tables);
        if let Some(right) = &join.right {
            collect_node(default_schema, right, tables);
        }
    }
    let Some(from) = &select.from else {
        return Vec::new();
    };
    let mut tables = BTreeMap::new();
    collect_join(default_schema, from, &mut tables);
    let mut names: Vec<TableName> = tables.into_values().collect();
    names.sort_by_key(ToString::to_string);
    names
}

/// Go `collectJoinPredicates`: the ON conditions of the FROM clause.
fn collect_join_predicates(join: &Join, extractor: &mut PredicateColumnExtractor<'_>) {
    if let Some(on) = &join.on {
        on.clone().accept(extractor);
    }
    for node in std::iter::once(&join.left).chain(join.right.as_ref()) {
        if let JoinNode::Join(join) = node {
            collect_join_predicates(join, extractor);
        }
    }
}

/// Go `extractSelectIndexHints`: per FROM table, the index hints worth
/// trying (an empty option first, so no index path is forced).
fn extract_select_index_hints(
    sctx: &mut dyn PlanExploreSession,
    default_schema: &str,
    stmt: &Stmt,
) -> Vec<Vec<Option<IndexHint>>> {
    let Some(select) = select_of(stmt) else {
        return Vec::new();
    };
    let table_names = extract_select_table_names_with_alias(default_schema, select);
    if table_names.is_empty() {
        return Vec::new();
    }
    let allow_any_table = table_names.len() == 1;
    let mut hint_options = Vec::with_capacity(table_names.len());
    for target in &table_names {
        let mut options: Vec<Option<IndexHint>> = vec![None];
        let mut extractor = PredicateColumnExtractor {
            table: target,
            columns: HashSet::new(),
            allow_any_table,
        };
        if let Some(where_clause) = &select.where_clause {
            where_clause.clone().accept(&mut extractor);
        }
        if let Some(from) = &select.from {
            collect_join_predicates(from, &mut extractor);
        }
        if extractor.columns.is_empty() {
            hint_options.push(options);
            continue;
        }
        let Some(table_info) = sctx.table_info_by_name(&target.schema, &target.name) else {
            hint_options.push(options);
            continue;
        };
        let use_invisible = sctx.optimizer_use_invisible_indexes();
        let table_info = table_info.read();
        let mut seen = HashSet::new();
        for index in table_info.indices.iter_deref() {
            let index = index.read();
            if index.state != tidb_model::SchemaState::PUBLIC {
                continue;
            }
            if !use_invisible && index.invisible {
                continue;
            }
            if index.is_columnar_index() || index.inverted_info.is_some() {
                continue;
            }
            if table_info.is_common_handle && index.primary {
                continue;
            }
            let Some(first) = index.columns.get(0) else {
                continue;
            };
            if !extractor.columns.contains(first.read().name.lowercase()) {
                continue;
            }
            let hint = IndexHint {
                table: target.clone(),
                index: index.name.original().to_owned(),
            };
            if !seen.insert(hint.index.clone()) {
                continue;
            }
            options.push(Some(hint));
        }
        hint_options.push(options);
    }
    hint_options
}

/// Go `binding_plan_generation_test.go`.
#[cfg(test)]
mod tests {
    use super::*;

    fn round_to_4_decimal(value: OptVarValue) -> f64 {
        let OptVarValue::Float(num) = value else { panic!("a float") };
        (num * 1e4).round() / 1e4
    }

    #[test]
    fn test_adjust_fixes() {
        assert_eq!(adjust_fix(tidb_fixcontrol::FIX_44855, "on").unwrap(), tidb_vardef::global_sysvar_initial::OFF);
        assert_eq!(adjust_fix(tidb_fixcontrol::FIX_44855, "off      ").unwrap(), tidb_vardef::global_sysvar_initial::ON);
        assert_eq!(adjust_fix(tidb_fixcontrol::FIX_45132, "1000").unwrap(), "500");
        assert_eq!(adjust_fix(tidb_fixcontrol::FIX_45132, "30").unwrap(), "15");
        assert_eq!(adjust_fix(tidb_fixcontrol::FIX_45132, "8").unwrap(), "8");
    }

    #[test]
    fn test_adjust_vars() {
        // Cost factors.
        let f = OptVarValue::Float;
        assert_eq!(adjust_var(vars::TIDB_OPT_INDEX_SCAN_COST_FACTOR, f(1.0)).unwrap(), f(5.0));
        assert_eq!(adjust_var(vars::TIDB_OPT_INDEX_SCAN_COST_FACTOR, f(5.0)).unwrap(), f(25.0));
        assert_eq!(adjust_var(vars::TIDB_OPT_INDEX_SCAN_COST_FACTOR, f(1e5)).unwrap(), f(5e5));
        assert_eq!(adjust_var(vars::TIDB_OPT_INDEX_SCAN_COST_FACTOR, f(2e6)).unwrap(), f(2e6));

        let ratio = vars::TIDB_OPT_ORDERING_IDX_SEL_RATIO;
        assert_eq!(round_to_4_decimal(adjust_var(ratio, f(-1.0)).unwrap()), 0.1);
        assert_eq!(round_to_4_decimal(adjust_var(ratio, f(0.2)).unwrap()), 0.3);
        assert_eq!(round_to_4_decimal(adjust_var(ratio, f(0.55)).unwrap()), 0.65);
        assert_eq!(round_to_4_decimal(adjust_var(ratio, f(0.95)).unwrap()), 0.95);

        assert!(adjust_var(vars::TIFLASH_REPLICA_READ, f(-1.0)).is_err()); // unsupported
    }

    #[test]
    fn test_start_state() {
        let vars: Vec<String> = [
            vars::TIDB_OPT_INDEX_SCAN_COST_FACTOR,
            vars::TIDB_OPT_INDEX_READER_COST_FACTOR,
            vars::TIDB_OPT_TABLE_READER_COST_FACTOR,
            vars::TIDB_OPT_TABLE_FULL_SCAN_COST_FACTOR,
            vars::TIDB_OPT_TABLE_RANGE_SCAN_COST_FACTOR,
            vars::TIDB_OPT_TABLE_ROW_ID_SCAN_COST_FACTOR,
            vars::TIDB_OPT_TABLE_TIFLASH_SCAN_COST_FACTOR,
            vars::TIDB_OPT_INDEX_LOOKUP_COST_FACTOR,
            vars::TIDB_OPT_INDEX_MERGE_COST_FACTOR,
            vars::TIDB_OPT_SORT_COST_FACTOR,
            vars::TIDB_OPT_TOP_N_COST_FACTOR,
            vars::TIDB_OPT_LIMIT_COST_FACTOR,
            vars::TIDB_OPT_STREAM_AGG_COST_FACTOR,
            vars::TIDB_OPT_HASH_AGG_COST_FACTOR,
            vars::TIDB_OPT_MERGE_JOIN_COST_FACTOR,
            vars::TIDB_OPT_HASH_JOIN_COST_FACTOR,
            vars::TIDB_OPT_INDEX_JOIN_COST_FACTOR,
            vars::TIDB_OPT_ORDERING_IDX_SEL_RATIO,
            vars::TIDB_OPT_RISK_EQ_SKEW_RATIO,
            vars::TIDB_OPT_RISK_RANGE_SKEW_RATIO,
            vars::TIDB_OPT_RISK_GROUP_NDV_SKEW_RATIO,
            vars::TIDB_OPT_SELECTIVITY_FACTOR,
            vars::TIDB_OPT_PREFER_RANGE_SCAN,
            vars::TIDB_OPT_ENABLE_NO_DECORRELATE_IN_SELECT,
            vars::TIDB_OPT_ENABLE_SEMI_JOIN_REWRITE,
            vars::TIDB_OPT_CARTESIAN_JOIN_ORDER_THRESHOLD,
        ]
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
        let fixes = [tidb_fixcontrol::FIX_44855, tidb_fixcontrol::FIX_45132, tidb_fixcontrol::FIX_52869];
        let state = get_start_state(&vars, &fixes, 0).unwrap();
        assert_eq!(
            state.encode(),
            "1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,0.0100,0.0000,0.0000,0.0000,0.8000,true,false,false,0.0000,OFF,1000,OFF"
        );
    }
}
