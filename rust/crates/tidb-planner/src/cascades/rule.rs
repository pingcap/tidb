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

//! Go `pkg/planner/cascades/rule`: the rule contract (`rule.go`,
//! `rule_type.go`), the binder that enumerates the memo's matches of a
//! rule's pattern (`binder.go`), the rule sets (`ruleset/rule_set.go`) and
//! the rules themselves (`apply/decorrelateapply`, `join/join_to_apply.go`).

use std::collections::HashMap;
use std::sync::LazyLock;

use tidb_planner_core_base::{PlanError, PlanIdAllocator};
use tidb_planner_core_operator_logicalop::{LogicalPlan, APPLY_GEN_FROM_XF_DECORRELATE_RULE_FLAG};

use super::memo::{CopyInChild, CopyInPlan, GeId, GroupExpression, GroupId, Memo};
use super::pattern::{get_operand, EngineTypeSet, Operand, Pattern};

/// Go `rule.Type` (`rule_type.go`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum Type {
    /// Go `DefaultNone`.
    DefaultNone,
    /// Go `XFJoinToApply`.
    XFJoinToApply,
    /// Go `XFDeCorrelateSimpleApply`.
    XFDeCorrelateSimpleApply,
    /// Go `XFPullCorrPredFromProj`.
    XFPullCorrPredFromProj,
    /// Go `XFPullCorrPredFromSel`.
    XFPullCorrPredFromSel,
    /// Go `XFPullCorrPredFromDS`.
    XFPullCorrPredFromDS,
    /// Go `XFPullCorrPredFromSort`.
    XFPullCorrPredFromSort,
    /// Go `XFPullCorrPredFromLimit`.
    XFPullCorrPredFromLimit,
    /// Go `XFPullCorrPredFromMax1Row`.
    XFPullCorrPredFromMax1Row,
    /// Go `XFPullCorrPredFromAgg1`.
    XFPullCorrPredFromAgg1,
    /// Go `XFPullCorrPredFromAgg2`.
    XFPullCorrPredFromAgg2,
    /// Go `XFMaximumRuleLength`.
    XFMaximumRuleLength,
}

impl Type {
    /// Go `Type.String()`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::XFJoinToApply => "join_to_apply",
            _ => "default_none",
        }
    }
}

/// Go `rule.Rule`.
pub trait Rule: Sync + Send {
    /// Go `ID()`; Go `BaseRule.ID` answers 0.
    fn id(&self) -> u32 {
        0
    }

    /// Go `String(writer)`.
    fn string(&self, w: &mut String);

    /// Go `Pattern()`.
    fn pattern(&self) -> &Pattern;

    /// Go `PreCheck(holder)`.
    fn pre_check(&self, _holder: &Holder, _memo: &Memo<'_>) -> bool {
        true
    }

    /// Go `Match(holder)`.
    fn matches(&self, _holder: &Holder, _memo: &Memo<'_>) -> bool {
        true
    }

    /// Go `XForm(holder)`: the new expressions for the holder's group and
    /// whether the holder's own expression is to be removed.
    ///
    /// # Errors
    ///
    /// A rule's failure.
    fn xform<'a>(
        &self,
        _holder: &Holder,
        _memo: &Memo<'a>,
        _allocator: &PlanIdAllocator,
    ) -> Result<(Vec<CopyInPlan<'a>>, bool), PlanError> {
        Ok((Vec::new(), false))
    }
}

/// Go `BaseRule`: a rule type and its pattern.
#[derive(Debug)]
pub struct BaseRule {
    tp: Type,
    pattern: Pattern,
}

impl BaseRule {
    /// Go `NewBaseRule(tp, pattern)`.
    #[must_use]
    pub const fn new(tp: Type, pattern: Pattern) -> Self {
        Self { tp, pattern }
    }
}

impl Rule for BaseRule {
    fn string(&self, w: &mut String) {
        w.push_str(self.tp.as_str());
    }

    fn pattern(&self) -> &Pattern {
        &self.pattern
    }
}

/// The expressions a binder matched: Go's placeholder tree, where the
/// matched group expression's wrapped operator has its children set to the
/// child groups' matched expressions.
#[derive(Clone, Debug, Default)]
pub struct Holder {
    /// The matched group expression.
    pub ge: GeId,
    /// The matched child expressions, by input.
    pub children: Vec<Option<Holder>>,
}

/// Go `Binder` (`binder.go`): enumerates, one `next` at a time, the
/// expression trees in the memo below a group expression that match a
/// pattern. Its state is the stack of the expression each visited group is
/// pinned at.
#[derive(Debug)]
pub struct Binder<'p> {
    /// Go `p`.
    p: &'p Pattern,
    /// Go `traceID`.
    trace_id: i64,
    /// Go `stackInfo`.
    stack_info: Vec<Option<GeId>>,
    /// Go `holder`.
    holder: Option<Holder>,
    /// Go `bsw`: the stack trace written after every step (tests).
    pub bsw: Option<String>,
}

/// Go `match(p, gE)`.
fn match_pattern(p: &Pattern, ge: &GroupExpression<'_>) -> bool {
    if p.operand == Operand::Any {
        return true;
    }
    get_operand(ge.get_wrapped_logical_plan()).matches(p.operand)
}

/// Go `anyHasBeenMatched(p, g, cur)`: an `OperandAny` pattern matches a
/// group once, with its first expression.
fn any_has_been_matched(p: &Pattern, memo: &Memo<'_>, g: GroupId, cur: GeId) -> bool {
    p.operand == Operand::Any && memo.group(g).get_first_elem(p.operand) != Some(cur)
}

impl<'p> Binder<'p> {
    /// Go `NewBinder(p, gE)`.
    #[must_use]
    pub const fn new(p: &'p Pattern, ge: GeId) -> Self {
        Self {
            p,
            trace_id: -1,
            stack_info: Vec::new(),
            holder: Some(Holder {
                ge,
                children: Vec::new(),
            }),
            bsw: None,
        }
    }

    /// Go `Next()`: the next matched tree, or `None` when exhausted.
    pub fn next(&mut self, memo: &Memo<'_>) -> Option<&Holder> {
        let mut ok;
        loop {
            self.trace_id = -1;
            if let Some(continue_group_element) = self.stack_info.last().copied() {
                let next = continue_group_element.and_then(|ge| {
                    memo.ge(ge)
                        .get_group()
                        .and_then(|group| memo.group(group).next_elem(ge))
                });
                *self.stack_info.last_mut().expect("non-empty") = next;
            }
            let mut holder = self.holder.take();
            ok = self.dfs_match(memo, self.p, &mut holder);
            self.holder = holder;
            if self.bsw.is_some() {
                self.print_stack_info(memo);
            }
            if ok || self.stack_info.is_empty() {
                break;
            }
        }
        if ok {
            self.holder.as_ref()
        } else {
            None
        }
    }

    /// Go `dfsMatch(p, parentHolder)`.
    fn dfs_match(&mut self, memo: &Memo<'_>, p: &Pattern, parent_holder: &mut Option<Holder>) -> bool {
        let Some(holder) = parent_holder else {
            return false;
        };
        let ge = memo.ge(holder.ge);
        if !match_pattern(p, ge) {
            return false;
        }
        if p.children.is_empty() {
            return true;
        }
        if p.children.len() != ge.inputs.len() {
            return false;
        }
        if holder.children.len() < p.children.len() {
            holder.children.resize(p.children.len(), None);
        }
        for (i, child_pattern) in p.children.iter().enumerate() {
            let child_group = ge.inputs[i];
            self.trace_in(memo, child_pattern, child_group);
            holder.children[i] = self
                .pick_group_expression(memo, child_pattern, child_group)
                .map(|ge| Holder {
                    ge,
                    children: Vec::new(),
                });
            if !self.dfs_match(memo, child_pattern, &mut holder.children[i]) {
                return false;
            }
        }
        true
    }

    /// Go `pickGroupExpression(p, g)`.
    fn pick_group_expression(&mut self, memo: &Memo<'_>, p: &Pattern, g: GroupId) -> Option<GeId> {
        let current_group = usize::try_from(self.trace_id).expect("traced in");
        let current = self.stack_info[current_group];
        match current {
            Some(cur) if match_pattern(p, memo.ge(cur)) && !any_has_been_matched(p, memo, g, cur) => Some(cur),
            _ => {
                self.stack_info.truncate(current_group);
                None
            }
        }
    }

    /// Go `traceIn(p, g)`.
    fn trace_in(&mut self, memo: &Memo<'_>, p: &Pattern, g: GroupId) {
        self.trace_id += 1;
        let trace_id = usize::try_from(self.trace_id).expect("non-negative");
        while self.stack_info.len() <= trace_id {
            self.stack_info.push(memo.group(g).get_first_elem(p.operand));
        }
    }

    /// Go `printStackInfo(w)`.
    fn print_stack_info(&mut self, memo: &Memo<'_>) {
        let mut out = String::new();
        for (i, one) in self.stack_info.iter().enumerate() {
            if i != 0 {
                out.push_str(" -> ");
            }
            if let Some(one) = one {
                memo.ge(*one).string(&mut out);
            }
        }
        if !self.stack_info.is_empty() {
            out.push('\n');
        }
        if let Some(bsw) = self.bsw.as_mut() {
            bsw.push_str(&out);
        }
    }

    /// Go `GetHolder()`.
    #[must_use]
    pub const fn get_holder(&self) -> Option<&Holder> {
        self.holder.as_ref()
    }
}

// ***** ruleset/rule_set.go *****

/// Go `ruleset.SetType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SetType {
    /// Go `DefaultNone`.
    DefaultNone,
    /// Go `XFSetDeCorrelateApply`.
    XFSetDeCorrelateApply,
}

/// Go `ListRules`.
#[derive(Clone, Default)]
pub struct ListRules(pub Vec<&'static dyn Rule>);

impl ListRules {
    /// Go `ListRules.Filter(mask)`: the rules whose id is in `mask`.
    #[must_use]
    pub fn filter(&self, mask: u64) -> Self {
        Self(
            self.0
                .iter()
                .copied()
                .filter(|one| mask & (1 << one.id()) != 0)
                .collect(),
        )
    }
}

/// Go `OperandRules`.
pub struct OperandRules {
    set_map: HashMap<SetType, Vec<&'static dyn Rule>>,
    set_list: Vec<&'static dyn Rule>,
}

impl OperandRules {
    /// Go `OperandRules.Filter(ge)`: an Apply a decorrelation rule built
    /// takes only the decorrelation set.
    #[must_use]
    pub fn filter(&self, ge: &GroupExpression<'_>) -> ListRules {
        if ge.get_wrapped_logical_plan().base().flag & APPLY_GEN_FROM_XF_DECORRELATE_RULE_FLAG > 0 {
            return ListRules(
                self.set_map
                    .get(&SetType::XFSetDeCorrelateApply)
                    .cloned()
                    .unwrap_or_default(),
            );
        }
        ListRules(self.set_list.clone())
    }
}

/// Go `DefaultRuleSets`: the rules the optimizer applies, by operand. Go
/// lists none today (the Apply set is commented out).
pub static DEFAULT_RULE_SETS: LazyLock<HashMap<Operand, OperandRules>> = LazyLock::new(HashMap::new);

static XF_DECORRELATE_SIMPLE_APPLY: LazyLock<XFDeCorrelateSimpleApply> =
    LazyLock::new(XFDeCorrelateSimpleApply::new);

/// Go `OperandApplyRules`.
pub static OPERAND_APPLY_RULES: LazyLock<OperandRules> = LazyLock::new(|| OperandRules {
    set_map: HashMap::from([(
        SetType::XFSetDeCorrelateApply,
        vec![&*XF_DECORRELATE_SIMPLE_APPLY as &'static dyn Rule],
    )]),
    set_list: vec![&*XF_DECORRELATE_SIMPLE_APPLY as &'static dyn Rule],
});

// ***** join/join_to_apply.go *****

/// Go `XFJoinToApply`: a placeholder rule over `Join{ANY, Join}`.
#[derive(Debug)]
pub struct XFJoinToApply {
    base: BaseRule,
}

impl XFJoinToApply {
    /// Go `NewJoinToApply()`.
    #[must_use]
    pub fn new() -> Self {
        let mut pa = Pattern::new(Operand::Join, EngineTypeSet::TIDB_ONLY);
        pa.set_children(vec![
            Pattern::new(Operand::Any, EngineTypeSet::ALL),
            Pattern::new(Operand::Join, EngineTypeSet::TIDB_ONLY),
        ]);
        Self {
            base: BaseRule::new(Type::XFJoinToApply, pa),
        }
    }
}

impl Default for XFJoinToApply {
    fn default() -> Self {
        Self::new()
    }
}

impl Rule for XFJoinToApply {
    fn string(&self, w: &mut String) {
        self.base.string(w);
    }

    fn pattern(&self) -> &Pattern {
        self.base.pattern()
    }
}

// ***** apply/decorrelateapply *****

/// Go `XFDeCorrelateSimpleApply`: an Apply whose inner side uses none of
/// the outer side's columns is the join it embeds.
#[derive(Debug)]
pub struct XFDeCorrelateSimpleApply {
    base: BaseRule,
}

impl XFDeCorrelateSimpleApply {
    /// Go `NewXFDeCorrelateSimpleApply()`.
    #[must_use]
    pub fn new() -> Self {
        let mut pa = Pattern::new(Operand::Apply, EngineTypeSet::TIDB_ONLY);
        pa.set_children(vec![
            Pattern::new(Operand::Any, EngineTypeSet::TIDB_ONLY),
            Pattern::new(Operand::Any, EngineTypeSet::TIDB_ONLY),
        ]);
        Self {
            base: BaseRule::new(Type::XFDeCorrelateSimpleApply, pa),
        }
    }
}

impl Default for XFDeCorrelateSimpleApply {
    fn default() -> Self {
        Self::new()
    }
}

impl Rule for XFDeCorrelateSimpleApply {
    fn id(&self) -> u32 {
        Type::XFDeCorrelateSimpleApply as u32
    }

    fn string(&self, w: &mut String) {
        self.base.string(w);
    }

    fn pattern(&self) -> &Pattern {
        self.base.pattern()
    }

    /// Go `XFDeCorrelateApplyBase.PreCheck`: not an Apply marked
    /// `NoDecorrelate`.
    fn pre_check(&self, holder: &Holder, memo: &Memo<'_>) -> bool {
        match memo.ge(holder.ge).get_wrapped_logical_plan() {
            LogicalPlan::Apply(apply) => !apply.no_decorrelate,
            _ => false,
        }
    }

    fn xform<'a>(
        &self,
        holder: &Holder,
        memo: &Memo<'a>,
        allocator: &PlanIdAllocator,
    ) -> Result<(Vec<CopyInPlan<'a>>, bool), PlanError> {
        let (Some(Some(outer)), Some(Some(inner))) = (holder.children.first(), holder.children.get(1)) else {
            return Ok((Vec::new(), false));
        };
        let LogicalPlan::Apply(apply) = memo.ge(holder.ge).get_wrapped_logical_plan() else {
            return Ok((Vec::new(), false));
        };
        let remove = apply.join.base.flag & APPLY_GEN_FROM_XF_DECORRELATE_RULE_FLAG > 0;
        let outer_schema = memo
            .ge(outer.ge)
            .get_wrapped_logical_plan()
            .schema()
            .cloned()
            .unwrap_or_default();
        let mut inner_plan = memo.ge(inner.ge).get_wrapped_logical_plan().clone();
        let cor_cols =
            crate::expression_rewriter::extract_cor_columns_by_schema_4_logical_plan(&mut inner_plan, &outer_schema);
        if cor_cols.is_empty() {
            // Go `LogicalJoinShallowRef` and `ReAlloc4Cascades(TypeJoin, ...)`:
            // the embedded join as a new operator with its own plan id.
            let mut cloned_join = apply.join.clone();
            cloned_join.base.base.set_id(allocator.alloc());
            cloned_join.base.base.set_tp(tidb_plancodec::TYPE_JOIN);
            return Ok((
                vec![CopyInPlan::Owned {
                    plan: Box::new(LogicalPlan::Join(cloned_join)),
                    inputs: vec![
                        CopyInChild::GroupExpression(outer.ge),
                        CopyInChild::GroupExpression(inner.ge),
                    ],
                }],
                remove,
            ));
        }
        Ok((Vec::new(), false))
    }
}
