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

//! Go `pkg/planner/cascades/memo`: groups of equivalent group expressions
//! (`group.go`, `group_expr.go`), their id generator
//! (`group_id_generator.go`), and the memo that interns them (`memo.go`).
//!
//! Go links its objects by pointer; here the memo owns every group and group
//! expression and they refer to each other by id. A group expression copied
//! in from the optimized logical tree wraps that tree's operator, whose
//! children stay in place exactly as Go's wrapped `LogicalPlan` keeps its
//! own; one a rule builds owns its operator.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};

use tidb_expr::schema::Schema;
use tidb_planner_cascades_base::{new_hash_equaler, Hasher};
use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_logicalop::LogicalPlan;
use tidb_planner_core_operator_physicalop::task::Task;
use tidb_planner_property::stats_info::StatsInfo;
use tidb_planner_property::PhysicalProperty;

use super::pattern::{get_operand, Operand};

/// Go `memo.GroupID`.
pub type GroupId = u64;

/// A group expression's index in its memo.
pub type GeId = usize;

/// Go `GroupIDGenerator` (`group_id_generator.go`).
#[derive(Debug, Default)]
pub struct GroupIdGenerator {
    id: u64,
}

impl GroupIdGenerator {
    /// Go `NextGroupID`: ids start at 1.
    pub fn next_group_id(&mut self) -> GroupId {
        self.id += 1;
        self.id
    }
}

/// Go `property.LogicalProperty` as the memo keeps it per group: the
/// group's schema and statistics. The possible orders and the TiFlash mark
/// Go also keeps here live on the operators themselves in this port
/// (`prepare_possible_properties` stores them where the physical search
/// reads them).
#[derive(Clone, Debug, Default)]
pub struct LogicalProperty {
    /// Go `Schema`.
    pub schema: Option<Schema>,
    /// Go `Stats`.
    pub stats: Option<StatsInfo>,
}

/// The operator a group expression wraps.
#[derive(Debug)]
pub enum GePlan<'a> {
    /// An operator of the optimized logical tree, copied in in place.
    Tree(&'a LogicalPlan),
    /// An operator a transformation rule built.
    Owned(Box<LogicalPlan>),
}

impl GePlan<'_> {
    fn get(&self) -> &LogicalPlan {
        match self {
            GePlan::Tree(plan) => plan,
            GePlan::Owned(plan) => plan,
        }
    }
}

/// Go `GroupExpression` (`group_expr.go`).
#[derive(Debug)]
pub struct GroupExpression<'a> {
    /// Go's embedded `base.LogicalPlan`.
    plan: GePlan<'a>,
    /// Go `group`: the group this expression belongs to.
    group: Option<GroupId>,
    /// Go `Inputs`: the child groups.
    pub inputs: Vec<GroupId>,
    /// Go `hash64`.
    hash64: u64,
    /// Go `mask`: the rules already applied to this expression.
    mask: u64,
    /// Go `abandoned`.
    abandoned: bool,
}

impl<'a> GroupExpression<'a> {
    /// Go `GetWrappedLogicalPlan`.
    #[must_use]
    pub fn get_wrapped_logical_plan(&self) -> &LogicalPlan {
        self.plan.get()
    }

    /// Go `GetGroup`.
    #[must_use]
    pub const fn get_group(&self) -> Option<GroupId> {
        self.group
    }

    /// Go `GetHash64`.
    #[must_use]
    pub const fn get_hash64(&self) -> u64 {
        self.hash64
    }

    /// Go `IsExplored(i)`.
    #[must_use]
    pub const fn is_explored(&self, rule: u32) -> bool {
        self.mask & (1 << rule) != 0
    }

    /// Go `SetExplored(i)`.
    pub fn set_explored(&mut self, rule: u32) {
        self.mask |= 1 << rule;
    }

    /// Go `IsAbandoned`.
    #[must_use]
    pub const fn is_abandoned(&self) -> bool {
        self.abandoned
    }

    /// Go `SetAbandoned`.
    pub fn set_abandoned(&mut self) {
        self.abandoned = true;
    }

    /// Go `InputsLen`.
    #[must_use]
    pub fn inputs_len(&self) -> usize {
        self.inputs.len()
    }

    /// Go `GroupExpression.Hash64(h)`: the operator, then each input group's
    /// id.
    pub fn hash64(&self, h: &mut dyn Hasher) {
        self.plan.get().hash64(h);
        for child in &self.inputs {
            h.hash_uint64(*child);
        }
    }

    /// Go `GroupExpression.Equals`: the same number of inputs, the same
    /// operand, `Equals` operators and the same input groups.
    #[must_use]
    pub fn equals(&self, other: &GroupExpression<'_>) -> bool {
        self.inputs.len() == other.inputs.len()
            && get_operand(self.plan.get()) == get_operand(other.plan.get())
            && self.plan.get().equals(other.plan.get())
            && self.inputs == other.inputs
    }

    /// Go `GroupExpression.String(w)`.
    pub fn string(&self, w: &mut String) {
        w.push_str("GE:");
        w.push_str(&self.plan.get().base().base.explain_id(false));
        w.push('{');
        for (i, input) in self.inputs.iter().enumerate() {
            if i != 0 {
                w.push_str(", ");
            }
            w.push_str(&format!("GID:{input}"));
        }
        w.push('}');
    }
}

/// Go `Group` (`group.go`).
#[derive(Debug, Default)]
pub struct Group {
    /// Go `groupID`.
    group_id: GroupId,
    /// Go `logicalExpressions`, in list order: the expressions of one operand
    /// are kept together, the first of each run marked in
    /// `operand_2_first_expr`.
    logical_expressions: Vec<GeId>,
    /// Go `Operand2FirstExpr`.
    operand_2_first_expr: HashMap<Operand, GeId>,
    /// Go `hash2GroupExpr`: the expressions by hash, compared by `Equals`.
    hash_2_group_expr: HashMap<u64, Vec<GeId>>,
    /// Go `hash2ParentGroupExpr`: the expressions that take this group as an
    /// input.
    hash_2_parent_group_expr: BTreeSet<GeId>,
    /// Go `logicalProp`.
    logical_prop: Option<LogicalProperty>,
    /// Go `explored`.
    explored: bool,
    /// Go `bestPhysicalMap`, keyed by `PhysicalProperty.HashCode()`.
    best_physical_map: RefCell<HashMap<Vec<u8>, Task>>,
}

impl Group {
    /// Go `NewGroup(prop)`.
    #[must_use]
    pub fn new(prop: Option<LogicalProperty>) -> Self {
        Self {
            logical_prop: prop,
            ..Self::default()
        }
    }

    /// Go `GetGroupID`.
    #[must_use]
    pub const fn get_group_id(&self) -> GroupId {
        self.group_id
    }

    /// Go `GetLogicalExpressions`.
    #[must_use]
    pub fn get_logical_expressions(&self) -> &[GeId] {
        &self.logical_expressions
    }

    /// Go `GetFirstElem(operand)`.
    #[must_use]
    pub fn get_first_elem(&self, operand: Operand) -> Option<GeId> {
        if operand == Operand::Any {
            return self.logical_expressions.first().copied();
        }
        self.operand_2_first_expr.get(&operand).copied()
    }

    /// The expression after `ge` in this group's list (Go `Element.Next()`),
    /// or `None` past the end or when `ge` has left the group.
    #[must_use]
    pub fn next_elem(&self, ge: GeId) -> Option<GeId> {
        let position = self.logical_expressions.iter().position(|one| *one == ge)?;
        self.logical_expressions.get(position + 1).copied()
    }

    /// Go `HasLogicalProperty`.
    #[must_use]
    pub const fn has_logical_property(&self) -> bool {
        self.logical_prop.is_some()
    }

    /// Go `GetLogicalProperty`.
    #[must_use]
    pub fn get_logical_property(&self) -> Option<&LogicalProperty> {
        self.logical_prop.as_ref()
    }

    /// Go `SetLogicalProperty`.
    pub fn set_logical_property(&mut self, prop: LogicalProperty) {
        self.logical_prop = Some(prop);
    }

    /// Go `IsExplored`.
    #[must_use]
    pub const fn is_explored(&self) -> bool {
        self.explored
    }

    /// Go `SetExplored`.
    pub fn set_explored(&mut self) {
        self.explored = true;
    }

    /// Go `String(w)`.
    pub fn string(&self, w: &mut String) {
        w.push_str(&format!("GID:{}", self.group_id));
    }

    /// Go `SetBestTask(prop, task)`.
    pub fn set_best_task(&self, prop: &PhysicalProperty, task: &Task) {
        self.best_physical_map.borrow_mut().insert(prop.hash_code(), task.copy());
    }

    /// Go `GetBestTask(prop)`.
    #[must_use]
    pub fn get_best_task(&self, prop: &PhysicalProperty) -> Option<Task> {
        self.best_physical_map.borrow().get(&prop.hash_code()).map(Task::copy)
    }

    /// The parent expressions, as Go's `hash2ParentGroupExpr` holds them.
    #[must_use]
    pub fn parent_group_exprs(&self) -> Vec<GeId> {
        self.hash_2_parent_group_expr.iter().copied().collect()
    }

    /// Go `Clear`.
    fn clear(&mut self) {
        self.logical_expressions.clear();
        self.hash_2_group_expr.clear();
        self.hash_2_parent_group_expr.clear();
        self.operand_2_first_expr.clear();
        self.logical_prop = None;
    }
}

/// What `Memo::copy_in` copies: an operator whose inputs are either group
/// expressions already in the memo or further operators to copy in. Go
/// spells both as `LogicalPlan` children and tells them apart by type.
#[derive(Debug)]
pub enum CopyInPlan<'a> {
    /// An operator of the optimized tree; its children are copied in too.
    Tree(&'a LogicalPlan),
    /// A rule-built operator with explicit inputs.
    Owned {
        /// The operator.
        plan: Box<LogicalPlan>,
        /// Its inputs, in child order.
        inputs: Vec<CopyInChild<'a>>,
    },
}

/// One input of a [`CopyInPlan::Owned`].
#[derive(Debug)]
pub enum CopyInChild<'a> {
    /// A group expression already in the memo: its group is the input.
    GroupExpression(GeId),
    /// An operator to copy in first.
    Plan(CopyInPlan<'a>),
}

/// Go `Memo` (`memo.go`).
#[derive(Debug, Default)]
pub struct Memo<'a> {
    /// Go `groupIDGen`.
    group_id_gen: GroupIdGenerator,
    /// Go `rootGroup`.
    root_group: Option<GroupId>,
    /// Go `groups`, in list order.
    groups: Vec<GroupId>,
    /// Go `groupID2Group`, here the groups' owner.
    group_id_2_group: HashMap<GroupId, Group>,
    /// Go `hash2GlobalGroupExpr`: every live expression by hash, compared by
    /// `Equals`.
    hash_2_global_group_expr: HashMap<u64, Vec<GeId>>,
    /// Every group expression ever built, by id.
    ges: Vec<GroupExpression<'a>>,
}

impl<'a> Memo<'a> {
    /// Go `NewMemo()`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `Destroy`.
    pub fn destroy(&mut self) {
        *self = Self::default();
    }

    /// The group expression `id`.
    #[must_use]
    pub fn ge(&self, id: GeId) -> &GroupExpression<'a> {
        &self.ges[id]
    }

    /// The group expression `id`, mutably.
    pub fn ge_mut(&mut self, id: GeId) -> &mut GroupExpression<'a> {
        &mut self.ges[id]
    }

    /// The group `id`.
    ///
    /// # Panics
    ///
    /// When `id` is not a live group.
    #[must_use]
    pub fn group(&self, id: GroupId) -> &Group {
        &self.group_id_2_group[&id]
    }

    fn group_mut(&mut self, id: GroupId) -> &mut Group {
        self.group_id_2_group.get_mut(&id).expect("a live group")
    }

    /// Go `GetGroups`, in list order.
    #[must_use]
    pub fn get_groups(&self) -> &[GroupId] {
        &self.groups
    }

    /// Go `GetRootGroup`.
    #[must_use]
    pub const fn get_root_group(&self) -> Option<GroupId> {
        self.root_group
    }

    /// Go `ForEachGroup(f)`.
    pub fn for_each_group(&self, mut f: impl FnMut(&Group) -> bool) {
        for id in &self.groups {
            if !f(self.group(*id)) {
                break;
            }
        }
    }

    /// Go `Group.ForEachGE(f)` over group `g`.
    pub fn for_each_ge(&self, g: GroupId, mut f: impl FnMut(GeId, &GroupExpression<'a>) -> bool) {
        for id in &self.group(g).logical_expressions {
            if !f(*id, self.ge(*id)) {
                break;
            }
        }
    }

    /// Go `Group.SetExplored()` on group `g`.
    pub fn group_explored(&mut self, g: GroupId) {
        self.group_mut(g).set_explored();
    }

    /// Go `Init(plan)`: copies the tree in and makes its group the root.
    ///
    /// # Errors
    ///
    /// As [`Self::copy_in`].
    pub fn init(&mut self, plan: &'a LogicalPlan) -> Result<GeId, PlanError> {
        debug_assert!(self.groups.is_empty());
        let ge = self.copy_in(None, CopyInPlan::Tree(plan))?;
        self.root_group = self.ge(ge).group;
        Ok(ge)
    }

    /// Go `CopyIn(target, lp)`: interns `lp` and, recursively, its inputs;
    /// a new expression in a new group derives the group's logical property.
    ///
    /// # Errors
    ///
    /// None today; Go's `DeriveLogicalProp` can fail.
    pub fn copy_in(&mut self, target: Option<GroupId>, lp: CopyInPlan<'a>) -> Result<GeId, PlanError> {
        let (plan, children): (GePlan<'a>, Vec<CopyInChild<'a>>) = match lp {
            CopyInPlan::Tree(plan) => (
                GePlan::Tree(plan),
                plan.children()
                    .iter()
                    .map(|child| CopyInChild::Plan(CopyInPlan::Tree(child)))
                    .collect(),
            ),
            CopyInPlan::Owned { plan, inputs } => (GePlan::Owned(plan), inputs),
        };
        let mut child_groups = Vec::with_capacity(children.len());
        for child in children {
            let current_child_g = match child {
                CopyInChild::GroupExpression(ge) => self.ge(ge).group,
                CopyInChild::Plan(child) => {
                    let ge = self.copy_in(None, child)?;
                    self.ge(ge).group
                }
            };
            let current_child_g = current_child_g.expect("a copied-in child has a group");
            debug_assert!(Some(current_child_g) != target);
            child_groups.push(current_child_g);
        }
        let group_expr = self.new_group_expression(plan, child_groups);
        let (group_expr, ok) = self.insert_group_expression(group_expr, target);
        if ok && target.is_none() {
            self.derive_logical_prop(group_expr);
        }
        Ok(group_expr)
    }

    /// Go `GroupExpression.DeriveLogicalProp()`: a group without a logical
    /// property takes this expression's schema and statistics. The operator
    /// already carries them: the cascades path derives each operator's
    /// statistics as the memo would before copying the tree in
    /// (`LogicalPlan::memo_derive_stats_with_context`).
    fn derive_logical_prop(&mut self, ge: GeId) {
        let group = self.ge(ge).group.expect("an inserted expression has a group");
        if self.group(group).has_logical_property() {
            return;
        }
        let plan = self.ge(ge).get_wrapped_logical_plan();
        let prop = LogicalProperty {
            schema: plan.schema().cloned(),
            stats: plan.base().base.stats_info().cloned(),
        };
        self.group_mut(group).set_logical_property(prop);
    }

    /// Go `RemoveOut(target, ge)`.
    pub fn remove_out(&mut self, target: GroupId, ge: GeId) {
        self.group_delete(target, ge);
        self.global_remove(ge);
        for child in self.ge(ge).inputs.clone() {
            self.remove_parent_ges(child, ge);
        }
        self.ge_mut(ge).set_abandoned();
    }

    /// Go `NewGroupExpression(lp, inputs)`: the expression and its hash.
    pub fn new_group_expression(&mut self, plan: GePlan<'a>, inputs: Vec<GroupId>) -> GeId {
        let mut ge = GroupExpression {
            plan,
            group: None,
            inputs,
            hash64: 0,
            mask: 0,
            abandoned: false,
        };
        let mut h = new_hash_equaler();
        ge.hash64(&mut h);
        ge.hash64 = h.sum64();
        self.ges.push(ge);
        self.ges.len() - 1
    }

    /// Go `InsertGroupExpression(groupExpr, target)`: an expression `Equals`
    /// to a live one answers that one (merging its group into `target`);
    /// otherwise it joins `target`, or a new group.
    pub fn insert_group_expression(&mut self, group_expr: GeId, target: Option<GroupId>) -> (GeId, bool) {
        if let Some(existed_ge) = self.global_get(group_expr) {
            let existed_group = self.ge(existed_ge).group.expect("a live expression has a group");
            self.merge_group(existed_group, target);
            return (existed_ge, false);
        }
        let target = match target {
            Some(target) => target,
            None => self.new_group(),
        };
        self.group_insert(target, group_expr);
        self.global_put(group_expr);
        for child in self.ge(group_expr).inputs.clone() {
            self.add_parent_ges(child, group_expr);
        }
        (group_expr, true)
    }

    /// Go `NewGroup()`.
    pub fn new_group(&mut self) -> GroupId {
        let mut group = Group::new(None);
        group.group_id = self.group_id_gen.next_group_id();
        let id = group.group_id;
        self.groups.push(id);
        self.group_id_2_group.insert(id, group);
        id
    }

    /// Go `mergeGroup(src, dst)`: moves `src`'s expressions into `dst`,
    /// re-hashing every parent that took `src` as an input.
    fn merge_group(&mut self, src: GroupId, dst: Option<GroupId>) {
        let Some(dst) = dst else {
            return;
        };
        if dst == src || !self.group_id_2_group.contains_key(&src) {
            return;
        }
        self.groups.retain(|id| *id != src);
        if self.root_group == Some(src) {
            self.root_group = Some(dst);
        }
        let mut lazy_call_pair = Vec::new();
        for val in self.group(src).parent_group_exprs() {
            let parent_owner_g = self.ge(val).group.expect("a parent expression has a group");
            if parent_owner_g == dst {
                continue;
            }
            self.global_remove(val);
            self.group_delete(parent_owner_g, val);
            self.replace_ge_child(val, src, dst);
            self.group_insert(parent_owner_g, val);
            match self.global_get(val) {
                Some(existed_ge) => {
                    self.ge_mut(val).set_abandoned();
                    let existed_group = self.ge(existed_ge).group.expect("a live expression has a group");
                    if existed_group == parent_owner_g {
                        self.ge_merge_to(val, existed_ge);
                    } else {
                        lazy_call_pair.push((parent_owner_g, existed_group));
                    }
                }
                None => self.global_put(val),
            }
        }
        self.group_merge_to(src, dst);
        self.group_id_2_group.remove(&src);
        for (first, second) in lazy_call_pair {
            self.merge_group(first, Some(second));
        }
    }

    /// Go `replaceGEChild(ge, older, newer)`.
    fn replace_ge_child(&mut self, ge: GeId, older: GroupId, newer: GroupId) {
        self.remove_parent_ges(older, ge);
        for input in &mut self.ges[ge].inputs {
            if *input == older {
                *input = newer;
            }
        }
        let mut h = new_hash_equaler();
        self.ges[ge].hash64(&mut h);
        self.ges[ge].hash64 = h.sum64();
        self.add_parent_ges(newer, ge);
    }

    // ***** Go Group methods that read the expressions *****

    fn find_equal(&self, bucket: Option<&Vec<GeId>>, ge: GeId) -> Option<GeId> {
        let target = self.ge(ge);
        bucket?
            .iter()
            .copied()
            .find(|one| *one == ge || self.ge(*one).equals(target))
    }

    fn global_get(&self, ge: GeId) -> Option<GeId> {
        self.find_equal(self.hash_2_global_group_expr.get(&self.ge(ge).hash64), ge)
    }

    fn global_put(&mut self, ge: GeId) {
        let hash = self.ge(ge).hash64;
        self.hash_2_global_group_expr.entry(hash).or_default().push(ge);
    }

    /// Go `hashmap.Remove(ge)`: the entry whose key `Equals` `ge`.
    fn global_remove(&mut self, ge: GeId) {
        let hash = self.ge(ge).hash64;
        if let Some(found) = self.find_equal(self.hash_2_global_group_expr.get(&hash), ge) {
            if let Some(bucket) = self.hash_2_global_group_expr.get_mut(&hash) {
                bucket.retain(|one| *one != found);
            }
        }
    }

    /// Go `Group.Insert(e)`.
    fn group_insert(&mut self, g: GroupId, e: GeId) -> bool {
        let hash = self.ge(e).hash64;
        if self.find_equal(self.group(g).hash_2_group_expr.get(&hash), e).is_some() {
            return false;
        }
        let operand = get_operand(self.ge(e).get_wrapped_logical_plan());
        let group = self.group_mut(g);
        match group.operand_2_first_expr.get(&operand).copied() {
            Some(mark) => {
                let position = group
                    .logical_expressions
                    .iter()
                    .position(|one| *one == mark)
                    .expect("the operand mark is in the list");
                group.logical_expressions.insert(position + 1, e);
            }
            None => {
                group.logical_expressions.push(e);
                group.operand_2_first_expr.insert(operand, e);
            }
        }
        group.hash_2_group_expr.entry(hash).or_default().push(e);
        self.ge_mut(e).group = Some(g);
        true
    }

    /// Go `Group.Delete(e)`.
    fn group_delete(&mut self, g: GroupId, e: GeId) {
        let hash = self.ge(e).hash64;
        let Some(exist) = self.find_equal(self.group(g).hash_2_group_expr.get(&hash), e) else {
            return;
        };
        let operand = get_operand(self.ge(exist).get_wrapped_logical_plan());
        let next = self.group(g).next_elem(exist);
        let next_operand = next.map(|next| get_operand(self.ge(next).get_wrapped_logical_plan()));
        let group = self.group_mut(g);
        if let Some(bucket) = group.hash_2_group_expr.get_mut(&hash) {
            bucket.retain(|one| *one != exist);
        }
        if group.operand_2_first_expr.get(&operand) == Some(&exist) {
            match (next, next_operand) {
                (Some(next), Some(next_operand)) if next_operand == operand => {
                    group.operand_2_first_expr.insert(operand, next);
                }
                _ => {
                    group.operand_2_first_expr.remove(&operand);
                }
            }
        }
        group.logical_expressions.retain(|one| *one != exist);
        self.ge_mut(e).group = None;
    }

    fn add_parent_ges(&mut self, g: GroupId, parent: GeId) {
        let inserted = self.group_mut(g).hash_2_parent_group_expr.insert(parent);
        debug_assert!(inserted);
    }

    fn remove_parent_ges(&mut self, g: GroupId, parent: GeId) {
        let removed = self.group_mut(g).hash_2_parent_group_expr.remove(&parent);
        debug_assert!(removed);
    }

    /// Go `Group.mergeTo(target)`.
    fn group_merge_to(&mut self, g: GroupId, target: GroupId) {
        let parents = self.group(g).parent_group_exprs();
        self.group_mut(target).hash_2_parent_group_expr.extend(parents);
        for ge in self.group(g).logical_expressions.clone() {
            let hash = self.ge(ge).hash64;
            match self.find_equal(self.group(target).hash_2_group_expr.get(&hash), ge) {
                Some(existed_ge) => {
                    debug_assert!(existed_ge != ge);
                    self.ge_merge_to(ge, existed_ge);
                }
                None => {
                    self.group_insert(target, ge);
                }
            }
        }
        self.group_mut(g).clear();
    }

    /// Go `GroupExpression.mergeTo(target)`.
    fn ge_merge_to(&mut self, e: GeId, target: GeId) {
        if let Some(group) = self.ge(e).group {
            self.group_delete(group, e);
        }
        let mask = self.ge(e).mask;
        self.ge_mut(target).mask |= mask;
        for child in std::mem::take(&mut self.ge_mut(e).inputs) {
            self.remove_parent_ges(child, e);
        }
        self.ge_mut(e).group = None;
    }

    /// Go `Group.Check()`: the group's indexes agree with its list.
    pub fn check_group(&self, g: GroupId) {
        let group = self.group(g);
        assert!(group.group_id > 0);
        let indexed: usize = group.hash_2_group_expr.values().map(Vec::len).sum();
        assert_eq!(group.logical_expressions.len(), indexed);
        for ge in &group.logical_expressions {
            assert_eq!(self.ge(*ge).group, Some(g));
        }
        for first in group.operand_2_first_expr.values() {
            assert!(group.logical_expressions.contains(first));
        }
        for parent in &group.hash_2_parent_group_expr {
            assert!(self.ge(*parent).inputs.contains(&g));
        }
    }
}

/// One logical plan Go's `IteratorLP` assembles from the memo: a group
/// expression per visited group, its children the inputs' picks, and Go's
/// `planIDsHash` (the children's hashes, then the operator's plan id).
#[derive(Clone, Debug)]
pub struct IteratedPlan {
    /// The picked group expression.
    pub ge: GeId,
    /// Go `GetPlanIDsHash()`.
    pub plan_ids_hash: u64,
    /// The inputs' picks, in input order.
    pub children: Vec<IteratedPlan>,
}

/// Go `IteratorLP` (`memo.go`): enumerates every logical plan the memo
/// holds below its root, one expression per group, the last group's
/// choice advancing first.
#[derive(Debug)]
pub struct IteratorLp<'m, 'a> {
    memo: &'m Memo<'a>,
    /// Go `root`.
    root: Option<GroupId>,
    /// Go `stackInfo`.
    stack_info: Vec<Option<GeId>>,
    /// Go `traceID`.
    trace_id: i64,
}

impl<'a> Memo<'a> {
    /// Go `NewIterator()`.
    #[must_use]
    pub fn new_iterator(&self) -> IteratorLp<'_, 'a> {
        IteratorLp {
            memo: self,
            root: self.root_group,
            stack_info: Vec::with_capacity(self.groups.len()),
            trace_id: -1,
        }
    }
}

impl IteratorLp<'_, '_> {
    /// Go `Each(f)`.
    pub fn each(&mut self, mut f: impl FnMut(&IteratedPlan) -> bool) {
        while let Some(cur) = self.next_plan() {
            if !f(&cur) {
                break;
            }
        }
    }

    /// Go `Next()`: the next plan, or `None` when every combination has
    /// been produced.
    pub fn next_plan(&mut self) -> Option<IteratedPlan> {
        let root = self.root?;
        loop {
            self.trace_id = -1;
            if let Some(continue_group_element) = self.stack_info.last().copied() {
                let memo = self.memo;
                let next = continue_group_element.and_then(|ge| {
                    memo.ge(ge)
                        .get_group()
                        .and_then(|group| memo.group(group).next_elem(ge))
                });
                *self.stack_info.last_mut().expect("non-empty") = next;
            }
            let logic = self.dfs(root);
            if logic.is_some() || self.stack_info.is_empty() {
                return logic;
            }
        }
    }

    /// Go `dfs(target)`.
    fn dfs(&mut self, target: GroupId) -> Option<IteratedPlan> {
        self.trace_in(target);
        let ge = self.pick_group_expression()?;
        let mut children = Vec::new();
        for child_group in self.memo.ge(ge).inputs.clone() {
            children.push(self.dfs(child_group)?);
        }
        let mut h = new_hash_equaler();
        for child in &children {
            h.hash_uint64(child.plan_ids_hash);
        }
        h.hash_int(i64::from(self.memo.ge(ge).get_wrapped_logical_plan().id()));
        Some(IteratedPlan {
            ge,
            plan_ids_hash: h.sum64(),
            children,
        })
    }

    /// Go `traceIn(g)`.
    fn trace_in(&mut self, g: GroupId) {
        self.trace_id += 1;
        let trace_id = usize::try_from(self.trace_id).expect("non-negative");
        while self.stack_info.len() <= trace_id {
            self.stack_info
                .push(self.memo.group(g).get_logical_expressions().first().copied());
        }
    }

    /// Go `pickGroupExpression()`.
    fn pick_group_expression(&mut self) -> Option<GeId> {
        let current_group = usize::try_from(self.trace_id).expect("traced in");
        let current = self.stack_info[current_group];
        if current.is_none() {
            self.stack_info.truncate(current_group);
        }
        current
    }
}
