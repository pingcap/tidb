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

//! Go's cascades package tests: `memo/memo_test.go`,
//! `memo/group_and_expr_test.go`, `memo/group_id_generator_test.go`,
//! `rule/binder_test.go`, `task/task_test.go`,
//! `task/task_scheduler_test.go`, `pattern/pattern_test.go` and
//! `pattern/engine_test.go`.
//!
//! Go's memo and binder tests copy in `DataSource{}`s that differ only in
//! plan id. Their `Equals` holds, yet Go keeps them in separate groups only
//! because `DeriveLogicalProp` materializes the first one's lazy empty
//! schema before the second is interned; the sources here carry distinct
//! schemas instead, the shape every planned `DataSource` has.

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expression_aggregation::ByItems;
use tidb_planner_cascades_base::new_hash_equaler;
use tidb_planner_core_base::{LogicalJoinType, PlanError, PlanIdAllocator};
use tidb_planner_core_operator_logicalop::{
    BaseLogicalPlan, DataSource, LogicalJoin, LogicalLimit, LogicalPlan, LogicalProjection, LogicalSort,
};

use super::memo::{CopyInPlan, GePlan, GroupIdGenerator, Memo};
use super::pattern::{get_operand, EngineType, EngineTypeSet, Operand, Pattern};
use super::rule::Binder;
use super::task::{SimpleTaskScheduler, Stack, Task};
use super::Context;

fn column(unique_id: i64) -> Column {
    Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong))
}

fn data_source(id: i32, unique_id: i64, alias: Option<&str>) -> LogicalPlan {
    let mut base = BaseLogicalPlan::with_id(id, DataSource::TYPE, 0);
    base.base.set_schema(Some(Schema::new(vec![column(unique_id)])));
    LogicalPlan::DataSource(DataSource {
        base,
        table_as_name: alias.map(str::to_owned),
        ..DataSource::default()
    })
}

fn join(id: i32, children: Vec<LogicalPlan>) -> LogicalPlan {
    let mut base = BaseLogicalPlan::with_id(id, LogicalJoin::TYPE, 0);
    base.set_children(children);
    LogicalPlan::Join(LogicalJoin::new(base, LogicalJoinType::Inner))
}

fn sort(by_items: usize) -> LogicalPlan {
    LogicalPlan::Sort(LogicalSort::new(
        BaseLogicalPlan::with_id(0, LogicalSort::TYPE, 0),
        vec![
            ByItems {
                expr: Expression::Constant(Constant::new_one()),
                desc: true,
            };
            by_items
        ],
    ))
}

fn limit(id: i32, offset: u64) -> LogicalPlan {
    LogicalPlan::Limit(LogicalLimit::new(BaseLogicalPlan::with_id(id, LogicalLimit::TYPE, 0), offset, 0))
}

fn projection_one() -> LogicalPlan {
    LogicalPlan::Projection(LogicalProjection::new(
        BaseLogicalPlan::with_id(0, LogicalProjection::TYPE, 0),
        vec![Expression::Constant(Constant::new_one())],
    ))
}

fn alias(memo: &Memo<'_>, ge: usize) -> String {
    match memo.ge(ge).get_wrapped_logical_plan() {
        LogicalPlan::DataSource(ds) => ds.table_as_name.clone().unwrap_or_default(),
        _ => String::new(),
    }
}

// ***** memo/group_id_generator_test.go *****

#[test]
fn group_id_generator_next_group_id() {
    let mut g = GroupIdGenerator::default();
    for want in 1..=3 {
        assert_eq!(g.next_group_id(), want);
    }
}

// ***** memo/memo_test.go *****

#[test]
fn memo_init_numbers_groups_from_one() {
    let plan = join(3, vec![data_source(1, 1, None), data_source(2, 2, None)]);
    let mut mm = Memo::new();
    mm.init(&plan).unwrap();
    assert_eq!(mm.get_groups(), &[1, 2, 3]);
}

#[test]
fn insert_ge_into_a_new_group() {
    let plan = join(3, vec![data_source(1, 1, None), data_source(2, 2, None)]);
    let mut mm = Memo::new();
    mm.init(&plan).unwrap();
    assert_eq!(mm.get_groups().len(), 3);
    let limit = limit(-4, 0);
    let root = mm.get_root_group().unwrap();
    let group_expr = mm.new_group_expression(GePlan::Tree(&limit), vec![root]);
    mm.insert_group_expression(group_expr, None);
    assert_eq!(mm.get_groups(), &[1, 2, 3, 4]);
}

/// Go `TestMergeGroup`: re-inserting `sort1` (as `sort3`) into `dstG`
/// merges `srcG` into it; the parents' limits differ, so they stay apart.
#[test]
fn merge_group_moves_the_source_groups_expressions_and_parents() {
    let sort1 = sort(1);
    let sort2 = sort(2);
    let sort3 = sort(1);
    let limit1 = limit(0, 1);
    let limit2 = limit(0, 2);
    let mut mm = Memo::new();
    let _root = mm.new_group();
    let child_g1 = mm.new_group();
    let src_g = mm.new_group();
    let ge = mm.new_group_expression(GePlan::Tree(&sort1), vec![child_g1]);
    mm.insert_group_expression(ge, Some(src_g));
    let dst_g = mm.new_group();
    let ge = mm.new_group_expression(GePlan::Tree(&sort2), vec![child_g1]);
    mm.insert_group_expression(ge, Some(dst_g));
    let src_parent_ge = mm.new_group_expression(GePlan::Tree(&limit1), vec![src_g]);
    let src_parent_group = mm.new_group();
    mm.insert_group_expression(src_parent_ge, Some(src_parent_group));
    let dst_parent_ge = mm.new_group_expression(GePlan::Tree(&limit2), vec![dst_g]);
    let dst_parent_group = mm.new_group();
    mm.insert_group_expression(dst_parent_ge, Some(dst_parent_group));

    let sort3_ge = mm.new_group_expression(GePlan::Tree(&sort3), vec![child_g1]);
    let (existed_ge, inserted) = mm.insert_group_expression(sort3_ge, Some(dst_g));
    assert!(!inserted);
    assert_eq!(mm.ge(existed_ge).get_group(), Some(4));
    assert_eq!(mm.ge(existed_ge).inputs, vec![child_g1]);
    assert_eq!(mm.ge(existed_ge).get_hash64(), mm.ge(sort3_ge).get_hash64());
    mm.check_group(dst_g);
    assert_eq!(mm.group(dst_g).get_logical_expressions().len(), 2);
    let parent_groups: Vec<_> = mm
        .group(dst_g)
        .parent_group_exprs()
        .into_iter()
        .map(|ge| mm.ge(ge).get_group().unwrap())
        .collect();
    assert_eq!(parent_groups.len(), 2);
    assert!(parent_groups.contains(&src_parent_group));
    assert!(parent_groups.contains(&dst_parent_group));
    // srcG is gone from the memo.
    assert!(!mm.get_groups().contains(&src_g));
    assert_eq!(mm.get_groups().len(), 5);
    // childG's parents now all live in dstG.
    let child_parents: Vec<_> = mm
        .group(child_g1)
        .parent_group_exprs()
        .into_iter()
        .map(|ge| mm.ge(ge).get_group().unwrap())
        .collect();
    assert_eq!(child_parents, vec![dst_g, dst_g]);
}

/// Go `TestRecursiveMergeGroup`: merging `srcG` into `dstG` makes the two
/// parents' equal limits collide, which merges their groups too.
#[test]
fn recursive_merge_group_merges_the_parents_that_collide() {
    let proj = projection_one();
    let sort1 = sort(1);
    let sort2 = sort(1);
    let limit1 = limit(0, 1);
    let limit2 = limit(0, 1);
    let mut mm = Memo::new();
    let _root = mm.new_group();
    let child_g1 = mm.new_group();
    let proj_g2 = mm.new_group();
    let ge = mm.new_group_expression(GePlan::Tree(&proj), vec![child_g1]);
    mm.insert_group_expression(ge, Some(proj_g2));
    let src_g = mm.new_group();
    let ge = mm.new_group_expression(GePlan::Tree(&sort1), vec![child_g1]);
    mm.insert_group_expression(ge, Some(src_g));
    let dst_g = mm.new_group();
    let ge = mm.new_group_expression(GePlan::Tree(&sort1), vec![proj_g2]);
    mm.insert_group_expression(ge, Some(dst_g));
    let src_parent_ge = mm.new_group_expression(GePlan::Tree(&limit1), vec![src_g]);
    let src_parent_group = mm.new_group();
    mm.insert_group_expression(src_parent_ge, Some(src_parent_group));
    let dst_parent_ge = mm.new_group_expression(GePlan::Tree(&limit2), vec![dst_g]);
    let dst_parent_group = mm.new_group();
    mm.insert_group_expression(dst_parent_ge, Some(dst_parent_group));

    let sort2_ge = mm.new_group_expression(GePlan::Tree(&sort2), vec![child_g1]);
    let (existed_ge, inserted) = mm.insert_group_expression(sort2_ge, Some(dst_g));
    assert!(!inserted);
    assert_eq!(mm.ge(existed_ge).get_group(), Some(5));
    assert_eq!(mm.group(dst_g).get_logical_expressions().len(), 2);
    mm.check_group(dst_g);
    let dst_parents = mm.group(dst_g).parent_group_exprs();
    assert_eq!(dst_parents.len(), 1);
    assert_eq!(mm.ge(dst_parents[0]).get_group(), Some(dst_parent_group));
    // srcG and srcParentGroup are merged away.
    assert!(!mm.get_groups().contains(&src_g));
    assert!(!mm.get_groups().contains(&src_parent_group));
    assert_eq!(mm.get_groups().len(), 5);
    mm.check_group(dst_parent_group);
    assert_eq!(mm.group(dst_parent_group).get_logical_expressions(), &[dst_parent_ge]);
    mm.check_group(proj_g2);
    mm.check_group(child_g1);
    let child_parents: Vec<_> = mm
        .group(child_g1)
        .parent_group_exprs()
        .into_iter()
        .map(|ge| mm.ge(ge).get_group().unwrap())
        .collect();
    assert_eq!(child_parents.len(), 2);
    assert!(child_parents.iter().all(|g| *g == dst_g || *g == proj_g2));
    assert!(child_parents.contains(&dst_g));
}

// ***** memo/group_and_expr_test.go *****

/// Go `TestGroupExpressionHashEquals`: the input order changes the hash and
/// the equality; the owning group does not.
#[test]
fn group_expression_hash_equals_follow_the_inputs_in_order() {
    let a_plan = projection_one();
    let b_plan = projection_one();
    let mut mm = Memo::new();
    let a = mm.new_group_expression(GePlan::Tree(&a_plan), vec![1, 2]);
    let b = mm.new_group_expression(GePlan::Tree(&b_plan), vec![1, 2]);
    assert_eq!(mm.ge(a).get_hash64(), mm.ge(b).get_hash64());
    assert!(mm.ge(a).equals(mm.ge(b)));
    let c = mm.new_group_expression(GePlan::Tree(&b_plan), vec![2, 1]);
    assert_ne!(mm.ge(a).get_hash64(), mm.ge(c).get_hash64());
    assert!(!mm.ge(a).equals(mm.ge(c)));
}

/// Go `TestGroupHashEquals`: a group hashes its id.
#[test]
fn group_hash_is_its_id() {
    let mut h1 = new_hash_equaler();
    let mut h2 = new_hash_equaler();
    tidb_planner_cascades_base::Hasher::hash_uint64(&mut h1, 1);
    tidb_planner_cascades_base::Hasher::hash_uint64(&mut h2, 1);
    assert_eq!(
        tidb_planner_cascades_base::Hasher::sum64(&h1),
        tidb_planner_cascades_base::Hasher::sum64(&h2)
    );
}

/// Go `TestGroupParentGERefs`: every input group knows the expression above
/// it, and the memo indexes all three expressions.
#[test]
fn group_parent_ge_refs() {
    let plan = join(3, vec![data_source(1, 1, None), data_source(2, 2, None)]);
    let mut mm = Memo::new();
    let j = mm.init(&plan).unwrap();
    assert_eq!(mm.get_groups().len(), 3);
    let root = mm.get_root_group().unwrap();
    assert!(mm.group(root).parent_group_exprs().is_empty());
    assert_eq!(mm.group(root).get_logical_expressions(), &[j]);
    assert!(mm.ge(j).get_wrapped_logical_plan().equals(&plan));
    for (i, child) in plan.children().iter().enumerate() {
        let group = mm.ge(j).inputs[i];
        assert_eq!(mm.group(group).parent_group_exprs(), vec![j]);
        let [one] = mm.group(group).get_logical_expressions() else {
            panic!("one expression");
        };
        assert!(mm.ge(*one).get_wrapped_logical_plan().equals(child));
        mm.check_group(group);
    }
}

// ***** rule/binder_test.go *****

#[test]
fn binder_success() {
    let plan = join(3, vec![data_source(1, 1, None), data_source(2, 2, None)]);
    let mut mm = Memo::new();
    mm.init(&plan).unwrap();
    let mut pa = Pattern::new(Operand::Join, EngineTypeSet::ALL);
    pa.set_children(vec![
        Pattern::new(Operand::DataSource, EngineTypeSet::ALL),
        Pattern::new(Operand::DataSource, EngineTypeSet::ALL),
    ]);
    let root_ge = *mm.group(mm.get_root_group().unwrap()).get_logical_expressions().last().unwrap();
    let mut binder = Binder::new(&pa, root_ge);
    let holder = binder.next(&mm).unwrap();
    assert!(std::ptr::eq(mm.ge(holder.ge).get_wrapped_logical_plan(), &plan));
    let left = holder.children[0].as_ref().unwrap().ge;
    let right = holder.children[1].as_ref().unwrap().ge;
    assert!(std::ptr::eq(mm.ge(left).get_wrapped_logical_plan(), &plan.children()[0]));
    assert!(std::ptr::eq(mm.ge(right).get_wrapped_logical_plan(), &plan.children()[1]));
}

#[test]
fn binder_fail() {
    let plan = join(3, vec![data_source(1, 1, None), data_source(2, 2, None)]);
    let mut mm = Memo::new();
    mm.init(&plan).unwrap();
    let mut pa = Pattern::new(Operand::Join, EngineTypeSet::ALL);
    pa.set_children(vec![
        Pattern::new(Operand::DataSource, EngineTypeSet::ALL),
        Pattern::new(Operand::Projection, EngineTypeSet::ALL),
    ]);
    let root_ge = *mm.group(mm.get_root_group().unwrap()).get_logical_expressions().last().unwrap();
    let mut binder = Binder::new(&pa, root_ge);
    binder.bsw = Some(String::new());
    assert!(binder.next(&mm).is_none());
    assert_eq!(binder.bsw.as_deref(), Some("GE:DataSource_1{}\n"));

    let mut p2 = Pattern::new(Operand::Limit, EngineTypeSet::ALL);
    p2.set_children(vec![Pattern::new(Operand::Join, EngineTypeSet::ALL)]);
    let mut pa = Pattern::new(Operand::Projection, EngineTypeSet::ALL);
    pa.set_children(vec![p2]);
    let mut binder = Binder::new(&pa, root_ge);
    binder.bsw = Some(String::new());
    assert!(binder.next(&mm).is_none());
    assert_eq!(binder.bsw.as_deref(), Some(""));

    let mut limit_base = BaseLogicalPlan::with_id(4, LogicalLimit::TYPE, 0);
    limit_base.set_children(vec![data_source(1, 1, None)]);
    let s1 = LogicalPlan::Limit(LogicalLimit::new(limit_base, 0, 0));
    let mut proj_base = BaseLogicalPlan::with_id(5, LogicalProjection::TYPE, 0);
    proj_base.set_children(vec![s1]);
    let p1 = LogicalPlan::Projection(LogicalProjection::new(proj_base, Vec::new()));
    let mut mm = Memo::new();
    mm.init(&p1).unwrap();
    let root_ge = *mm.group(mm.get_root_group().unwrap()).get_logical_expressions().last().unwrap();
    let mut binder = Binder::new(&pa, root_ge);
    binder.bsw = Some(String::new());
    assert!(binder.next(&mm).is_none());
    assert_eq!(binder.bsw.as_deref(), Some("GE:Limit_4{GID:1}\n"));
}

#[test]
fn binder_top_node_and_one_node() {
    let plan = join(3, vec![data_source(1, 1, None), data_source(2, 2, None)]);
    let mut mm = Memo::new();
    mm.init(&plan).unwrap();
    let pa = Pattern::new(Operand::Join, EngineTypeSet::ALL);
    let root_ge = *mm.group(mm.get_root_group().unwrap()).get_logical_expressions().last().unwrap();
    let mut binder = Binder::new(&pa, root_ge);
    let holder = binder.next(&mm).unwrap();
    assert_eq!(get_operand(mm.ge(holder.ge).get_wrapped_logical_plan()), Operand::Join);

    let lone = join(1, Vec::new());
    let mut mm = Memo::new();
    mm.init(&lone).unwrap();
    assert_eq!(mm.get_groups().len(), 1);
    let root_ge = *mm.group(mm.get_root_group().unwrap()).get_logical_expressions().last().unwrap();
    let mut binder = Binder::new(&pa, root_ge);
    let holder = binder.next(&mm).unwrap();
    assert_eq!(get_operand(mm.ge(holder.ge).get_wrapped_logical_plan()), Operand::Join);
}

#[test]
fn binder_sub_tree_match() {
    let join1 = join(3, vec![data_source(1, 1, None), data_source(2, 2, None)]);
    let join2 = join(6, vec![data_source(4, 4, None), data_source(5, 5, None)]);
    let join3 = join(7, vec![join1, join2]);
    let mut mm = Memo::new();
    mm.init(&join3).unwrap();
    assert_eq!(mm.get_groups().len(), 7);
    let mut pa = Pattern::new(Operand::Join, EngineTypeSet::ALL);
    pa.set_children(vec![
        Pattern::new(Operand::Join, EngineTypeSet::ALL),
        Pattern::new(Operand::Join, EngineTypeSet::ALL),
    ]);
    let root_ge = *mm.group(mm.get_root_group().unwrap()).get_logical_expressions().last().unwrap();
    let mut binder = Binder::new(&pa, root_ge);
    let holder = binder.next(&mm).unwrap().clone();
    assert!(std::ptr::eq(mm.ge(holder.ge).get_wrapped_logical_plan(), &join3));
    assert!(std::ptr::eq(
        mm.ge(holder.children[0].as_ref().unwrap().ge).get_wrapped_logical_plan(),
        &join3.children()[0]
    ));
    assert!(std::ptr::eq(
        mm.ge(holder.children[1].as_ref().unwrap().ge).get_wrapped_logical_plan(),
        &join3.children()[1]
    ));
    assert!(binder.next(&mm).is_none());
    let mut pa2 = Pattern::new(Operand::Join, EngineTypeSet::ALL);
    pa2.set_children(vec![
        Pattern::new(Operand::DataSource, EngineTypeSet::ALL),
        Pattern::new(Operand::DataSource, EngineTypeSet::ALL),
    ]);
    let mut binder = Binder::new(&pa2, root_ge);
    assert!(binder.next(&mm).is_none());
}

fn multi_memo<'a>(join1: &'a LogicalPlan, t3: &'a LogicalPlan, t4: &'a LogicalPlan) -> (Memo<'a>, usize) {
    let mut mm = Memo::new();
    let ge = mm.init(join1).unwrap();
    let inputs = mm.ge(ge).inputs.clone();
    mm.copy_in(Some(inputs[0]), CopyInPlan::Tree(t3)).unwrap();
    mm.copy_in(Some(inputs[1]), CopyInPlan::Tree(t4)).unwrap();
    (mm, ge)
}

fn bound(mm: &Memo<'_>, binder: &mut Binder<'_>) -> Option<(String, String)> {
    let holder = binder.next(mm)?.clone();
    assert_eq!(get_operand(mm.ge(holder.ge).get_wrapped_logical_plan()), Operand::Join);
    Some((
        alias(mm, holder.children[0].as_ref().unwrap().ge),
        alias(mm, holder.children[1].as_ref().unwrap().ge),
    ))
}

#[test]
fn binder_multi_next() {
    let join1 = join(3, vec![data_source(1, 1, Some("t1")), data_source(2, 2, Some("t2"))]);
    let t3 = data_source(4, 4, Some("t3"));
    let t4 = data_source(5, 5, Some("t4"));
    let (mm, ge) = multi_memo(&join1, &t3, &t4);
    let mut pa = Pattern::new(Operand::Join, EngineTypeSet::ALL);
    pa.set_children(vec![
        Pattern::new(Operand::DataSource, EngineTypeSet::ALL),
        Pattern::new(Operand::DataSource, EngineTypeSet::ALL),
    ]);
    let mut binder = Binder::new(&pa, ge);
    binder.bsw = Some(String::new());
    let pairs: Vec<_> = std::iter::from_fn(|| bound(&mm, &mut binder)).collect();
    let want = [("t1", "t2"), ("t1", "t4"), ("t3", "t2"), ("t3", "t4")];
    assert_eq!(
        pairs,
        want.iter().map(|(a, b)| ((*a).to_owned(), (*b).to_owned())).collect::<Vec<_>>()
    );
    assert!(binder.bsw.as_deref().unwrap().starts_with(
        "GE:DataSource_1{} -> GE:DataSource_2{}\n\
         GE:DataSource_1{} -> GE:DataSource_5{}\n\
         GE:DataSource_1{}\n\
         GE:DataSource_4{} -> GE:DataSource_2{}\n\
         GE:DataSource_4{} -> GE:DataSource_5{}\n"
    ));
}

#[test]
fn binder_any_matches_a_group_once() {
    let join1 = join(3, vec![data_source(1, 1, Some("t1")), data_source(2, 2, Some("t2"))]);
    let t3 = data_source(4, 4, Some("t3"));
    let t4 = data_source(5, 5, Some("t4"));
    let (mm, ge) = multi_memo(&join1, &t3, &t4);
    let mut pa = Pattern::new(Operand::Join, EngineTypeSet::ALL);
    pa.set_children(vec![
        Pattern::new(Operand::DataSource, EngineTypeSet::ALL),
        Pattern::new(Operand::Any, EngineTypeSet::ALL),
    ]);
    let mut binder = Binder::new(&pa, ge);
    binder.bsw = Some(String::new());
    assert_eq!(bound(&mm, &mut binder), Some(("t1".to_owned(), "t2".to_owned())));
    assert_eq!(bound(&mm, &mut binder), Some(("t3".to_owned(), "t2".to_owned())));
    assert_eq!(bound(&mm, &mut binder), None);
    assert_eq!(
        binder.bsw.as_deref(),
        Some(
            "GE:DataSource_1{} -> GE:DataSource_2{}\n\
             GE:DataSource_1{}\n\
             GE:DataSource_4{} -> GE:DataSource_2{}\n\
             GE:DataSource_4{}\n"
        )
    );

    let mut pa = Pattern::new(Operand::Join, EngineTypeSet::ALL);
    pa.set_children(vec![
        Pattern::new(Operand::Any, EngineTypeSet::ALL),
        Pattern::new(Operand::Any, EngineTypeSet::ALL),
    ]);
    let mut binder = Binder::new(&pa, ge);
    binder.bsw = Some(String::new());
    assert_eq!(bound(&mm, &mut binder), Some(("t1".to_owned(), "t2".to_owned())));
    assert_eq!(bound(&mm, &mut binder), None);
    assert_eq!(
        binder.bsw.as_deref(),
        Some("GE:DataSource_1{} -> GE:DataSource_2{}\nGE:DataSource_1{}\n")
    );
}

// ***** task/task_test.go, task/task_scheduler_test.go *****

struct TestTaskImpl(i64);

impl Task for TestTaskImpl {
    fn execute(&self, _ctx: &mut Context<'_>) -> Result<(), PlanError> {
        if self.0 == 2 {
            return Err(PlanError::internal("mock error at task id = 2"));
        }
        Ok(())
    }

    fn desc(&self, _ctx: &Context<'_>, w: &mut String) {
        w.push_str(&self.0.to_string());
    }
}

#[test]
fn task_stack_is_last_in_first_out() {
    let allocator = PlanIdAllocator::new();
    let ctx = Context::new(&allocator);
    let mut ts = Stack::with_capacity(4);
    ts.push(Box::new(TestTaskImpl(1)));
    ts.push(Box::new(TestTaskImpl(2)));
    for want in ["2", "1"] {
        let mut w = String::new();
        ts.pop().unwrap().desc(&ctx, &mut w);
        assert_eq!(w, want);
    }
    assert!(ts.pop().is_none());
    for a in 3..=6 {
        ts.push(Box::new(TestTaskImpl(a)));
    }
    assert_eq!(ts.len(), 4);
    for want in ["6", "5", "4", "3"] {
        let mut w = String::new();
        ts.pop().unwrap().desc(&ctx, &mut w);
        assert_eq!(w, want);
    }
    assert!(ts.empty());
}

#[test]
fn simple_task_scheduler_stops_at_the_first_error() {
    let allocator = PlanIdAllocator::new();
    let mut ctx = Context::new(&allocator);
    ctx.scheduler = SimpleTaskScheduler::new();
    for a in 1..=3 {
        ctx.push_task(Box::new(TestTaskImpl(a)));
    }
    let err = super::task::execute_tasks(&mut ctx).unwrap_err();
    assert!(err.to_string().contains("mock error at task id = 2"), "{err}");
}

// ***** pattern/pattern_test.go, pattern/engine_test.go *****

#[test]
fn get_operand_and_match() {
    assert_eq!(get_operand(&join(1, Vec::new())), Operand::Join);
    assert_eq!(get_operand(&data_source(1, 1, None)), Operand::DataSource);
    assert_eq!(get_operand(&projection_one()), Operand::Projection);
    assert!(Operand::Any.matches(Operand::Join));
    assert!(Operand::Join.matches(Operand::Any));
    assert!(Operand::Join.matches(Operand::Join));
    assert!(!Operand::Join.matches(Operand::Selection));
    let mut p = Pattern::new(Operand::Join, EngineTypeSet::TIDB_ONLY);
    assert!(p.matches(Operand::Join, EngineType::TIDB));
    assert!(!p.matches(Operand::Join, EngineType::TIKV));
    p.set_children(vec![Pattern::new(Operand::Any, EngineTypeSet::ALL)]);
    assert_eq!(p.children.len(), 1);
    assert!(p.children[0].match_operand_any(EngineType::TIFLASH));
}

#[test]
fn engine_type_set_contains() {
    assert!(EngineTypeSet::TIDB_ONLY.contains(EngineType::TIDB));
    assert!(!EngineTypeSet::TIDB_ONLY.contains(EngineType::TIKV));
    assert!(EngineTypeSet::TIKV_OR_TIFLASH.contains(EngineType::TIFLASH));
    assert!(!EngineTypeSet::TIKV_OR_TIFLASH.contains(EngineType::TIDB));
    assert!(EngineTypeSet::ALL.contains(EngineType::TIKV));
    assert_eq!(EngineType::TIFLASH.as_str(), "EngineTiFlash");
}

/// Go `TestIteratorLogicalPlan`: `G1{join1}` over `G2{t1,t3}` and
/// `G3{t2,t4}` yields the four plans, the right input advancing first, each
/// with Go's plan-id hash.
#[test]
fn iterator_logical_plan_enumerates_every_combination() {
    let join1 = join(3, vec![data_source(1, 1, Some("t1")), data_source(2, 2, Some("t2"))]);
    let t3 = data_source(4, 4, Some("t3"));
    let t4 = data_source(5, 5, Some("t4"));
    let (mm, _) = multi_memo(&join1, &t3, &t4);
    let hash_of = |ids: &[u64], id: i64| {
        let mut h = new_hash_equaler();
        for one in ids {
            tidb_planner_cascades_base::Hasher::hash_uint64(&mut h, *one);
        }
        tidb_planner_cascades_base::Hasher::hash_int(&mut h, id);
        tidb_planner_cascades_base::Hasher::sum64(&h)
    };
    let mut got = Vec::new();
    mm.new_iterator().each(|plan| {
        let lhs = &plan.children[0];
        let rhs = &plan.children[1];
        assert_eq!(plan.plan_ids_hash, hash_of(&[lhs.plan_ids_hash, rhs.plan_ids_hash], 3));
        got.push((alias(&mm, lhs.ge), alias(&mm, rhs.ge), lhs.plan_ids_hash, rhs.plan_ids_hash));
        true
    });
    let want = [("t1", 1, "t2", 2), ("t1", 1, "t4", 5), ("t3", 4, "t2", 2), ("t3", 4, "t4", 5)];
    assert_eq!(
        got,
        want.iter()
            .map(|(l, lid, r, rid)| ((*l).to_owned(), (*r).to_owned(), hash_of(&[], *lid), hash_of(&[], *rid)))
            .collect::<Vec<_>>()
    );
}
