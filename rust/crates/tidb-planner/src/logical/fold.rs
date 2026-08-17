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

//! The owned post-order rewrite fold that every logical optimization rule runs
//! on.
//!
//! # Why this exists
//!
//! [`crate::logical`]'s module header states the constraint on measured
//! grounds: a recursive `match` walk over this tree survives roughly 30,000
//! levels on a 2 MiB stack and aborts by 50,000, so every walk the module
//! offers uses an EXPLICIT STACK. But the walks it offered before this module
//! —- `walk_preorder`, `plan_count`, `max_depth`, `dismantle` -— are all
//! read-only or teardown, and NO optimization rule has that shape.
//!
//! Go's rule bodies are owned post-order rewrites. Every one of them reads
//!
//! ```go
//! PredicatePushDown([]expression.Expression) ([]expression.Expression, LogicalPlan, error)
//! PruneColumns([]*expression.Column) (LogicalPlan, error)
//! PushDownTopN(base.LogicalPlan) base.LogicalPlan
//! ```
//!
//! — a node consumes itself, rewrites its children first, and MOVES a
//! (possibly different) node back out. In Rust that is `fn(self) -> Self`, and
//! an explicit-stack version of it is materially harder than a read-only
//! visitor: while a child is being rewritten, its parent has already been
//! partly consumed (its children taken) and has to be PARKED somewhere.
//!
//! Rather than have each of the ~20 rules hand-roll that parking — which is
//! exactly how the recursion ban gets violated twenty times — the parking
//! happens once, here. A rule implements [`OwnedRewrite`] and calls
//! [`fold_owned`]; it never writes a stack, a recursion, or a `Vec` of
//! half-built parents.
//!
//! # The shape, and why it is this shape
//!
//! Every Go rule body decomposes the same way:
//!
//! * a DESCENDING half that runs before the children are touched and decides
//!   what each child is asked to do — the predicates a `LogicalSelection`
//!   forwards, the used-column set a `LogicalJoin` splits per side, the
//!   `LogicalTopN` a `LogicalLimit` hands down;
//! * an ASCENDING half that runs after the children have been replaced and
//!   decides what this node becomes and what it reports upward — a
//!   `LogicalSelection` that collapses to its child, a `LogicalJoin` that
//!   wraps a leftover predicate back into a `LogicalSelection`, a node that
//!   re-attaches the `LogicalTopN` above itself.
//!
//! So [`OwnedRewrite`] has exactly those two methods, with a `Down` value
//! flowing in and a `Up` value flowing out. [`Descend::Stop`] is the third
//! thing Go's bodies do: `DataSource`, `LogicalCTE` and `LogicalTableDual`
//! answer without recursing at all.
//!
//! # How it avoids recursion
//!
//! Two explicit `Vec`s and no self-calls:
//!
//! * `work` holds [`Task`]s. `Task::Enter` carries a node that has not been
//!   descended into yet, together with its `Down`. `Task::Exit` carries the
//!   PARKED parent — the node with its children already moved out — plus how
//!   many results to collect for it.
//! * `done` holds finished `(node, Up)` pairs, in completion order. A parked
//!   parent claims its children by splitting the last `n` entries off `done`.
//!
//! Entering a node pushes one `Exit` then one `Enter` per child in reverse, so
//! the children pop in order and the parent pops after all of them. Depth
//! costs heap, not stack. See [`fold_owned`] for the loop.
//!
//! # Fallibility: the fold is infallible on purpose
//!
//! Go's `PruneColumns` and `PredicatePushDown` return an `error`, which in Go
//! is free — the caller still holds the plan pointer. In a by-value IR it is
//! not free: a `?` inside this loop would drop `work` and `done` together,
//! destroying every half-rebuilt subtree, and the caller would be left with
//! neither the old plan nor the new one.
//!
//! So [`OwnedRewrite::descend`] and [`OwnedRewrite::ascend`] CANNOT fail. A
//! rule that hits Go's error path records it in its own `&mut self` state
//! (see [`RewriteFailure`]) and returns a structurally valid node anyway; the
//! fold always completes and always yields a whole tree, and the rule surfaces
//! the failure afterwards. [`crate::logical::rule::LogicalOptRule::optimize`]
//! then returns `Err((plan, err))` — the plan is handed back even on the error
//! path, which is the by-value equivalent of Go still holding its pointer.

use crate::plan_base::PlanError;

use super::LogicalPlan;

/// What [`OwnedRewrite::descend`] decided about a node's children.
#[derive(Debug)]
pub enum Descend<D, U> {
    /// Rewrite the children. The vector has ONE entry per child, in child
    /// order; a shorter vector leaves the trailing children un-rewritten with
    /// no `Down` at all, which is a rule bug, so [`fold_owned`] requires the
    /// lengths to match and pads with a clone-free `Default` otherwise.
    Children(Vec<D>),
    /// Answer without touching the children at all. Go's `DataSource`,
    /// `LogicalCTE` and `LogicalTableDual` bodies do this.
    Stop(U),
}

/// A post-order owned rewrite of a [`LogicalPlan`] tree.
///
/// Implementors carry their own mutable state (a session flag, a collected
/// failure, a change bit) in `Self`; the fold borrows them mutably for the
/// whole walk.
pub trait OwnedRewrite {
    /// What a parent asks of a child. Go's rule argument.
    type Down;
    /// What a child reports to its parent. Go's rule's non-plan return.
    type Up;

    /// Runs before the children are rewritten, with the node still whole
    /// except that its children have not yet been taken.
    ///
    /// The node may be mutated freely here; this is where Go's bodies rewrite
    /// their own condition lists and schemas.
    fn descend(
        &mut self,
        node: &mut LogicalPlan,
        down: Self::Down,
    ) -> Descend<Self::Down, Self::Up>;

    /// Runs after the children have been rewritten and put back, and returns
    /// this node's replacement plus what it reports upward.
    ///
    /// `child_ups` is in child order and has one entry per child.
    fn ascend(&mut self, node: LogicalPlan, child_ups: Vec<Self::Up>) -> (LogicalPlan, Self::Up);
}

/// A first-failure slot, the by-value replacement for Go's `error` return.
///
/// A rule owns one of these, writes the FIRST failure it sees into it, and
/// keeps rewriting so the tree survives. See this module's header.
#[derive(Debug, Default)]
pub struct RewriteFailure {
    failure: Option<PlanError>,
}

impl RewriteFailure {
    /// Records `error` unless a failure is already recorded.
    pub fn record(&mut self, error: PlanError) {
        if self.failure.is_none() {
            self.failure = Some(error);
        }
    }

    /// Whether any failure was recorded.
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        self.failure.is_some()
    }

    /// Takes the recorded failure, if any.
    #[must_use]
    pub fn take(&mut self) -> Option<PlanError> {
        self.failure.take()
    }
}

/// A parked unit of work; see the module header.
enum Task<D> {
    /// A node that has not been descended into, with what its parent asked.
    Enter(LogicalPlan, D),
    /// A node whose children have been MOVED OUT and which is waiting for
    /// `usize` results to appear on the `done` stack.
    Exit(LogicalPlan, usize),
}

/// Rewrites `root` bottom-up with `rewrite`, without recursion.
///
/// Returns the rewritten root and the `Up` it reported.
///
/// `Down` must be `Default` only so that a [`Descend::Children`] vector that
/// is shorter than the child list can be padded rather than panicking; a
/// correct rule never triggers that path.
pub fn fold_owned<R>(rewrite: &mut R, root: LogicalPlan, down: R::Down) -> (LogicalPlan, R::Up)
where
    R: OwnedRewrite,
    R::Down: Default,
{
    let mut work: Vec<Task<R::Down>> = vec![Task::Enter(root, down)];
    let mut done: Vec<(LogicalPlan, R::Up)> = Vec::new();

    while let Some(task) = work.pop() {
        match task {
            Task::Enter(mut node, down) => {
                match rewrite.descend(&mut node, down) {
                    Descend::Stop(up) => done.push((node, up)),
                    Descend::Children(mut downs) => {
                        // Park the parent WITHOUT its children; the children
                        // are pushed as their own work items. This is the step
                        // a read-only visitor never has to take.
                        let children = node.base_mut().take_children();
                        let child_count = children.len();
                        downs.resize_with(child_count, R::Down::default);
                        work.push(Task::Exit(node, child_count));
                        for (child, child_down) in children.into_iter().zip(downs).rev() {
                            work.push(Task::Enter(child, child_down));
                        }
                    }
                }
            }
            Task::Exit(mut node, child_count) => {
                let finished = done.split_off(done.len() - child_count);
                let mut children = Vec::with_capacity(child_count);
                let mut ups = Vec::with_capacity(child_count);
                for (child, up) in finished {
                    children.push(child);
                    ups.push(up);
                }
                node.set_children(children);
                let (replacement, up) = rewrite.ascend(node, ups);
                done.push((replacement, up));
            }
        }
    }

    done.pop()
        .unwrap_or_else(|| unreachable!("fold_owned always finishes with exactly one result"))
}
