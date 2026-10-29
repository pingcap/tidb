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

//! The index-lookup joins' inner side as Go's join package reaches it: the
//! `IndexJoinExecutorBuilder` interface the executor's data-reader builder
//! implements (`BuildExecutorForIndexJoin`), here the lookup reader a join
//! seeds with each outer batch's probes ([`IndexJoinLookup`]) and the
//! template a lane thread opens a task from ([`IndexJoinLookupFork`]).

use tidb_datatype::Datum;
use tidb_driver_error::ExecError;
use tidb_executor_internal_exec::Executor;
use tidb_expr::expression::Expression;

/// One outer batch's deduped probes plus the per-probe bound values the join
/// evaluated alongside them. `bound_values` is empty when the decision carries
/// no bounds; otherwise it aligns 1:1 with `keys`.
#[derive(Clone, Debug, Default)]
pub struct IndexJoinProbes {
    pub keys: Vec<Vec<Datum>>,
    pub bound_values: Vec<Vec<Datum>>,
}

/// One outer-derived comparison against the object-key column just past the
/// probe prefix -- Go's `ColWithCmpFuncManager` row. Where Go rebuilds a
/// dynamic range for the compared key column per lookup content
/// (`BuildRangesByRow` -> the range's last slot), this tier evaluates `arg`
/// over each outer row and extends that probe's point range with the result.
#[derive(Clone, Debug)]
pub struct LookupProbeBound {
    /// The compared expression, evaluated over one OUTER child's output row:
    /// every `Column.index` is already an outer-child offset.
    pub arg: Expression,
}

/// Probe values shared by a composite index-join inner subtree and its
/// target table leaf. The outer join publishes one batch at a time; the leaf
/// refreshes its lookup source lazily when the generation changes.
#[derive(Debug, Default)]
pub struct SharedIndexJoinProbes {
    probes: IndexJoinProbes,
    generation: u64,
}

impl SharedIndexJoinProbes {
    /// The probes of the latest published batch.
    #[must_use]
    pub fn probes(&self) -> &IndexJoinProbes {
        &self.probes
    }

    /// The publication count, which a leaf compares to its own to refresh.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Publishes one outer batch's probes.
    pub fn publish(&mut self, probes: IndexJoinProbes) {
        self.probes = probes;
        self.generation = self.generation.wrapping_add(1);
    }
}

/// One base-table lookup reader on an index join's inner side.
pub trait IndexJoinLookup: Executor {
    /// Seeds the reader with one outer batch's probes.
    fn set_probes(&mut self, probes: IndexJoinProbes) -> Result<(), ExecError>;

    /// The template a lane thread rebuilds this reader's task from, when the
    /// lookup can run per batch on its own thread.
    fn fork_template(&self) -> Option<Box<dyn IndexJoinLookupFork>>;

    /// Counts rows a lane produced on the reader's behalf.
    fn add_produced(&self, rows: u64);
}

/// The immutable lookup description a lane thread opens one task from.
pub trait IndexJoinLookupFork: Send + Sync {
    /// Opens the task for `probes`; probes that reach no range come back
    /// unopened for the session's own reader to serve.
    fn open(
        &self,
        probes: IndexJoinProbes,
    ) -> Result<Result<Box<dyn IndexJoinLookup>, IndexJoinProbes>, ExecError>;
}
