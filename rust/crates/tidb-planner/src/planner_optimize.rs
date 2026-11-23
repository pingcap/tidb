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

//! Go `pkg/planner/optimize.go`'s `optimize`: the statement's first
//! logical-plan round and, under `tidb_opt_enable_alternative_logical_plans`,
//! the alternative rounds (`alternativeRounds`) the first round's signals
//! ask for; the cheapest round's plan is the statement's
//! (`buildAndOptimizeLogicalPlanRound`).
//!
//! One round -- Go's `buildLogicalPlan`, `CheckPrivilege` /
//! `CheckTableLock` and `core.DoOptimize` with the plan's cost -- is the
//! caller's [`LogicalPlanRounds::build_and_optimize`], over the statement's
//! catalog and its subquery evaluation.

use std::sync::atomic::{AtomicBool, Ordering};

use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_logicalop::LogicalPlan;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_sessionctx::StmtContext;

use crate::optimize::OptimizeContext as _;

/// The statement's rounds as Go's `optimize` runs them.
pub trait LogicalPlanRounds {
    /// One build of the statement's plan under `ctx` with Go `adjustFlag`
    /// applied to the builder's optimization flag: Go `buildLogicalPlan`,
    /// the statement's privilege check (once), and `core.DoOptimize`, with
    /// the physical plan's cost.
    fn build_and_optimize(
        &mut self,
        ctx: &StmtContext,
        opt_flag_adjust: fn(u64) -> u64,
    ) -> Result<(LogicalPlan, PhysicalPlan, f64), PlanError>;

    /// Go `buildLogicalPlan` starts every round's `PlanID`, `PlanColumnID`
    /// and `MapScalarSubQ` where the first round started: remembers them.
    fn save_round_start(&mut self);

    /// Puts the plan and column ids and the scalar subqueries back where the
    /// first round started.
    fn restore_round_start(&mut self);
}

/// Go `alternativeRound`: one more build of the statement's plan that
/// `optimize` costs against the first.
pub struct AlternativeRound {
    /// Go `name`.
    pub name: &'static str,
    /// Go `adjustFlag`.
    pub adjust_flag: fn(u64) -> u64,
    /// Go `enabled`, read off the signals the first round left.
    pub enabled: fn(&StmtContext) -> bool,
    /// Go `setup` and the cleanup it returns: the context the round builds
    /// under, which shares the statement's state, and what to undo on the
    /// statement once the round is over.
    #[allow(clippy::type_complexity)]
    pub setup: for<'a> fn(&'a StmtContext) -> (StmtContext, Option<Box<dyn FnOnce() + 'a>>),
}

fn signal(signal: &AtomicBool) -> bool {
    signal.load(Ordering::Acquire)
}

/// Go `shouldTryEngineRestrictedRounds`.
#[must_use]
pub fn should_try_engine_restricted_rounds(ctx: &StmtContext) -> bool {
    let signals = ctx.alternative_logical_plan_signals();
    signal(&signals.mixed_storage_engines)
        && !signal(&signals.has_store_type_hint)
        && !ctx.optimizer_cost_env().session.mpp_enforced
}

/// Go `alternativeRounds`, in the order `optimize` runs them.
pub const ALTERNATIVE_ROUNDS: [AlternativeRound; 7] = [
    AlternativeRound {
        name: "non-decorrelate",
        adjust_flag: |flag| flag & !tidb_planner_core_rule::flags::DECORRELATE,
        // Go `shouldTryNonDecorrelationRound`.
        enabled: |ctx| {
            let signals = ctx.alternative_logical_plan_signals();
            signal(&signals.decorrelated_apply) && !signal(&signals.same_order_index_join)
        },
        setup: |ctx| (ctx.clone(), None),
    },
    AlternativeRound {
        name: "order-aware-reorder",
        adjust_flag: |flag| flag | tidb_planner_core_rule::flags::ORDER_AWARE_JOIN_REORDER,
        // Go `shouldTryOrderAwareReorderRound`.
        enabled: |ctx| signal(&ctx.alternative_logical_plan_signals().order_aware_join_reorder),
        setup: |ctx| (ctx.clone(), None),
    },
    AlternativeRound {
        name: "correlate",
        adjust_flag: |flag| flag | tidb_planner_core_rule::flags::CORRELATE,
        // Go `shouldTryCorrelateRound`.
        enabled: |ctx| signal(&ctx.alternative_logical_plan_signals().prefer_correlate),
        setup: |ctx| (ctx.clone().with_enable_correlate_subquery(true), None),
    },
    AlternativeRound {
        name: "semi-join-rewrite",
        adjust_flag: |flag| flag,
        // Go `shouldTrySemiJoinRewriteRound`.
        enabled: |ctx| {
            signal(&ctx.alternative_logical_plan_signals().semi_join_rewrite) && !ctx.enable_semi_join_rewrite()
        },
        setup: |ctx| (ctx.clone().with_enable_semi_join_rewrite(true), None),
    },
    AlternativeRound {
        name: "fts-like-fallback",
        adjust_flag: |flag| flag,
        enabled: |ctx| {
            let signals = ctx.alternative_logical_plan_signals();
            signal(&signals.fts_like_fallback) || signal(&signals.has_predicate_context_match)
        },
        setup: |ctx| {
            let fts_like_fallback = &ctx.alternative_logical_plan_signals().fts_like_fallback;
            let saved = fts_like_fallback.swap(true, Ordering::AcqRel);
            (ctx.clone(), Some(Box::new(move || fts_like_fallback.store(saved, Ordering::Release))))
        },
    },
    AlternativeRound {
        name: "tikv-only",
        adjust_flag: |flag| flag,
        // Go `shouldTryTiKVOnlyRound`.
        enabled: should_try_engine_restricted_rounds,
        setup: |ctx| (ctx.clone().with_isolation_read_engines("tikv,tidb".to_owned()), None),
    },
    AlternativeRound {
        name: "tiflash-only",
        adjust_flag: |flag| flag,
        // Go `shouldTryTiFlashOnlyRound`.
        enabled: |ctx| {
            should_try_engine_restricted_rounds(ctx)
                && !signal(&ctx.alternative_logical_plan_signals().missing_tiflash_path)
                && ctx.optimizer_cost_env().session.mpp_allowed
        },
        setup: |ctx| (ctx.clone().with_isolation_read_engines("tiflash,tidb".to_owned()), None),
    },
];

/// Go `optimize`'s `bestPlan`, `bestCost` and `bestLogicalPlanCtx`.
struct BestPlan {
    logical: LogicalPlan,
    physical: PhysicalPlan,
    cost: f64,
    logical_plan_ctx: tidb_sessionctx::LogicalPlanBuildState,
}

/// Go `buildAndOptimizeLogicalPlanRound`: one build of the statement's plan,
/// kept in `best_plan` when it is the first or the cheapest so far.
fn build_and_optimize_logical_plan_round(
    ctx: &StmtContext,
    rounds: &mut dyn LogicalPlanRounds,
    best_plan: &mut Option<BestPlan>,
    opt_flag_adjust: fn(u64) -> u64,
) -> Result<(), PlanError> {
    let (logical, physical, cost) = rounds.build_and_optimize(ctx, opt_flag_adjust)?;
    let signals = ctx.alternative_logical_plan_signals();
    if signals.predicate_match.load(Ordering::Acquire) {
        signals.has_predicate_context_match.store(true, Ordering::Release);
    }
    // A predicate-context MATCH whose native form cannot run: the plan is no
    // candidate, and every round that follows rewrites it to ILIKE.
    if signals.non_viable_fts_match.load(Ordering::Acquire) {
        signals.fts_like_fallback.store(true, Ordering::Release);
        return Ok(());
    }
    if best_plan.as_ref().is_none_or(|best| cost < best.cost) {
        *best_plan = Some(BestPlan { logical, physical, cost, logical_plan_ctx: ctx.save_logical_plan_build_ctx() });
    }
    Ok(())
}

/// Go `optimize`'s logical-plan rounds: the first round, then each enabled
/// alternative round, the cheapest plan winning. Every alternative round is
/// off without `tidb_opt_enable_alternative_logical_plans`.
pub fn optimize(ctx: &StmtContext, rounds: &mut dyn LogicalPlanRounds) -> Result<(LogicalPlan, PhysicalPlan), PlanError> {
    if !ctx.enable_alternative_logical_plans() {
        return rounds.build_and_optimize(ctx, |flag| flag).map(|(logical, physical, _)| (logical, physical));
    }
    let initial_logical_plan_ctx = ctx.save_logical_plan_build_ctx();
    rounds.save_round_start();
    ctx.reset_alternative_logical_plan_signals();
    let signals = ctx.alternative_logical_plan_signals();
    let mut best_plan = None;
    build_and_optimize_logical_plan_round(ctx, rounds, &mut best_plan, |flag| flag)?;
    // The engine-restricted rounds arm on the shape of the first round's
    // chosen plan: only a plan that mixes TiKV and TiFlash reads leaves a
    // homogeneous alternative uncosted, and a single-scan index join is left
    // as the first round built it.
    if let Some(best) = &best_plan {
        let (has_tikv, has_tiflash) =
            tidb_planner_core_operator_physicalop::storage_engine_usage::storage_engine_usage(&best.physical);
        if has_tikv
            && has_tiflash
            && !tidb_planner_core_operator_physicalop::single_scan_index_join::has_single_scan_index_join(
                &best.physical,
            )
        {
            signals.mixed_storage_engines.store(true, Ordering::Release);
        }
    }
    // Which rounds run is decided by the first round's signals alone: a later
    // round's build sets signals too.
    let enabled_rounds: Vec<&AlternativeRound> = ALTERNATIVE_ROUNDS.iter().filter(|round| (round.enabled)(ctx)).collect();
    let mut last_alt_round_err = None;
    for alternative in enabled_rounds {
        ctx.restore_logical_plan_build_ctx(&initial_logical_plan_ctx);
        ctx.reset_alternative_logical_plan_marks();
        rounds.restore_round_start();
        let (round_ctx, cleanup) = (alternative.setup)(ctx);
        let result = build_and_optimize_logical_plan_round(&round_ctx, rounds, &mut best_plan, alternative.adjust_flag);
        if let Some(cleanup) = cleanup {
            cleanup();
        }
        if let Err(error) = result {
            // The session answers this one by retrieving the rows and
            // planning the statement again; it is no failure of the round.
            if matches!(error.kind(), tidb_planner_core_base::PlanErrorKind::MemTableRetrievalNeeded) {
                return Err(error);
            }
            // An alternative round is optional: the first round's plan
            // stands. The fts-like-fallback round's error is the statement's
            // when the first round's plan was discarded as non-executable.
            tracing::warn!(round = alternative.name, %error, "alternative logical plan round failed");
            last_alt_round_err = Some(error);
        }
    }
    let Some(best) = best_plan else {
        return Err(last_alt_round_err.unwrap_or_else(|| PlanError::internal("failed to build logical plan")));
    };
    ctx.restore_logical_plan_build_ctx(&best.logical_plan_ctx);
    Ok((best.logical, best.physical))
}
