# Planner Maintainer Guide

This doc records non-obvious design choices, invariants, and maintainer workflows
for TiDB's planner/optimizer.

It intentionally:
- prioritizes relatively complex logic and explains why it is designed this way
- skips content that is easiest to learn by reading the code
- stays concise and action-oriented for maintainers/reviewers/oncall

Status: describes current behavior as implemented in `pkg/planner/` and its
boundary modules (`pkg/executor/compiler.go`, `pkg/planner/optimize.go`,
`pkg/planner/core/preprocess.go`, `pkg/planner/core/plan_cache.go`,
`pkg/planner/core/optimizer.go`). If you change a correctness fence (plan cache,
schema snapshot rules, binding/hint behavior), update this doc in the same patch.

Hard rule:
- Use this guide as a map, but confirm conclusions against code/tests.

## Contents
1. Scope And Non-Negotiable Invariants
2. Responsibility Boundaries (Where Changes Belong)
3. Planning Mental Model (Statement Scope + Hidden State)
4. Plan Cache Invariants (Prepared / Non-Prepared / Instance)
5. Optimizer Engine Invariants (Volcano vs Cascades)
6. Debugging Anchors And Common Failure Modes
7. Maintainer Playbooks (Common Change Types)
8. Glossary

## 1. Scope And Non-Negotiable Invariants

This guide covers:
- statement planning policy and wiring: `pkg/planner/optimize.go`
- preprocess (name resolution + schema snapshot selection): `pkg/planner/core/preprocess.go`
- plan cache safety boundary (fences, keying, reuse/adjust): `pkg/planner/core/plan_cache.go`
- optimizer engines and shared finalization: `pkg/planner/core/optimizer.go`

It intentionally skips:
- parser and AST shapes (`pkg/parser/*`)
- executor/runtime semantics (`pkg/executor/*`, `pkg/expression/*`)
- statistics collection lifecycle (owned by `pkg/statistics/*` / `pkg/domain/*`)

Top invariants a planner maintainer must not break:
1. A statement MUST be planned against exactly one InfoSchema snapshot.
2. Plan cache reuse MUST be fenced by schema/version checks, MDL, and per-exec
   adjustment (param typing + range rebuild).
3. Plan cache key/caching rules MUST treat "plan meaning" as the boundary: any
   plan-affecting input must be reflected in the key or ruled non-cacheable.
4. Cached plans MUST NOT leak mutable state across sessions; instance plan cache
   requires cloning and "read-only cached value" semantics.
5. Volcano vs Cascades MUST be semantically equivalent (shape may differ; result
   set must not).

Other planner sharp edges (easy to break accidentally):
- Preprocess MUST be safe to re-run; plan-cache reuse may trigger re-preprocess
  when schema fences fail.
- Planner code that mutates an AST MUST restore it (or operate on a copy);
  leaking mutations changes later planning steps and can corrupt plan-cache
  reuse/binding match behavior.

Maintainer preflight (write these down before touching plan cache behavior):
- Which axis are you changing: prepared / non-prepared / instance plan cache?
- Are you changing plan meaning? (cache key inputs, cacheability rules, per-exec
  adjustment, bindings/hints, schema snapshot selection)
- What are the correctness fences you rely on (schema meta version, per-table
  revision, MDL, privilege/table-lock checks)?
- Does your change introduce mutable state inside cached values? If yes, how is
  it isolated (session-only) or cloned (instance cache)?
- What is the minimal regression test: execute twice with different parameters,
  execute across DDL, and (for instance cache) cross-session coverage.

## 2. Responsibility Boundaries (Where Changes Belong)

Keep these boundaries sharp; it makes correctness reviewable.

- `pkg/executor/compiler.go`: statement boundary orchestration; MUST NOT grow
  planner logic.
- `pkg/planner/core/preprocess.go`: resolve + validation that depends on schema
  snapshot/staleness/session variables; produces the InfoSchema used for the
  statement.
- `pkg/planner/optimize.go`: statement-level planning policy:
  - picking cache frontend (prepared vs non-prepared)
  - hint parsing + binding selection
  - fast-plan attempts
- `pkg/planner/core/plan_cache.go`: plan-cache backend and safety fences:
  - schema/MDL fencing
  - cache key construction + lookup
  - hit adjustment / miss generation
- `pkg/planner/core/plan_cache_utils.go`: cache key/value encoding utilities,
  param-type extraction, clone helpers (treat as correctness-critical).
- `pkg/planner/core/plan_cacheable_checker.go`: cacheability rules (AST-level and
  plan-shape-level). Keep conservative.

Rule of thumb:
- If an input can change plan meaning, it MUST appear in preprocess, cache key,
  or cacheability rules. Do not hide it in incidental globals or mutable fields.

## 3. Planning Mental Model (Statement Scope + Hidden State)

Planner execution is statement-scoped, but several "hidden" cross-statement
mechanisms exist:
- plan cache (prepared/non-prepared/instance) introduces correctness-sensitive
  reuse across executions.
- bindings/hints can make plan selection depend on persisted metadata.
- schema can change concurrently (DDL), so reuse must be fenced.

One-minute flow (ignoring details):
1. `pkg/executor/compiler.go` picks a schema snapshot and calls planner.
2. `pkg/planner/optimize.go` applies hints/bindings, and then either:
   - goes through plan cache (EXECUTE or eligible non-prepared), or
   - builds/optimizes a fresh plan.
3. On a plan-cache hit, planner MUST still validate fences and rebuild
   param-dependent pieces; reuse is never a blind memoization.

## 4. Plan Cache Invariants (Prepared / Non-Prepared / Instance)

The plan cache is a correctness boundary. The common failure mode is treating it
as a pure performance layer and missing a "plan meaning" input or fence.

### 4.0 Jump Table (Start Here When Debugging/Reviewing)

Plan cache is intentionally layered:
- Frontend: prepares the statement carrier (`PlanCacheStmt`) and inputs for lookup
  (AST, parameter markers/values, resolve context).
- Backend: applies correctness fences, does lookup, hit adjustment, and miss handling.

Frontend entrypoints:
- Prepared statements:
  - PREPARE builds `PlanCacheStmt` (AST + resolve/visit infos + schema fences)
  - EXECUTE routes to the backend with the `PlanCacheStmt` and current params
- Non-prepared statements:
  - parameterize + restore AST to get parameterized SQL
  - build/reuse a per-session `PlanCacheStmt` for that parameterized SQL
  - route to the same backend

- Backend entrypoint (shared): `pkg/planner/core/plan_cache.go` (`GetPlanFromPlanCache`)
- Preprocess fences on reuse: `pkg/planner/core/plan_cache.go` (`planCachePreprocess`)
- Cache key: `pkg/planner/core/plan_cache_utils.go` (`NewPlanCacheKey`)
- Cacheability rules:
  - prepared AST: `pkg/planner/core/plan_cacheable_checker.go` (`IsASTCacheable`)
  - non-prepared: `pkg/planner/core/plan_cacheable_checker.go` (`NonPreparedPlanCacheableWithCtx`)
  - plan-shape: `pkg/planner/core/plan_cacheable_checker.go` (`isPlanCacheable`)
- Hit adjustment (ranges): `pkg/planner/core/plan_cache_rebuild.go` (`RebuildPlan4CachedPlan`)
- Non-prepared parameterization: `pkg/planner/core/plan_cache_param.go` (`GetParamSQLFromAST`)
- Cache implementations:
  - session cache (LRU): `pkg/planner/core/plan_cache_lru.go`
  - instance cache (node-level): `pkg/planner/core/plan_cache_instance.go`

### 4.1 Two Cached Objects: `PlanCacheStmt` vs `PlanCacheValue`

There are two different "cached things". Conflating them causes subtle bugs.

- `PlanCacheStmt` (statement carrier; reused across executions):
  - owns the prepared AST, resolve context, visit infos (privilege/table-lock
    checks), parameter markers, and schema fences (schema version + per-table
    revisions).
  - may include short-path caches (e.g. PointGet executor reuse) that MUST be
    cleared when schema changes.
- `PlanCacheValue` (plan carrier; stored in the plan cache):
  - owns the physical plan and its metadata for a particular `(cacheKey,
    paramTypes)` match.

Non-obvious mutability rule:
- Session plan cache: cached plan is session-owned; it may contain mutable state
  as long as it cannot escape the session.
- Instance plan cache: cached value MUST be treated as read-only and MUST be a
  clone; every execution clones again before using it.

Why this exists:
- Session cache optimizes for simplicity and speed inside a single goroutine-ish
  session context.
- Instance cache is shared across sessions; any mutable state becomes a data
  race or cross-session correctness leak.

### 4.2 Reuse Fences: What MUST Be Validated On Every Hit

On a hit, planner MUST validate (and may need to refresh) these fences before
returning the plan:

- Schema fencing:
  - InfoSchema meta version matches.
  - Per-table revision fencing matches (single-table changes must invalidate).
  - In some cases (e.g. `READ COMMITTED` / `FOR UPDATE`), reuse must be sensitive
    to the latest domain schema version, not only the prepare-time one.
- MDL fencing:
  - required metadata locks for referenced tables are acquired/validated.
- Authorization fencing:
  - privilege checks and table-lock checks must still be enforced per execution
    (with a known PointGet-executor reuse exception; see 4.6).

Workflow invariant:
- If schema fencing fails, plan cache preprocess can re-run `Preprocess` to
  refresh resolve context and schema fences. Any new preprocess behavior must
  remain idempotent under retries.

Why this exists:
- Plan reuse without these fences can produce wrong results, panic (schema
  mismatch), or privilege bypass.

### 4.3 Cache Key Design: "Plan Meaning" MUST Be In The Key

Rule: all information that can change plan meaning MUST be represented in the
cache key, or caching MUST be disabled for statements affected by that input.

High-signal groups that are intentionally part of the key (non-exhaustive):
- identity: user/host (do not share plans across users), current DB, statement
  text (parameterized text for both prepared and non-prepared)
- effective hints/bindings: matched binding SQL must be reflected
- schema fences: schema meta version + per-table revision map; partition prune
  mode is part of the key
- session environment: SQL mode, time zone/offset, connection collation/charset,
  isolation read engines, `sql_select_limit`, restricted SQL marker, foreign-key
  checks, and other knobs that can alter plan selection/semantics
- runtime semantics state: "dirty table" markers (union scan) and transaction
  flags that change locking/reads
- optional freshness invalidation: when enabled, a stats-version hash is
  included so fresh stats naturally force a miss

Design intent:
- "key completeness" is the safety net; cacheability rules are the coarse filter.
- If you add a new plan-affecting session var / hint / binding behavior, you
  must explicitly decide: key it, or forbid reuse (and test that decision).

### 4.4 Per-Execution Adjustment: Param Types + Range Rebuild

Plan cache lookup matches by `(cacheKey, paramTypes)`:
- the same `cacheKey` can hold multiple `PlanCacheValue`s for different
  parameter type vectors.
- param type extraction differs by protocol (binary vs text-protocol user vars);
  be careful when changing typing rules.

Even on a hit, planner MUST rebuild param-dependent ranges for current values
before executing:
- scan ranges (table/index), point-get/batch-point-get ranges, index-join ranges
  must be regenerated.
- wrapper plans such as `SelectInto` must be unwrapped to their target plan for
  cacheability checks, plan digest, and range rebuild; the executor still
  receives the wrapper plan so statement semantics are preserved.
- hidden stored-routine prepared templates may need statement-visible warnings
  produced while building the cached plan (for example constant-fold side
  effects) to be replayed on cache hits so warning handlers observe the same
  warnings on hit and miss paths. Plain prepared statements currently preserve
  their historical behavior and do not replay those warnings on hits.
- if rebuild fails or hits a safety fallback, the cached plan is treated as
  unusable and the execution falls back to "miss" behavior.

Why this exists:
- the cached plan is a template; ranges are execution-specific and must not be
  reused blindly.

### 4.5 Non-Prepared Plan Cache: Parameterization Must Be Reversible

Non-prepared plan cache is a text-protocol optimization that reuses the same
backend (`GetPlanFromPlanCache`) after turning constants into parameters.

Invariants:
- Parameterization MUST NOT leave mutations on the original AST. If you need to
  mutate, you must restore (or operate on a copied AST).
- The stored-routine-specific rules below apply only when
  `tidb_enable_sp_plan_cache=ON`; with the switch OFF, routine internal SQL
  stays on the plain execution path.
- Stored routine parameterization MUST respect SQL name-resolution boundaries;
  for example, alias-sensitive `GROUP BY` / `HAVING` / `ORDER BY` references
  should fall back rather than being rewritten before the planner can
  disambiguate aliases and routine vars.
- Stored routine `LIMIT` routine variables MUST fall back; prepared-parameter
  semantics reject values like `-1` and bypass the routine-only mutability
  checks that plain procedure execution applies.
- Stored routine `ENUM` / `SET` variables MUST fall back for now; the hidden
  prepared `EXECUTE` path can lose scalar-result metadata for those parameter
  types, so the uncached routine execution path is the current correctness
  boundary.
- Hidden stored-routine prepared templates MUST isolate statement-scoped table
  tracking such as `StmtCtx.RelatedTableIDs`; otherwise one internal SQL can
  inherit another statement's schema fences and fail after unrelated DDL.
- Hidden stored-routine prepared-template generation can run while the outer
  statement still has active cache state; seed and restore temporary
  `PlanCacheParams` slots during probing so parameter markers do not read the
  outer execution's runtime arguments.
- Hidden stored-routine prepared-template probes MUST isolate warnings; an
  unsupported probe should fall back without changing `SHOW WARNINGS` output or
  stored-routine warning-handler behavior. Hidden `EXECUTE` paths may replace
  `SessionVars.StmtCtx`, so warning filtering must be applied to the final
  active statement context, not only to the pre-execution pointer.
- Non-prepared cacheability rules MUST remain conservative; widen only with
  dedicated semantic coverage.
- The per-session `PlanCacheStmt` for non-prepared cache is keyed by the
  parameterized SQL; it is a different object from the cached plan values.

Why this exists:
- Mutating the original AST leaks state across retries/other code paths and
  breaks plan digest / hint matching / later planning phases.

### 4.6 PointGet Fast Paths: Executor Reuse Has Special Privilege Semantics

Prepared statements can reuse a cached PointGet executor for an autocommit
point-get fast path.

Sharp edge:
- In the PointGet-executor reuse path, `GetPlanFromPlanCache` can skip the usual
  per-execution privilege check.

Maintainer rule:
- If you touch privilege semantics, point-get executor reuse, or the fencing
  conditions, add/extend a regression test for this specific path.

### 4.7 Instance Plan Cache: Clone Discipline And Memory/Eviction

Instance plan cache is node-level and shared across sessions.

Non-negotiable rules:
- Values stored in the instance cache MUST be clones and treated as read-only.
- On hit, the cached value MUST be cloned again for the current execution.
- Cached values MUST NOT reference session-specific pointers (StmtCtx, session
  vars, allocators, mutable global slices, etc).

Operational notes:
- Instance cache eviction is memory-driven (soft/hard limits) and coordinated by
  a domain-owned background goroutine. Be cautious when adding per-plan memory.
- `admin flush instance plan_cache` uses a domain-level invalidation timestamp;
  sessions observe it lazily during preprocess and clear their local cache.

## 5. Optimizer Engine Invariants (Volcano vs Cascades)

TiDB has two planning engines behind a switch (`EnableCascadesPlanner`):
- Volcano: rule-based logical optimize + cost-based physical selection.
- Cascades: memo-based normalize/explore/implement.

Invariants:
- Both engines MUST preserve SQL semantics. Treat divergences as correctness
  regressions even if they "only" show up under a flag.
- Shared finalization steps (post-optimization fixups) must stay consistent; if a
  new physical operator requires finalization, wire it in the shared path.

Workflow tip:
- When adding a new logical rewrite, decide explicitly whether it belongs in:
  - Volcano logical rule batches
  - Cascades normalization
  - Cascades exploration rules
  and ensure the other engine has an equivalent semantic behavior (or is gated
  with explicit justification + tests).

## 6. Debugging Anchors And Common Failure Modes

Where to look first (planner-specific):
- Only fails on 2nd execution / after "hit": start at `pkg/planner/core/plan_cache.go`
  (fences, param types, rebuild, clone).
- Only fails across DDL: inspect schema fencing + re-preprocess behavior.
- Only fails cross-session (instance cache): suspect missing clone/read-only
  discipline or session-pointer leakage.
- "ignored hint" / binding unexpected: start at `pkg/planner/optimize.go` and
  binding match logic; validate AST restore.
- Volcano vs Cascades divergence: search for engine-specific code paths under
  `pkg/planner/core/optimizer.go`.

Test surfaces that should catch most regressions:
- prepared/non-prepared plan cache: `pkg/planner/core/casetest/plancache/`
- instance plan cache: `pkg/planner/core/casetest/instanceplancache/`
- SQL integration tests for planner behavior: `tests/integrationtest/t/planner/`
- sysvar surface coverage: `tests/integrationtest/t/sessionctx/setvar.test`

## 7. Maintainer Playbooks (Common Change Types)

### 7.1 Changing Cache Key Inputs

Checklist:
- Decide whether the input changes plan meaning; if yes, key it or ban caching.
- Add a test that executes twice and proves "old key would be wrong".
- For schema-related keying, add coverage with "execute -> DDL -> execute".

### 7.2 Changing Cacheability Rules

Checklist:
- Keep the rules conservative; widen only with targeted coverage.
- Ensure both prepared and non-prepared checkers are updated when applicable.
- Add at least one semantic test, not only plan-shape tests.

### 7.3 Changing Hit Adjustment / Range Rebuild

Checklist:
- Verify param typing extraction for both protocols (binary and text-protocol).
- Add a test that hits the cache with different param values and validates
  ranges are rebuilt (or validate via result set difference).

### 7.4 Changing Instance Cache Behavior

Checklist:
- Audit clone paths: on miss (store) and on hit (execute).
- Add cross-session coverage.
- Watch for any newly stored per-plan data that could carry session pointers or
  mutable slices.

### 7.5 Changing Privilege / Table-Lock Semantics Around Cached Plans

Checklist:
- Validate normal hit path and PointGet-executor reuse path explicitly.
- Add a regression test that changes privileges between executes.

## 8. Glossary

- InfoSchema: the per-statement schema snapshot used for name resolution and planning.
- MDL: metadata lock used to fence schema changes vs plan reuse.
- PlanCacheStmt: statement carrier reused across executions (AST/resolve/visit infos/schema fences).
- PlanCacheValue: cached physical plan value stored for a specific key + param types match.
- Prepared plan cache: plan cache for `PREPARE` / `EXECUTE`.
- Non-prepared plan cache: plan cache for parameterized text-protocol statements.
- Instance plan cache: node-level plan cache shared across sessions; requires cloning.
- Volcano: rule-based optimizer pipeline with cost-based physical selection.
- Cascades: memo-based optimizer pipeline (normalize -> explore -> implement).
