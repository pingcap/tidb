# TiDB Architecture Index for Agents

This file is a navigation index for quick subsystem discovery.
Hard requirements remain in the repository root `AGENTS.md`.

## How to Use This Index
1. Map the task to one primary subsystem.
2. Find existing tests covering the same behavior.
3. Reuse existing fixtures/testdata before creating new shapes.
4. Expand to adjacent subsystems only when cross-module effects are clear.

## Core Subsystems

### Planner and optimization
- `pkg/planner/`
- `pkg/planner/core/base/`
- `pkg/planner/core/operator/logicalop/`
- `pkg/planner/core/operator/physicalop/`
- Typical changes: rule matching, plan shape, cost-based choices.
- First tests to inspect:
  - `pkg/planner/core/casetest/`
  - `pkg/planner/core/casetest/rule/testdata/`

### Execution and expressions
- `pkg/executor/`
- `pkg/expression/`
- Typical changes: runtime operator semantics, builtin behavior, evaluation edge cases.
- First tests to inspect:
  - package unit tests under the same path
  - SQL integration tests for user-visible behavior

### Session, variables, protocol
- `pkg/session/`
- `pkg/sessionctx/`
- `pkg/sessionctx/variable/`
- `pkg/server/`
- Typical changes: session lifecycle, statement context behavior, protocol-level behavior.

### DDL and metadata
- `pkg/ddl/`
- `pkg/infoschema/`
- `pkg/meta/`
- `pkg/meta/autoid/`
- Typical changes: schema evolution, metadata persistence, ID generation.

### Storage and distributed execution
- `pkg/kv/`
- `pkg/store/`
- `pkg/distsql/`
- `pkg/tablecodec/`
- Typical changes: KV semantics, storage integration, distributed query paths.

### Domain and statistics
- `pkg/domain/`
- `pkg/statistics/`
- Typical changes: schema/statistics lifecycle, cardinality/estimation behavior.

### Parser and AST
- `pkg/parser/`
- Typical changes: SQL grammar, AST nodes, parser behavior.

## Test Surfaces
- Unit tests: package-local tests under `pkg/**`.
- Integration tests:
  - inputs: `tests/integrationtest/t/`
  - expected outputs: `tests/integrationtest/r/`
- RealTiKV tests:
  - `tests/realtikvtest/`
  - use when behavior depends on real TiKV/PD interaction.

## Practical Search Workflow
1. Start from symptom:
   - SQL keyword
   - error message
   - variable name
2. Find implementation entrypoint:
   - planner/executor/expression/session/ddl/store
3. Find existing tests around the same behavior.
4. Confirm neighboring modules only if call chain crosses boundaries.

## Common Cross-Module Paths
- Planner -> Executor -> Expression for query semantics.
- Session/Variables -> Executor for user-visible runtime behavior.
- DDL -> Infoschema/Meta -> Domain for schema lifecycle.
- Store/KV/DistSQL -> Executor for distributed execution behavior.

## Notes and Runbooks
- Planner notes: `docs/note/planner/rule/rule_ai_notes.md`
- Notes guide: `docs/agents/notes-guide.md`
- Testing runbook: `docs/agents/testing-flow.md`
- AGENTS review guide: `docs/agents/agents-review-guide.md`
- Root execution contract: `AGENTS.md`
