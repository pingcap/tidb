# How to Write a Good MAINTAINER_GUIDE.md (TiDB)

This doc is a writing guide for subsystem maintainers. It explains what a good
`MAINTAINER_GUIDE.md` should contain and how to keep it useful over time.

A good maintainer guide should:

- Focus on non-obvious design choices, invariants, and workflows.
- Explain "why" (design intent), not only "what" (observable behavior).
- Stay concise and action-oriented for maintainers and reviewers.

## 1. What A MAINTAINER_GUIDE.md Is (And Is Not)

A good maintainer guide is the "sharp edges" map of a subsystem:

- **For maintainers/reviewers/oncall:** preserve correctness/performance during
  change, debug failures quickly, and avoid regressions.
- **For contributors:** learn the invariants and intended boundaries before
  touching complex code.

It is not:

- A tutorial for end users (put that in user docs).
- A re-statement of code that is obvious from reading it.
- A design proposal (put that in `docs/design/`).
- A general "README" that lists exported APIs without intent/invariants.

Hard rule:

- If you change a design invariant, storage/encoding rule, state machine, or
  recovery behavior, update the maintainer guide in the same patch.

## 2. Scope And Reading Strategy

Write the guide with an explicit scope. The first section should answer:

- What code paths does this guide cover? (e.g. `pkg/ddl/`, `pkg/planner/core/`,
  `pkg/executor/`, `pkg/statistics/`.)
- What does it intentionally skip? (e.g. "SQL syntax and AST basics".)
- What are the top 5 things a maintainer must not break?

Recommended reading order (for most subsystems):

1. System context and responsibility boundaries.
2. Core concepts and invariants.
3. Critical paths (write/read/background) and their sequencing rules.
4. Failure modes + debug aids.
5. Maintainer playbook for common change types.

## 3. Where To Put The Guide

Prefer "code-adjacent" placement so the guide is naturally updated when the
code is touched.

Recommended locations:

- Package/subsystem scoped: `pkg/<subsystem>/MAINTAINER_GUIDE.md`

Avoid:

- A single monolithic repository-wide maintainer guide that tries to cover all
  of TiDB. TiDB is too large; the signal-to-noise ratio will degrade quickly.

If a subsystem already has multiple deep docs, prefer consolidating the key
invariants and maintainer workflows into a single `MAINTAINER_GUIDE.md` (and
link to long-form design docs under `docs/design/` when needed). This keeps
"one source of truth" for reviewers/oncall and avoids drift.

## 4. What To Include (Minimum Useful Sections)

### 4.1 System Context And Responsibility Boundaries

State what the subsystem owns and what it must not assume.

Include:

- Upstream/downstream modules and the contract between them.
- Which state is authoritative (memory vs persistent vs external systems).
- What must be deterministic / consistent across nodes.

Example prompts:

- "Which module owns retries? Which module owns timeouts?"
- "What is allowed to be eventually consistent vs must be linearizable?"

### 4.2 Core Concepts And Invariants

This is the heart of the guide. Write invariants as crisp, testable statements.

Good invariant examples (concrete, falsifiable):

- "Schema version must be monotonically increasing across owner transitions."
- "Redo/ingest checkpoint must be persisted before marking job step as done."
- "Snapshot reads must not observe partially applied schema diffs."

Weak statements to avoid:

- "Be careful with concurrency."
- "This should usually work."

Tips:

- Prefer "MUST/MUST NOT" for invariants.
- If an invariant is conditional, state the condition explicitly.
- Name the consequences of violations (data loss, wrong results, hang, OOM).

### 4.3 Critical Flows (Write/Read/Background)

Document the "golden paths" where subtle sequencing bugs hide:

- Write path: what changes state, what gets persisted, what gets retried.
- Read path: what is cached, what snapshot/version it uses, what can be stale.
- Background loops: ownership, scheduling, cancellation, checkpoints, fencing.

For each flow, include:

- A short step list (or a small sequence diagram if it helps).
- The key ordering constraints ("A must happen before B").
- The boundaries where errors can be returned vs retried internally.

### 4.4 Encoding, Persistence, And Compatibility

If the subsystem touches on-disk format, persistent metadata, or wire protocol,
record:

- Where the format lives (struct / table / key layout).
- Backward/forward compatibility rules for rolling upgrades.
- Versioning / feature flags (who reads old/new formats, for how long).

For TiDB specifically, consider:

- System tables and their compatibility constraints.
- Metadata state machines (DDL job, schema states) and resume semantics.
- Cross-version behavior during upgrade/downgrade windows.

### 4.5 Concurrency And Ordering

Call out:

- Which locks/transactions protect which invariants.
- What can run concurrently (and what must be serialized).
- Idempotency rules ("safe to retry") and deduplication keys.
- Owner/leader/fencing semantics if applicable.

### 4.6 Observability And Debug Aids

Make oncall life easier by listing:

- Metrics and what a "bad" value looks like.
- Log keywords, structured fields, and common error messages.
- Relevant failpoints (if any) and how to use them to make tests deterministic.
- Debug SQL / system table queries (when applicable).

Keep it practical: link the metric name/log tag to the code that emits it.

### 4.7 Common Failure Modes And Pitfalls

List common failures as symptoms -> likely causes -> where to look first.

Example shape:

- "DDL job stuck in `running`" -> "owner lease not progressing / reorg worker
  blocked" -> "check owner election logs + reorg checkpoint table"

Make sure these are specific to the subsystem. Generic advice belongs in
`AGENTS.md` or broader docs.

### 4.8 Maintainer Playbook For Changes

Add small checklists for change types that frequently introduce regressions.

Examples:

- Adding a new persistent field:
  - compatibility (old readers)
  - defaulting behavior
  - tests for mixed-version upgrade window
- Changing a state machine:
  - transition diagram
  - rollback/cancel semantics
  - persistence point and resumability
- Touching a hot path:
  - benchmark or regression test
  - metrics to watch
  - expected CPU/memory impact

### 4.9 Glossary

Define subsystem-specific terms you use throughout the doc. Keep it short and
precise.

## 5. Style And Maintenance Rules

### 5.1 Be Concise And "Why"-First

- Prefer bullets and short sections.
- Put the "why this matters" right next to the rule.
- Link to code instead of repeating it verbatim.

### 5.2 Treat The Code As Ground Truth

Docs can drift. In TiDB, behavior is often subtle and evolves.

- If the doc says X but code/tests do Y, treat the doc as outdated and update
  it, or explicitly call out a known drift.
- Avoid speculative claims. If you are not sure, state it as a hypothesis and
  point to how to validate.

### 5.3 Use Stable Anchors

When referencing code:

- Prefer type/function/file names over exact line numbers.
- Prefer links to a small set of entrypoints (the "where to start reading"
  list).

### 5.4 Keep It Reviewable

Maintainer guides are part of the code review surface:

- Make updates in the same PR as the behavior change.
- Keep diffs small and focused; avoid rewriting the entire doc unless needed.

## 6. Copy/Paste Template

Use this as a starting point for a subsystem guide.

```markdown
# <Subsystem> Maintainer Guide

This doc records non-obvious design choices, invariants, and workflows.

It intentionally:
- Prioritize relatively complex logic, and explain why it is designed this way.
- Skip content that can be easily found in the code.
- Keep concise, explicit, while readable.

Status: describes current behavior as implemented in `<path>`.
If you change a design invariant, update this doc in the same patch.

## Contents
1. Scope And Reading Strategy
2. System Context And Responsibility Boundaries
3. Core Concepts And Invariants
4. Critical Flows (Write/Read/Background)
5. Persistence, Encoding, And Compatibility
6. Concurrency, Ordering, And Idempotency
7. Observability And Debug Aids
8. Common Failure Modes And Pitfalls
9. Maintainer Playbook For Changes
10. Glossary

## 1. Scope And Reading Strategy
...

## 2. System Context And Responsibility Boundaries
...

## 3. Core Concepts And Invariants
...

## 4. Critical Flows (Write/Read/Background)
...

## 5. Persistence, Encoding, And Compatibility
...

## 6. Concurrency, Ordering, And Idempotency
...

## 7. Observability And Debug Aids
...

## 8. Common Failure Modes And Pitfalls
...

## 9. Maintainer Playbook For Changes
...

## 10. Glossary
...
```

## 7. Author Checklist (Before Sending A PR)

- Does this change modify an invariant? If yes, update `MAINTAINER_GUIDE.md`.
- Does this change touch persistence/encoding/system tables? If yes, add a
  compatibility note and a regression test.
- Does this change alter a state machine? If yes, update transitions and
  rollback/cancel semantics.
- Does this change affect a hot path? If yes, identify a benchmark or a focused
  regression test and name the metrics to watch.
