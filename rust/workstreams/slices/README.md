# Dependency-ready vertical slices

This directory is the checked dispatch queue for consumer-complete rewrite
work. Raw source-family candidates still come from `work-unit-queue.py queue`,
but an agent should not receive one until a steward has converted it into a
vertical slice here.

For sustained campaign work, the steward keeps at least six disjoint slices
ready **while the current six-slice campaign is active**. The steady-state
pipeline is therefore twelve visible slices: six claimed/frozen in the current
campaign and six dependency-ready in the next campaign. Scope the successor
before dispatching the last current batch. Empty `ready` output is a
backlog-preparation failure; do not make feature agents wait while root audits
or invents the next slice.

A slice may own several Go source files atomically. It must name an immediate
consumer, the exact original test obligations, a focused Rust test target, and
every prerequisite. A prerequisite can be another slice or an exact checked
ledger row. `work-unit-queue.py ready` emits only records whose slice
dependencies are `covered`, whose evidence prerequisites meet their minimum
status, and whose sources are not actively claimed.

```toml
schema = "1"
slice = "planner-example-consumer"
status = "ready" # ready | active | partial | covered | blocked
target = "tidb-planner"
ring = "plan"
consumer = "planner output consumed by executor result path"
test_target = "planner_example_source"
go_sources = ["pkg/planner/example.go", "pkg/executor/example.go"]
go_tests = ["pkg/planner/example_test.go:42:TestExample"]
depends_on = ["shared-datum-authority"]
evidence_prerequisites = [
  { capability = "field-type-runtime", evidence_owner = "datatype-value-context-and-format", kind = "source", anchor = "pkg/types/field_type.go", minimum_status = "PARTIAL" },
  { capability = "field-type-contract", evidence_owner = "datatype-value-context-and-format", kind = "test", anchor = "pkg/types/field_type_test.go:25:TestFieldType", minimum_status = "COVERED" },
]
rust_paths = [
  "rust/crates/tidb-planner/src/example.rs",
  "rust/crates/tidb-exec/src/example.rs",
]
```

Dispatch is one atomic operation:

```sh
scripts/work-unit-queue.py ready --target tidb-planner --ring plan
scripts/work-unit-queue.py claim-slice \
  --owner planner-example-consumer --slice planner-example-consumer
```

When a schema-2 claim owner is also a checked slice name, its source and test
sets must exactly match that slice even after the slice becomes `partial` or
otherwise freezes. After the shared integration gate, use `release --owner
<slice> --integrated`; it refuses to release a slice still marked `ready` or
`active`, requires the gate receipt, and rejects implementation/test edits made
after or during that gate, including edits omitted from a slice's `rust_paths`.
This prevents accepted or unvalidated work from reappearing in
the queue. Only the root steward runs the batched gate; leaf agents leave claims
active. Plain
release is rejected; `release --owner <slice> --abandon` is the explicit
recovery/abandonment path for a stale
lease and does not assert integration. Claims whose owner is not a slice name
remain available for direct evidence repair, but must not be used for feature
integration: they have no checked `rust_paths` write set and therefore can hide
real cross-agent file collisions.

Do not mark a prerequisite `covered` merely because it compiles. Covered means
its claimed Go behavior and original tests are translated and its stated
consumer works through the focused test target. `PARTIAL` source evidence can
remain valuable, but it is not a dependency-completion signal.

Use `depends_on` only when the complete checked slice is genuinely required.
Use `evidence_prerequisites` when the consumer needs one already-proven
capability from a larger or still-partial slice. Each entry names exactly one
source or test ledger anchor, the expected evidence owner, and a `PARTIAL` or
`COVERED` minimum. Requirements are evaluated independently; an owner's status
is never inferred from its other rows. An ownership transfer must update the
requirement explicitly. Sharing an artifact, source directory, or slice with a
covered row never makes another anchor covered. `UNTRIAGED` and `BLOCKED` never
satisfy either minimum.

Source-only consolidation uses `-` in all three transfer test columns
(`test_path`, `test_line`, and `test_name`). Partial omission is invalid, and a
new test must not be represented as though an earlier owner transferred it.
Test-only consolidation uses `-` for `source_path`; a transfer with neither a
source nor a test anchor is invalid.

`rust_paths` is an enforced write set, not descriptive metadata. Two active
schema-2 claims cannot share a listed Rust path, and the ready queue suppresses
such work until the current owner releases it. List every file the slice may
edit so parallel agents cannot collide through a shared crate root or consumer.

## Dispatch preflight

Before a campaign becomes `planned`, root performs this mechanical preflight
once for all of its slices. Do not rediscover these facts after agents start:

- Freeze every cross-slice public type/function signature in the ExecPlan.
- Search every existing caller and negative regression for the API or behavior
  being widened; include all stale tests in an owning write set.
- Inspect each target crate's `autotests` setting. If it is `false`, either
  assign its `Cargo.toml` registration to the slice or place the test in an
  already-registered shard.
- If a dependency edge changes, assign `Cargo.toml` and `rust/Cargo.lock` to
  exactly one foundation slice before claiming it.
- Trace acceptance evidence to its last observable consumer. If a live script
  needs a server event, assign the server emission file and freeze exact event
  fields before implementation.
- Run `work-unit-queue.py check`, then claim and dispatch the whole independent
  frontier in one batch. Agents implement immediately; source auditing is a
  bounded root preflight, not an agent phase.

Feature agents run focused tests only. Root batches cross-crate compilation,
the live proof, and the one expensive integration gate after the full vertical
path freezes.
