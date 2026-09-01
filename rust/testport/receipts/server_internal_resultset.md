# `pkg/server/internal/resultset` parity receipt

Status: complete inventory and explicit ownership boundary; no production edit
was required. This receipt covers the complete Go result-set adapter package
and does not claim repository-wide parity.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

Before deciding the boundary, all three tracked artifacts were read in full:
506 total lines. There is no package `doc.go`, test, fixture, generated source,
platform variant, benchmark, fuzz target, or nested package.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 22 | package target and server/runtime dependencies |
| `cursor.go` | 193 | cursor wrappers and lazy row iterator |
| `resultset.go` | 291 | record-set lifecycle, metadata conversion, detach, and RUv2 reporting |

The production files are byte-identical to the pinned Go master source.

## Rust ownership and parity decision

This package adapts Go `sqlexec.RecordSet`, chunk allocators, prepared-plan
metadata, session detachment, and server-side cursor fetch reporting. Rust's
`tidb-server` has independent result-set sources and `tidb-protocol` has wire
row streams, but no dependency-closed equivalent of Go's session-bound
`RecordSet`/chunk lifecycle or `TryDetach` contract. Implementing a facade now
would invent a second cursor state machine and resource-group integration.
No Rust-only behavior was found to remove, and no missing Go behavior can be
implemented safely outside the server/session migration unit.

## Validation

Profile: **Ready** for this documentation-only ownership boundary.

- `go test ./pkg/server/internal/resultset -count=1` — package compile check
  (no Go test files).
- `cmp` against Go master for `cursor.go` and `resultset.go` — passed.
- `make lint` and `git diff --check` — passed in the surrounding Ready gate.
- No Go or Bazel artifact changed, so `make bazel_prepare` was not required.

## Risks and unverified scope

Correctness risk is low because this batch changes no runtime code. Live cursor
fetch, record-set detachment, RUv2 reporting, generated Bazel execution, and
non-host platform builds remain outside this explicit Rust ownership boundary.
