# `pkg/util/naming` parity audit ExecPlan

## Objective

Keep the complete naming validator package aligned with current Go master,
including its source test, Bazel target, and repository ownership metadata.

## Progress

- Read all four artifacts at Go master
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: `naming.go`,
  `naming_test.go`, `BUILD.bazel`, and `OWNERS`. There are no docs, fixtures,
  generated/platform variants, benchmarks, fuzzers, examples, or harnesses.
- Verified production, test, and Bazel bytes match current Go master. The
  active branch retains older `OWNERS` routing; that repository-only metadata
  is preserved and has no Rust runtime representation.
- Verified `tidb-naming` remains the dependency-closed owner. Its source test
  and focused signed-repeat regression preserve Go's accepted names, maximum
  length, negative generated-pattern behavior, and >1000 repeat panic.

## Validation

- Active and detached tagged Go package suites — passed.
- Rust `tidb-naming` owner suite — passed (two tests, including the focused
  fail-before-fix repeat-domain regression).
- Rust fmt, diff checks, and pinned detached `make lint` — passed.

## Completion and risks

No runtime behavior changed in this refresh. The only worktree/Go-master delta
is user-owned `OWNERS` metadata. Direct callers of the signed maximum-length
API and the Go regexp panic boundary remain protected by the focused Rust
regression.
