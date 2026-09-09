# `pkg/util/schemacmp` parity audit ExecPlan

## Objective

Keep the complete schema-comparison package aligned as one dependency-closed
Rust owner, including charset/collation, lattice, table/type metadata, tests,
and Bazel metadata.

## Progress

- Read all ten Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: nine Go source files and
  `BUILD.bazel`, 3,293 lines total. The package has no docs, fixtures,
  generated/platform variants, benchmarks, fuzzers, examples, or nested
  packages.
- Enumerated all production declarations and nine source test identities,
  covering semilattice compatibility/joins, charset and collation ordering,
  schema/table restoration, and field-type behavior.
- Confirmed no Go source delta from the earlier extraction pin.
- Verified `tidb-schemacmp` is the sole dependency-closed Rust owner. Its
  aggregate harness runs all nine source-derived tests and no Rust-only
  duplicate comparator is present.

## Validation

- Active and detached Go-master `go test ./pkg/util/schemacmp -count=1` —
  passed.
- Rust `tidb-schemacmp` aggregate test (`--offline --locked`, nine tests) —
  passed.
- Rust fmt, diff checks, and the pinned detached `make lint` Ready gate —
  passed.
- `git diff --stat c6054025ed4c32ab3672a2a24ea46892714d21ec --
  pkg/util/schemacmp` — empty.

## Completion and risks

This authority refresh changes no production behavior and adds no duplicate
regression carrier; the existing nine source-derived tests are the focused
regression surface. Downstream DDL/schema-version consumers and non-host
platform selections remain outside this leaf validation.
