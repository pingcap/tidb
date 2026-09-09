# `pkg/util/disjointset` parity audit ExecPlan

This ExecPlan is a living document. Keep `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while the
rolling Go-to-Rust audit continues.

Reference: repository `PLANS.md` and root `AGENTS.md`.

## Purpose / Big Picture

Audit the complete dense and sparse disjoint-set package, including both Go
source tests, the common test harness, and Bazel target, then certify its Rust
owner and chunk consumer at current Go master.

## Progress

- [x] (2026-09-02) Read all six Go-master artifacts in full (302 lines): two
      production files, two source tests, `main_test.go`, and `BUILD.bazel`.
      Confirmed no fixture, generated/platform variant, nested package, or
      other build input.
- [x] (2026-09-02) Confirmed all six files are byte-identical to Go-master
      authority `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- [x] (2026-09-02) Read the Rust `tidb-util::disjointset` owner and its
      `tidb-chunk` consumer; no Rust-only behavior or missing Go behavior
      remained.
- [x] (2026-09-02) Ran current and detached Go suites and all three Rust owner
      tests, including the signed-boundary regression; all passed.
- [x] (2026-09-02) Ran the Ready formatting, pinned lint, and diff-hygiene
      gates. No production edit or new regression was warranted.
- [ ] Push this receipt/plan refresh to `origin/hparser-integration`, verify
      local/remote SHAs, and fetch the newest target branch before the next
      package boundary.

## Surprises & Discoveries

- Go's sparse implementation keeps signed `int` parent/index values and
  inserts missing values during lookup; Rust needed the same signed panic
  boundary, which is already guarded by a focused regression.
- `main_test.go` only supplies common setup/leak checks; it contributes no
  disjoint-set semantics.

## Decision Log

- Decision: make this a receipt/plan refresh only. Rationale: source and Rust
  owner are unchanged at current Go master and the existing signed-boundary
  fix already covers the only cross-language risk. Date/Author: 2026-09-02,
  Codex.
- Decision: retain the two source tests and signed-boundary regression rather
  than add duplicate vectors. Rationale: this preserves Go's test identity
  while keeping the prior ABI-width guarantee explicit. Date/Author:
  2026-09-02, Codex.

## Validation

Exact commands and outcomes are recorded in
`rust/testport/receipts/util_disjointset.md`. This batch changes only Markdown,
so `make bazel_prepare` is not required.

## Outcomes & Retrospective

The disjoint-set package inventory is current at Go master, with all source,
test, harness, and build artifacts accounted for. Rust remains aligned on
dense/sparse union semantics and signed indices; the rolling audit continues.
