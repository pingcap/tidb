# `pkg/util/backoff` parity audit ExecPlan

Go `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`
(2026-09-02) is the current authority.

## Objective

Keep the complete Go-master exponential backoff package aligned with its
native Rust owner and source test vector.

## Progress

- [x] Read all three Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: `BUILD.bazel`, `backoff.go`,
  and `backoff_test.go` (113 lines; one interface, one implementation, three
  production methods/declarations, and one source vector test).
- [x] Confirm there are no package docs, fixtures, generated/platform
  variants, benchmarks, fuzz targets, examples, `TestMain`, or nested
  packages.
- [x] Compare Rust `tidb-util::backoff`: signed Go duration domain, target-width
  retry count, reset-on-zero, exponential multiplication, maximum cap, and
  source vector are preserved. Rust-only formatting, const evaluation,
  diagnostics, manifests, and supplemental tests remain removed.
- [x] Revalidate current and exact detached Go-master tests, the source-derived
  Rust test, all-target owner/benchmark check, formatting, and diff quality.
- [x] Commit, push, pull, and verify `origin/hparser-integration` for the
  prior source/receipt batch; this authority refresh is now ready to publish.

## Validation gate

This rolling refresh uses the Ready documentation-only profile. No Go or Bazel
file changed, so `make bazel_prepare` is not required. No regression test is
added because this batch changes no behavior; `TestExponential` remains the
focused source-derived regression. Exact commands and boundaries are recorded
in `rust/testport/receipts/util_backoff.md`.

## Next boundary

Any future change must preserve the signed duration arithmetic, retry-zero
reset, multiplier/cap ordering, and all source vector cases. Do not introduce
jitter, Rust `Duration`, or source-absent diagnostics.
