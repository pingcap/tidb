# `pkg/util/vitess` parity audit ExecPlan

## Objective

Keep the complete Go-master Vitess shard-key hash helper aligned with its Rust
DES owner, source vectors, and production expression consumer.

## Progress

- [x] Read all four Go-master artifacts at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`: `BUILD.bazel`, `main_test.go`,
  `vitess_hash.go`, and `vitess_hash_test.go` (154 lines; one production
  function, five source vectors, TestMain, and one helper).
- [x] Confirm there are no package docs, fixtures, generated/platform files,
  benchmarks, or nested packages.
- [x] Compare Rust `tidb-util::vitess` and the `tidb-expr` consumer: one
  all-zero-key DES block over big-endian bytes, five exact vectors, and
  infallible fixed-width encryption match Go. Rust-only `must_use` policy,
  named null-key API, supplemental vectors, and expanded narrative remain
  removed.
- [x] Revalidate current and exact detached Go-master tests, the focused Rust
  vector test, consumer check, formatting, and diff quality.
- [ ] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This rolling refresh uses the Ready documentation-only profile. No Go or Bazel
file changed, so `make bazel_prepare` is not required. No regression test is
added because this batch changes no behavior; the existing five-vector test
remains the focused source-derived regression. Exact commands and boundaries
are recorded in `rust/testport/receipts/util_vitess.md`.

## Next boundary

Any future hash change must preserve big-endian encoding, the all-zero DES key,
fixed 64-bit block, nil error contract, and all five source vectors. Do not add
alternate keys, hash widths, or Rust-only API diagnostics.
