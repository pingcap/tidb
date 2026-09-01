# `pkg/util/collate` parity audit ExecPlan

## Goal

Keep the Rust collation owner aligned with the complete Go-master
`pkg/util/collate` package. The atomic boundary includes runtime collators,
tests and benchmarks, generated UCA/GBK/GB18030 data, generator inputs and
templates, embedded tables, and Bazel targets. Every source fix must carry a
focused regression test and a Ready validation receipt.

## Source and ownership

- Go authority: `origin/master` at
  `0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).
- Rust owner: `rust/crates/tidb-datatype/src/{charset.rs,collation.rs}` with
  `src/collation_data/`, `scripts/generate_collation_data.py`,
  `src/collation_tests.rs`, and `benches/collate.rs`.
- Existing extraction authority: `e2788410d8d696605e8cb002585877a063ccc909` (the Go package
  has no diff from this pin; the full SHA is recorded in the receipt).

## Progress

- [x] Inventory all 35 Go package files, including the 74,424-record embedded
  GB18030 table, DUCET inputs, generated arrays, retained UCA fixture, test
  and benchmark functions, nested generator packages, and four Bazel files.
- [x] Read and compare every runtime collator family: binary/padded/derived,
  General-CI, UCA 4.0, UCA 9.0, GBK, GB18030, charset helpers, protocol ID
  helpers, wildcard matching, and the reserved pinyin stub.
- [x] Verify the Rust generated images against the Go authorities and inspect
  parser charset generated inputs used by the Rust encoding boundary.
- [x] Record the current package receipt at
  `rust/testport/receipts/util_collate.md`.
- [ ] Continue the repository audit with the next uncovered package; reopen
  this boundary only when Go master changes or a downstream differential test
  identifies a collation mismatch.

## Decisions

1. Keep `tidb-datatype` as the single Rust owner. Adding another collate
   implementation would duplicate registry, encoding, and sort-key behavior.
2. Preserve `utf8mb4_zh_pinyin_tidb_as_cs` as an explicit `implement me`
   panic because that is the current Go source behavior; implementing it in
   Rust would create Rust-only behavior.
3. Treat the Go DUCET and `ucaimpl` generators as source inputs with one Rust
   generation/check gate. Do not hand-edit generated Go arrays or Rust binary
   images.
4. Keep Go-only logging and ecosystem integration details outside this
   collation boundary; no detached Rust API is justified by the package.

## Validation and handoff

The boundary was validated with the Ready profile for an evidence-only batch:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/collate/... -count=1
python3 rust/crates/tidb-datatype/scripts/generate_collation_data.py --check
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-datatype --test all -- --test-threads=1
```

No Go/Bazel/module file changed, so `make bazel_prepare` is not required. No
failpoint lifecycle was required. No package source fix was found in this
batch; any future fix must add a focused test before moving the boundary back
to complete.
