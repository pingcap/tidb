# `pkg/structure` — Go-master parity receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete Go inventory

The package contains exactly eight tracked artifacts and 1,423 lines. Every
production file, test, harness, and Bazel target was read in full before the
Rust owner was assessed. There is no package `doc.go`, benchmark, fixture,
generated Go source, or platform-specific Go variant.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 42 | `f2cec5bca484240f53132971387a2480f18e9f7a` | `467e707957b0b1b817e001e364b5a0dad5ef75257a5351ad2f49b750a91a7740` | public library and five-shard test targets |
| `hash.go` | 414 | `5e9b1c12df6b73b87f9af2ce623ad5419ba85ea5` | `c38ecf871a643d6485b2ba77879b733169a402bfef75fe7673469c71df76e11d` | hash CRUD, numeric updates, bounded and reverse iteration |
| `list.go` | 247 | `f83dcf1a52febd06321a5eb6b94c9a907c317cc5` | `2c27b78d768a68fcff50e094bd6652cdc85e2416c5d68d897de3c68e2653b289` | list metadata, push/pop, indexing, mutation, and clear |
| `main_test.go` | 36 | `ac8e6daff992ac9a6e590a2e866e7beec38868da` | `dea18ecb43f851859aca4b442c1159c4270dbc934a1150cdabdedc23a0db4043` | common setup and goleak test harness |
| `string.go` | 109 | `2d01bfe730a7f2cfd1f4fca6714c1b675a9bed0b` | `3645f5a70368b164c4a61a05e3dd8f1c9d44f2e671ec48496e5fb950588ec416` | string set/get/int64/iterate/clear operations |
| `structure.go` | 49 | `7d916a90720c19df2c84256f25ee28dc82541a24` | `5f9eb38194e56c17d0cdaf0d615a4574cb1cae6c110fde2dd5fb0e79c23414f2` | constructor, transaction handles, and error declarations |
| `structure_test.go` | 379 | `c5b25daede59abb65c98560d34d78d599679570d` | `c5f065853a1972b511b101373d481b42acadd8af3869e34b43eeb24d9b3c5387` | string/list/hash/error/bounded-iteration behavior tests |
| `type.go` | 147 | `bc92eabf0485eb9b3ab86f5ad611724d1d9752e5` | `9088530df5317b87e3a03a428d6c755fd4c48a99ec6bcc7f7031034062d4472c` | type flags and key encoders/decoders |

The production surface includes `NewStructure`, string operations (`Set`,
`Get`, `GetInt64`, `Inc`, `Iterate`, `Clear`), hash operations (`HSet`,
`HGet`, `EncodeHashAutoIDKeyValue`, `HInc`, `HGetInt64`, `HDel`, `HKeys`,
`HGetAll`, `HGetIter`, `HGetLen`, `HGetLastN`, `HClear`, `IterateHash`,
`IterateHashWithBoundedKey`), reverse iterator construction and methods, list
operations (`LPush`, `RPush`, `LPop`, `RPop`, `LLen`, `LGetAll`, `LIndex`,
`LSet`, `LClear`), and all key/meta helpers. The Go tests exercise the normal
and snapshot mutation boundaries, integer parsing, ordering, nil-value
handling, error codes, and malformed-key skipping.

## Rust ownership and decision

The dependency-closed Rust owner is `rust/crates/tidb-meta/src/structure.rs`
(784 lines), with raw transaction/iterator support in
`rust/crates/tidb-meta/src/transaction.rs` and error/value codecs in
`error.rs` and `value.rs`. The source-derived owner tests are aggregated by
`rust/crates/tidb-meta/tests/structure_source.rs` (282 lines) and cover all
five Go behavior tests. The crate's `Cargo.toml`, `src/lib.rs`, and generated
`tests/all.rs` harness were also inspected for module/build wiring.

The Rust owner already matches the Go key layout, byte-order metadata,
missing-key results, decimal int64 encoding, hash/list ordering, reverse
iterator start-field semantics, bounded-key malformed-entry skipping, and
snapshot write refusal. No Rust-only production path or ignored test was
found. The Rust API intentionally uses `Option`/`Result` and a
`read_only` constructor because Rust cannot alias two mutable transaction
handles; this is the documented equivalent of Go's nil `readWriter`.

No source change is justified for this package, so no regression test was
added in this batch. The existing source-derived tests remain the focused
regression surface for the complete owner.

## Validation

Profile: **Ready** for this package audit; the repository-wide loop remains
in progress.

```text
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-meta --test all structure_ --offline --locked
# 5 passed, 0 failed

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
  -p tidb-util -p tidb-meta -p tidb-executor -p tidb-session --offline --locked
# passed; existing warnings only
```

`cargo fmt --all -- --check` reports one pre-existing formatting difference in
`tidb-planner/src/ranger/go_cases.rs`; no structure file is implicated.
The workspace-wide check reaches an unavailable system OpenSSL when server
dependencies are included. No Go or Bazel file changed, so
`make bazel_prepare` is not required for this docs-only batch.

## Risk and remaining boundaries

- Correctness risk is low: this batch changes no executable code and records
  the already-tested owner boundary.
- Compatibility risk is limited to callers that require Go's concrete
  `kv.Transaction`/panic behavior; those are represented by the Rust raw
  transaction trait and explicit error result rather than a second API.
- Performance is unchanged; reverse and bounded scans retain their existing
  transaction adapter contracts.
- Not verified locally: Bazel analysis and the full Go `pkg/structure` test
  target (the Rust source suite is the deterministic package gate).
