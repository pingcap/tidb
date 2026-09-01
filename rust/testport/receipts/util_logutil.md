# `pkg/util/logutil` — current Go-master parity receipt

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The eight
top-level artifacts are byte-for-byte unchanged from the earlier audit pin
`3606de5c43fcf4fa5206596c41cd0793403b9818`; the nested
`pkg/util/logutil/consistency` package is inventoried separately in
`util_logutil_consistency.md`.

## Complete inventory

Every top-level production, test, harness, and Bazel artifact was read in
full before validating the existing Rust owner:

| Artifact | Lines | Blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 51 | `85c6357068424a400f10a68d7d5adf3f2e80d08c` | `fb948a6b50ce0aefe32c49251100a4a892560939f6cc39e4d0e8bbe07af6ee7f` | public library/test targets and the complete zap, tracing, gRPC, protobuf, and goleak dependency graph |
| `general_logger.go` | 44 | `0e295c9c54d1c7fbcbfb74dbac0503a25844047e` | `1e9454abe7c3bf159d6986321ba675f2da33337d37e74cc2ce1f3f3592246869` | default-level general logger constructor and copied file configuration |
| `hex.go` | 79 | `8777a15991a8ca057a28ee966b27eec6bcc9235e` | `4fe1364e7c05a2b76113a6bcdb5fde85261b28115bab59a96c2a073bab40207b` | proto `Hex` stringer and reflection pretty-printer, including byte-slice hex encoding, nil pointers, and `XXX` field filtering |
| `hex_test.go` | 59 | `b4c82d51340a34f9c02bfc74ef000f74a9a0db11` | `36fbbee52cfca1ea6da238a2d637e50818f46ababe900a7046cd2bdfbb7a3ee8` | `TestHex` region golden and `TestPrettyPrint` byte/key-range goldens |
| `log.go` | 484 | `ffdba802c55f6751eb720c5eb8713f997da75b03` | `6feb3d0d3341d2dd777a72f8695fcf4339e7a8532064f4cf8f474c74c0b71961` | global/configured logger lifecycle, context field helpers, trace hooks, proxy logging, and sampled logger factories |
| `log_test.go` | 388 | `0e6d3a4672981ed4567f309bf0b5a0c58846d9c5` | `4f307646d4647f8675ca6e893cc1e3b61c4880d9eff0a6da9e59bd3a063103bc` | ten logger/config/proxy/sampling tests plus helpers covering fields, cores, levels, dedicated files, rotation/compression, replacement, proxy precedence, and sampling |
| `main_test.go` | 52 | `e84a04de092d780c8e3de42d37a1fa7e579c9329` | `36b3be49dd68cf21bdc25b0802f586e1f0953de63151485545439379f35652f0` | common-test setup and goleak exclusions |
| `slow_query_logger.go` | 103 | `d1cb3ff170ece866176be6580da2198e9e10c36f` | `2d6979d233745b3d5597571c93d2988cde8ef30dc823be21f7e886cf3af573d0` | slow-query encoder, pooled buffer, dedicated config, and no-op zap field methods |

The top-level package has 1,260 Go lines. It has no `doc.go`, fixture or
testdata tree, generated file, platform/build-tag variant, benchmark, fuzz
target, or examples. The source test surface is exactly
`TestHex`, `TestPrettyPrint`, `TestFieldsFromTraceInfo`, `TestZapLoggerWithKeys`,
`TestZapLoggerWithCore`, `TestSetLevel`,
`TestSlowQueryLoggerAndGeneralLoggerCreation`,
`TestSlowQueryLoggerAndGeneralUseSameLogFileName`, `TestCompressedLog`,
`TestGlobalLoggerReplace`, `TestProxyFields`, and `TestSampleLoggerFactory`.

## Rust ownership and behavior

`rust/crates/tidb-util/src/logutil` remains the dependency-closed native
owner. Its complete implementation inventory is:

| Rust artifact | Lines | SHA-256 | Inventory |
| --- | ---: | --- | --- |
| `mod.rs` | 1,454 | `75622c33592ea0ba8bdb1a4157d94c4125bbd795332b0f52670b7f045f4a8579` | logger/config/global lifecycle, contextual field composition, proxy handling, sampling, age-retention regression, and 20 source-derived tests |
| `hex.rs` | 142 | `12914327c80bd7b940ad8635dd86ce1a0cea21090fb4a84fa09129c234ce0c9f` | explicit `PrettyValue` tree and both hex goldens |
| `file_sink.rs` | 233 | `5c62013fbcd22d0a697a8a3ffb640397dd90754b120b08f9bf3522ca4df1d06a` | backward-compatible open API plus directory-safe lumberjack timestamp parsing, retention by count/age, and gzip support |
| `tests/logutil.semantic.toml` | 18 | `165a9efca95e3ce2e9d4b8899c034784dc692b16ccc37a1558e8cd58c139fd1d` | semantic package command manifest |

The prior focused Rust regressions remain in place for slow-log field
composition, RFC3339Nano formatting, shared sink/level identity, replacement
logger construction, uppercase proxy precedence, fixed FNV sampling buckets,
disabled-level admission, and independent sample windows. This batch adds a
regression proving that `FileLogConfig.MaxDays` reaches the rotating sink and
that invalid filename lookalikes are retained. The Rust logger is used by
production consumers including memory alarms, timer workers, trace events,
session configuration, statistics logging, and DDL logging.

The Go package's gRPC logger replacement, OpenTracing `Event`/`Eventf`/`SetTag`
hooks, and runtime/trace `WithTraceLogger` tee are Go-ecosystem integrations;
there is no dependency-closed Rust consumer or equivalent runtime context for
them. They remain explicit integration boundaries rather than invented Rust
APIs. The Rust `Logger` composition API is the existing native representation
of Go context-attached fields, not a second production logger path.

## Validation

Profile: Ready for this package audit and the `MaxDays` fix. Rust source and
the receipt/ExecPlan changed; the Go package remains source-clean.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest,deadlock -count=1 ./pkg/util/logutil/...` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -race -tags=intest,deadlock -count=1 ./pkg/util/logutil/...` — passed (macOS linker warning only).
- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-util max_days_prunes_only_expired_lumberjack_backups --lib -- --test-threads=1` — failed before the fix (expired backup retained), then passed after the fix.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-util` — passed after the fix (510 tests, 2 ignored).
- `cargo +nightly-2026-08-22 clippy --offline --locked -p tidb-util --lib --no-deps -- -D warnings` — attempted; blocked by 13 unrelated existing diagnostics in encryption, allocator, collection, selector, watcher, and generated/client dependencies. No logutil diagnostic remains.
- `cargo +nightly-2026-08-22 fmt --all -- --check` — passed after the fix.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make -o tools/bin/revive lint` — passed.
- `git show 3353b29fb4^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/logutil.semantic.toml` (with the pinned OpenSSL environment) — passed (1 package, 3 commands).

`cargo clippy -p tidb-util --all-targets -- -D warnings` was attempted as a
workspace Ready check but remains blocked by unrelated existing
`tidb-mysql` `map_or_identity` diagnostics and generated-protobuf
`double_must_use` diagnostics; no logutil source is implicated. The focused
logutil test command passed independently. `make bazel_prepare` is not
required because no Go source/import, Bazel file, Go test function, or module
dependency changed.

## Risks and unverified scope

- Correctness: the logger/hex behavior covered by the source-derived tests is
  aligned, including age-based cleanup; omitted Go tracing integrations have
  no Rust runtime owner.
- Compatibility: file rotation and text encoding remain shared contracts with
  `tidb-log`; downstream consumers still determine caller paths and field
  ordering.
- Performance: rotation now scans only timestamp-shaped backups when cleanup
  is configured; sampler state remains bounded to 4,096 FNV buckets per log
  level as required by zap.
- Not verified locally: Windows-specific Go logger behavior, the Go goleak
  harness under every build-tag combination, and downstream users of the
  nested consistency reporter. The latter is separately inventoried and
  explicitly unclaimed.
