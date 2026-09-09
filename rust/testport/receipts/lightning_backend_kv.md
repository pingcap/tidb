# `pkg/lightning/backend/kv` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (`origin/master`).

## Complete inventory

The package has exactly 13 tracked artifacts and 3,150 Go lines. Every file
was read in full from the pinned source, including production, test, benchmark,
and build metadata. The current hparser branch predates five small
Go-master changes; the receipt compares the pinned source rather than copying
Go files into the Rust integration branch.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 101 | `51f9f5d3028b2cda4f32ffc87b8af482e10ee54d` | target metadata and 27-shard flaky test definition; no Rust build input |
| `allocator.go` | 81 | `5c97c27045b66e7dfffd207a4cfda9a1eb999f19` | panicking allocator and monotonic rebase helpers; no Rust owner |
| `allocator_test.go` | 35 | `2d28968d54b76bbaa869961f9d170f133d979795` | `TestAllocator` (one source test) |
| `base.go` | 402 | `cb7cc14abc3d46099b96796997e5e339ef9b93be` | row conversion, generated-column evaluation, logging, and allocator bridge; no Rust owner |
| `base_test.go` | 88 | `a7525e099fd511d1f2976ad2ff56285fff91b5db` | `TestLogKVConvertFailed`, `TestDatumToValueStringForCastError` |
| `context.go` | 233 | `3f2eb700c849514aff74411cf76d99514bbdda88` | Lightning expression/table mutation contexts; no Rust owner |
| `context_test.go` | 308 | `6da5fc6f1a5f9325d5bf7b10d06f22aa52efafd5` | `TestLitExprContext`, `TestLitTableMutateContext` |
| `kv2sql.go` | 149 | `c1b78745066d07e7786de1abc0bf96c05b8d5f86` | table KV decoder, generated-column reconstruction, and index-key iteration; no Rust owner |
| `kv2sql_test.go` | 116 | `c0ed03001ef0a876ee53b7d4015f904fe4bd0a19` | `TestIterRawIndexKeysClusteredPK`, `TestIterRawIndexKeysIntPK` |
| `session.go` | 383 | `abf5ea1d0e539f2d453208f28702440d56660940` | Lightning session, transaction shell, pooled byte buffers, and KV capture; no Rust owner |
| `session_internal_test.go` | 175 | `01a286364af051e0e99a71d96585e2c8485f2086` | three memory-buffer/session state tests |
| `sql2kv.go` | 362 | `4493d44bd5189ae4417ec9f1fcb076b5ebc9ff3c` | table encoder, row grouping/classification, generated columns, and auto-ID conversion; no Rust owner |
| `sql2kv_test.go` | 717 | `ff34fbf4ce4030ebdaddecd5afb45e034fe56ddf` | 12 source tests plus `BenchmarkSQL2KV` |

Production inventory is 110 function/method declarations: allocator (5),
base (16), context (20), decoder (7), session (42), and SQL-to-KV (20).
The test surface has 22 `TestXxx` functions and one benchmark. The BUILD file
declares the package library and flaky 27-shard test target. There are no
package docs, fixtures, testdata, generated sources, platform variants,
fuzz corpora, README files, or other build inputs.

## Source behavior and current-master delta

The package implements the concrete Lightning table encoder/decoder boundary:
session SQL-mode and system-variable setup, expression and generated-column
evaluation, automatic row-ID/random-ID/shard-ID conversion, tablecodec row and
index KV encoding/decoding, pooled byte-buffer ownership, KV grouping and
checksums, and diagnostic redaction/truncation. The source tests cover strict
and non-strict casts, row formats 1/2, timestamps, double auto-increment,
missing auto values, expression defaults, auto-random and shard IDs, clustered
and integer primary-key index deletion, generated columns, memory-buffer reuse,
and row classification.

Compared with the hparser branch, Go master adds the table's new-collation flag
to the encoder and decoder expression contexts, adds the corresponding
assertions in `base_test.go`, and routes generated-column expression building
through the context flag rather than a per-call option. Those changes are
recorded as source deltas; they are not silently omitted from the inventory.

## Rust ownership and parity result

No Rust crate contains a dependency-closed Lightning backend KV encoder or
decoder. `tidb-tablecodec` owns generic row-key/value primitives, and
`tidb-txnkv`/`tidb-executor` own ordinary transaction memory-buffer and union
scan behavior, but none implements this package's `EncodingConfig` consumer,
table metadata conversion, generated-column evaluation, `Pairs` carrier,
`TableKVDecoder`, or Lightning session context. The adjacent
`tidb-util::lightning_*` modules are separate package claims and have no call
site for these APIs.

No Rust-only behavior was found to remove. Implementing this package requires
an atomic closure over parser/table metadata, expression evaluation, datum
casting, tablecodec, auto-ID allocators, duplicate detection, checksums, and
Lightning backend writers. A narrowed facade or cache-only implementation
would not preserve Go behavior, so no speculative Rust code or regression
carrier was added.

## Validation

Profile: Ready for this documentation-only boundary update; no Go, Bazel,
module, generated, or Rust source changed.

Passed from the repository root on the current branch:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/lightning/backend/kv -count=1
ok   github.com/pingcap/tidb/pkg/lightning/backend/kv 0.938s
```

Passed from a detached worktree at the exact Go-master pin:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/lightning/backend/kv -count=1
ok   github.com/pingcap/tidb/pkg/lightning/backend/kv 1.076s
```

The package has no failpoint use or failpoint dependency. Its source tests
already provide focused coverage for the behavior in scope; no Rust regression
test is applicable while the dependency-closed owner is absent. Rust formatting,
repository lint, and `git diff --check` are run for the receipt batch. Because
only documentation is changing, `make bazel_prepare` is not required.

## Risk and next boundary

- Correctness: all 13 artifacts, 110 production declarations, 22 tests, and
  the benchmark are mapped; both current-branch and exact-master Go suites
  pass.
- Compatibility: the five Go-master source deltas are explicit. No alternate
  Rust context or KV representation was introduced.
- Performance: no runtime code changed; the source's pooled buffers and
  checksum paths remain an unimplemented Rust boundary.

The next audit should cover `pkg/lightning/backend/tidb` or the parent backend
contract only after the table/datum and writer dependencies can be closed
without duplicating this package's behavior.
