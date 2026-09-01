# `pkg/lightning/backend/encode` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (`origin/master`).

## Complete inventory

The package has exactly two tracked artifacts and 107 Go lines. Both artifacts
were read in full from the pinned source. The current branch has no source
delta for this path.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 15 | `4085e6a74004561d7af8b4edfde40684bd14e2b2` | Go target metadata; no Rust build input |
| `encode.go` | 92 | `931067181122ed345d634fff37e44ec874560861` | interface and configuration contract; no dependency-closed Rust owner |

There are no package docs, tests, benchmarks, fixtures, testdata, generated
sources, platform variants, README files, or additional build inputs. The
production file declares six exported contracts: `EncodingConfig`,
`EncodingBuilder`, `Encoder`, `SessionOptions`, `Rows`, and `Row`. Their methods are the builder's
`NewEncoder`/`MakeEmptyRows`, encoder `Close`/`Encode`, row collection `Clear`,
row `ClassifyAndAppend`, and row `Size`. No function body or executable branch
exists in this package.

The BUILD target depends on `pkg/lightning/log`,
`pkg/lightning/verification`, `pkg/parser/mysql`, `pkg/table`, and `pkg/types`.
Those dependencies are contracts for a future concrete encoder and are not
replaced with narrowed local shims here.

## Rust ownership and parity result

No Rust crate defines these encoding interfaces or a dependency-closed backend
encoder. The existing `tidb-util` Lightning modules (`lightning_duplicate`,
`lightning_importdef`, `lightning_log`, `lightning_manual`,
`lightning_metric`, `lightning_verification`, and `lightning_worker`) implement
adjacent packages only; none consumes or substitutes this API. Searches of all
Rust sources found no `EncodingConfig`, `EncodingBuilder`, `ClassifyAndAppend`,
or `MakeEmptyRows` owner or call site.

No Rust-only behavior was found to remove, and no speculative encoder or
cache-only facade was added. Implementing this package requires the missing
table, datum, tablecodec, duplicate-detection, checksum, and backend writer
dependency closure; that remains an explicit boundary rather than a partial
package claim.

## Validation

Profile: Ready for this documentation-only boundary update; no Go, Bazel,
module, generated, or Rust source changed.

Passed from the repository root:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/lightning/backend/encode -count=1
?    github.com/pingcap/tidb/pkg/lightning/backend/encode [no test files]
```

The package has no failpoint use or test target, so failpoint enablement and a
regression test are not applicable. No Go/Bazel source changed;
`make bazel_prepare` is therefore not required for this batch. Rust formatting,
repository lint, and `git diff --check` are run for the receipt batch.

## Risk and next boundary

- Correctness: all two artifacts and every declared method are mapped; there is
  no executable implementation to compare yet.
- Compatibility: adding a narrowed Rust interface would hide required Go
  dependencies and break future backend implementations, so the contract is
  intentionally left unimplemented.
- Performance: no runtime code changed.

The next audit should cover a concrete Lightning backend package once its
table/datum and writer dependencies can be inventoried atomically.
