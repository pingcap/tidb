# `pkg/util/engine` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The pinned package contains production `engine.go`, source test
`engine_test.go`, and `BUILD.bazel`. It has two top-level source tests and no
`doc.go`, package harness, fixture, generated source, benchmark, fuzz target,
example, platform variant, or build-tagged production variant. The checkout
package is byte-identical to the pin.

Go's package-local test maps to the standalone `engine_source` Cargo target.
The Bazel target's flaky scheduling annotation has no Cargo semantic analogue.

## Rust ownership and integration

`tidb-pd-client::engine` owns the complete package behavior. `is_tiflash`
classifies protobuf `metapb::Store` labels, while
`is_tiflash_http_response` and `is_tiflash_write_http_response` classify the
normalized `PdStore` returned by Rust's PD HTTP/control-plane boundary. Tuple
labels preserve the key/value strings and source order of Go's HTTP
`StoreLabel` representation.

The three functions retain Go's exact, case-sensitive rules: `engine=tiflash`
and `engine=tiflash_compute` are TiFlash, while only `engine=tiflash` is a
write node. No engine-role inference is added. The former Rust test combined
the two Go identities, tested the protobuf function absent from the source
test, and added supplemental case/order inputs. It was replaced with exactly
`TestIsTiFlashHTTPResp` and `TestIsTiFlashWriteHTTPResp`, each containing only
the five source cases.

## Validation

Profile: WIP; this is a complete package checkpoint inside the continuing
package-by-package parity audit, not repository-wide readiness.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/engine` — passed.
- `GOTOOLCHAIN=go1.25.10 go test ./pkg/util/engine -count=1` — passed; two tests.
- `cargo check -p tidb-pd-client` — passed.
- `cargo test -p tidb-pd-client --test engine_source` — passed; two tests.
- `cargo fmt -p tidb-pd-client` — passed.
- `git diff --check` — passed.

No Go source, Go test, Bazel metadata, or Go module file changed, so
`make bazel_prepare` is not required.

## Risk

- Correctness: production classification is unchanged; test scope now follows
  the pinned package exactly.
- Compatibility: label keys and values remain case-sensitive, compute nodes
  remain excluded only from the write classifier, and extra labels do not
  alter classification.
- Performance: unchanged linear scan with early return over store labels.
- Not verified locally: a live PD HTTP endpoint. The normalized `PdStore`
  conversion is owned and tested by `tidb-pd-client`; this package audit tests
  the complete classification boundary with source label matrices.
