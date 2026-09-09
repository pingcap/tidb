# `pkg/util/schemacmp` — current Go-master package parity receipt

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The package is
byte-for-byte unchanged from extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

All ten Go-master artifacts were read in full before validating the native
owner:

| Artifact | Lines | Blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 48 | `0da6003bbfb96354d4a69cab14392a9e26a7e91d` | `006b02fd57da2c945ad37b96fbf8da1ed3a6e584c76f94ebae797ecdd663ead0` | library/test targets and parser/model/format dependencies |
| `charset_collation.go` | 156 | `0f9f71e8abbb7ce63aa160bb55871c772ede728a` | `cc90b9b5b4a3785af1e3922840c962aa02c68e9ba07e3b229344fcd806cd10c8` | charset/collation lattice normalization, ordering, and joins |
| `charset_collation_test.go` | 116 | `ad7ea6a065be055b1b4a262dd5f649409abe4574` | `8170ce569e322269d7cbc1b3f4a5a95dbc410191e19677904f9c2d696974642e` | four source comparison/join tests |
| `lattice.go` | 814 | `c178b91e3dccdc6e0e02192ed6b79667222c8855` | `ea66736c9292dd40de0b6ade639b2d757939d7a66f0397c7f6a3d85e41e34f33` | error catalog, lattice interface, primitive/tuple/maybe/string/map semilattices |
| `lattice_test.go` | 597 | `1e58cbc961635c1258957bb51d750604dfbe2719` | `6ff793b6248fb5e4bc7b57b527db8c5a3b4a2d762ec588904de62012b7fcc81f` | compatibility matrix and comparison tests |
| `table.go` | 425 | `7b28f81bb2c26a2fcf6cd621b2658486278f22d1` | `94d225e47a022b7ada71296d21a84e67b4ad584199438ec40fa57cd55a928189` | column/index/table metadata encoding, joins, restores, and decode helpers |
| `table_test.go` | 540 | `1ddd662d7121b6ec9cd762006592873df206c008` | `25ecb58333a9b84062ebe050d866259b01659dab3196ef057280fc206e3d6cf2` | schema-join and restored CREATE TABLE tests |
| `type.go` | 221 | `bfaea2030ae98c2b40a97655d9150fed2b546fb0` | `aaff42110f24a64b13d5b1d30aa86cdc6e1260ad5b80d04a1f0dc2ceb48df57c` | field-type tuple encoding, missing-column defaults, key flags, and joins |
| `type_test.go` | 307 | `45325973f020a6102cebb996a5543407bd92b4a1` | `967be50723397a391c36ded7d46bf5c1a281f92c4f17696183abfc575286af61` | type unwrap and compare/join tests |
| `util.go` | 69 | `771a745cc3450fdd3deec250f6343a89f2b5f2ad` | `3efca856279b482cb476cfc410af16c32b61d69b2f36892fc4ed51b6911d22a6` | MySQL integer/blob type ordering helpers |

The package has 3,293 Go lines, nine production/test source files plus one
Bazel target, and no `doc.go`, generated/platform variant, fixture/testdata,
benchmark, fuzz target, or nested package.

## Rust ownership and behavior

`rust/crates/tidb-schemacmp` is the dependency-closed native owner. Its
`charset_collation`, `lattice`, `table`, `typ`, and `util` modules mirror the
Go semilattice and table metadata operations. The single `tests/all.rs`
aggregate now executes thirteen source test functions (the nine original
charset/collation, lattice, table, and type tests plus four return-contract
regressions) in one process, preserving the package's process-global
type/error behavior. The Rust owner had added `#[must_use]` to fourteen
Go-shaped return APIs (`Charset`, `Collation`, `Value` formatting/conversion,
the singleton/field-type/maybe/map helpers, `Type`, `Encode`, and
`DecodeColumnFieldTypes`), making ordinary Go-compatible discard sites fail
under `deny(unused_must_use)`. Those Rust-only annotations are removed; no
semantic production behavior or duplicate owner was found.

## Rust-only return-contract alignment (2026-09-06)

The focused regressions discard all fourteen affected returns under
`#[deny(unused_must_use)]`: charset/collation constructors in
`charset_collation_test.rs`, lattice/value helpers in `lattice_test.rs`, the
table encode/decode helpers in `table_test.rs`, and `Typ::new` in
`type_test.rs`. With the annotations present, the first three probes produced
exactly thirteen diagnostics and the type probe produced one additional
diagnostic (fourteen total). After removing the annotations, all four probes
compile and pass, proving Go's freely-discardable return contract without
weakening unrelated Rust error/result annotations.

## Validation

Profile: **Ready** for this package authority refresh; the repository-wide loop remains in
progress.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/schemacmp -count=1` — passed.
- The same focused Go suite passed in the exact detached Go-master checkout at `/tmp/tidb-go-latest-c605`.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-schemacmp --test all -- --test-threads=1` — passed (9 tests).
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-schemacmp --test all go_ -- --test-threads=1` — passed (3 focused return-contract tests).
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-schemacmp --test all --test-threads=1` — passed (13 tests, including the four new probes).
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler OPENSSL_STATIC=0 cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-schemacmp --all-targets --offline --locked` — passed.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check` — passed during the adjacent Ready validation.
- `git diff --stat c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/schemacmp` — empty; source is unchanged at current Go master.

No Go or Bazel file changed, so `make bazel_prepare` is not required. Full
cross-package DDL/schema-version consumers and non-source Rust workspace
sweeps were not rerun for this unchanged boundary.

## Risks and unverified scope

- Correctness: the nine source tests pass; schema joins still depend on
  parser/model field-type ordering and restore formatting contracts.
- Compatibility: error message text, map missing-column policy, key-flag
  lattice, and generated/default column restoration must remain byte-compatible.
- Performance: map joins and metadata restores retain their existing linear
  scans and deterministic sorting.
- Not verified locally: every downstream DDL/schema-compare call site and
  platform-specific Go test harness behavior beyond the package test run.
