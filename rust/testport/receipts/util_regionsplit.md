# `pkg/util/regionsplit` — Go-master package boundary receipt

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The package is
byte-for-byte unchanged from the earlier extraction pin.

## Complete inventory

All two Go-master artifacts were read in full before deciding ownership:

| Artifact | Lines | Blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 20 | `a73134067169d09b25b466355fedbf239cd103e7` | `e74fd23d07ec52c718150265f64647058100aeefe1cb88c08fd8f3b68954c1ee` | public library target and the complete table/codec/parser/session/table dependency set |
| `split_handle.go` | 236 | `d206453b4654dfd11a754106e63302e0572a7f5e` | `030b9ec3f891cc4854230ae22f60b6b6d421863d8e73c73f72e5155b2a710b6b` | integer/common-handle selection, unsigned/signed bound validation, table and index split-key generation, datum formatting, and handle-column adapters |

There is no `doc.go`, source test, fixture/testdata tree, benchmark/fuzz
target, generated output, platform-specific variant, or nested package. The
package has 256 Go lines (20 build + 236 source), ten production functions or
methods, and no Go test functions. `split_handle.go` is consumed by
`pkg/ddl/split_region.go` and `pkg/executor/split.go`; those consumers are
outside this package inventory and remain part of the package-atomic boundary
for any future port.

## Go behavior and consumers

`GetSplitTableKeys` emits the record-prefix boundary when indexes coexist,
then either arithmetic integer-handle keys (including unsigned PK handling and
the 1000-step minimum) or codec-encoded common-handle interpolation. It
rejects non-increasing bounds through the caller-provided typed error.
`GetSplitIndexKeys` adds the non-first-index start and successor-index end
boundaries, builds index keys using `math.MinInt64` as the synthetic handle,
rejects reversed bounds, and interpolates `num` split points. The adapters
preserve the int/common-handle distinction and the exact datum-to-string error
text used by DDL/executor callers.

## Rust ownership and decision

Rust has the lower-level table-key encoders in `tidb-codec`, a
`RegionSplit` storage extension in `tidb-txnkv`, and region-cache/request
fragmentation in `tidb-distsql`. It also models split-policy metadata and
parses `SPLIT` syntax. None is a dependency-closed owner of this package's
high-level arithmetic, common-handle/index key generation, boundary insertion,
typed-error contract, and DDL/executor call paths. The existing Rust region
transport APIs intentionally accept already-generated keys; using them as a
replacement would silently omit key derivation and alter SQL-visible split
behavior.

No Rust-only behavior was found and no safe missing Go behavior could be
implemented without first porting the dependent DDL/executor/table metadata
stack. This complete package is therefore explicitly unclaimed; no
production Rust change or focused regression test was added in this boundary
batch.

## Validation

Profile: Ready for this documentation-only boundary refresh; no source or
build artifact changed.

- `git diff --exit-code c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/regionsplit` — passed; source matches the exact latest Go-master authority.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/regionsplit -count=1` — PASS (`[no test files]`) in the current and exact detached Go-master worktrees.
- Rust search across codec, DDL/executor, distsql, and txnkv crates — found only lower-level encoders/transport and metadata, not a complete owner; no Rust source or test was added.
- Rust search across codec, DDL/executor, distsql, and txnkv crates — found only lower-level encoders/transport and metadata, not a complete owner.

No Go or Bazel file changed, so `make bazel_prepare` is not required. This is a
Ready documentation-only refresh; full
DDL split-region integration, PD/TiKV region scheduling, and end-to-end
unsigned/common-handle/index split scenarios were not run for this explicitly
unclaimed boundary.

## Risks and unverified scope

- Correctness: arithmetic overflow/underflow and codec interpolation depend on
  the source `types.Datum`, `tablecodec`, and `util.GetValuesList` contracts;
  no independent Rust implementation was introduced.
- Compatibility: preserve the synthetic `math.MinInt64` index handle, minimum
  step, prefix-boundary ordering, and typed error messages when this boundary
  is eventually ported.
- Performance: no key-generation or region-splitting runtime path changed.
- Not verified locally: DDL/executor integration, PD split RPC behavior, and
  all integer/common-handle/index combinations beyond the existing upstream
  callers.
