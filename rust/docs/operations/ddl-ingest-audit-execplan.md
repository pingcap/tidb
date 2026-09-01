# `pkg/ddl/ingest` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every ingest production, test, fixture/build, generated, platform,
and build artifact; remove Rust-only behavior; restore missing Go-master disk
admission behavior; and prove the package with focused and complete tests.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read all 20 artifacts and 4,777
  lines, including the 33-shard Bazel target and all 34 top-level test
  functions. No fixtures, generated inputs, platform variants, or fuzz targets
  exist outside the inventory.
- [x] (2026-09-02) Verified the focused regression failed before the fix due to
  missing `minFreeDiskBytes`, `riskOfDiskFull`, and local-sort admission
  symbols.
- [x] (2026-09-02) Restored Go master's local-sort disk-space admission,
  removed the Rust-only exported disk helper/test, and aligned the Bazel target
  and test dependencies byte-for-byte.
- [x] (2026-09-02) Focused and complete failpoint-aware ingest suites passed;
  failpoints were disabled by the wrapper afterward. Rust formatting, lint,
  and diff hygiene passed.
- [ ] Run the required `make bazel_prepare` gate (blocked locally because
  `bazel` is not installed), publish one batch commit to
  `origin/hparser-integration`, pull it back, and continue the rolling audit.

## Scope and decision

`pkg/ddl/ingest` owns TiDB Lightning local storage, checkpointing, engine
lifecycles, disk/memory admission, and SQL integration. Rust has no
dependency-closed TiDB ingest/storage owner, so this package remains Go-native;
the only Rust-facing action is documenting the boundary and removing the
Rust-only exported helper from the Go package.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/ddl/ingest -count=1
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
make bazel_prepare
```

The Bazel gate is mandatory for this batch because Go test/build metadata and
the import section changed; the command is expected to remain blocked until a
local Bazel executable is available.

## Outcome

The complete inventory and exact hashes are recorded in
`rust/testport/receipts/ddl_ingest.md`. Publication and remote synchronization
remain the final steps for this batch before the next package audit.
