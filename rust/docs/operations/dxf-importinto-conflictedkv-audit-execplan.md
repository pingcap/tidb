# `pkg/dxf/importinto/conflictedkv` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every Go-master production, test, package-doc, generated/platform,
fixture, and build artifact in `pkg/dxf/importinto/conflictedkv`; compare its
duplicate-KV collection and conflict-resolution lifecycle with Rust owners; and
avoid fabricating a partial conflict pipeline without global-sort, Lightning,
object-store, tablecodec, TiKV, and ImportInto scheduler closure.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read all 11 tracked artifacts and all 2,667 lines: the
  package doc, BUILD target, five production sources, and five test/support
  sources. Verify no fixture, benchmark, generated input/output,
  platform-specific variant, or owner metadata exists.
- [x] (2026-09-01) Trace all 47 production functions/methods and nine
  top-level tests with their nested cases: duplicate KV collection, checksum
  and row-file limits, data/index re-encoding, keyspace codecs, local/global
  deduplication, snapshot traffic, transactional deletion, retry/backoff,
  failpoint caps, and cleanup/lifecycle errors.
- [x] (2026-09-01) Search Rust DXF, executor, tablecodec, Lightning,
  object-store, and transaction owners. Confirm that only adjacent DXF step
  labels and generic DML conflict logic exist, not this dependency-closed
  ImportInto pipeline.
- [x] (2026-09-01) Run the exact failpoint-enabled Go-master package suite and
  the Ready documentation gates; record the explicit ownership boundary in the
  parity receipt.
- [x] (2026-09-01) Publish one meaningful receipt batch to
  `origin/hparser-integration`, verify the remote SHA, pull the branch's latest
  state, and continue the rolling package audit.

## Scope and decision

This package is one atomic duplicate-KV lifecycle. Global-sort emits encoded
conflicts; handlers decode and re-encode rows through table metadata; collectors
write bounded conflict-row files and subtract checksums; deleters snapshot,
batch, retry, and transactionally remove existing KVs; filters prevent duplicate
rows across index groups; and keyspace-aware codecs preserve classic and
next-generation behavior. These pieces share limits, progress, traffic, and
close/error contracts. A single helper or ignored test cannot preserve the
observable ImportInto behavior, so implementation remains deferred until the
native dependency closure exists.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/importinto/conflictedkv -count=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

No Go/Bazel/module or Rust source changed, so neither `make bazel_prepare` nor
a Rust test target is required for this receipt-only boundary batch.

## Outcome

The complete inventory and native ownership decision are recorded in
`rust/testport/receipts/dxf_importinto_conflictedkv.md`. The rolling audit
continues with the next unrecorded Go package; repository-wide parity is not
claimed.
