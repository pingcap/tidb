# `pkg/domain/serverinfo` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every server-info production, test, support, generated, platform,
and build artifact; restore Go-master endpoint-claim and lease lifecycle
behavior; and verify the cross-keyspace consumer boundary.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`; read all five artifacts and
  2,295 lines, including the complete 1,155-line regression suite. No
  fixtures, generated inputs, platform variants, or fuzz targets exist.
- [x] (2026-09-02) Restored normalized status-endpoint claims, conflict-safe
  reattachment, bounded failed-registration cleanup, `RevokeSession`,
  shutdown-before-restart ordering, `ServerInfo.String`, and five-shard Bazel
  metadata byte-for-byte with Go master.
- [x] (2026-09-02) Focused endpoint and claim integration tests plus the full
  failpoint-aware package suite passed; the wrapper disabled failpoints after
  each run.
- [ ] Run the required `make bazel_prepare` gate (blocked locally because
  `bazel` is not installed), publish this package with the related crossks
  consumer, pull the remote tip, and continue the rolling audit.

## Scope and decision

`pkg/domain/serverinfo` owns etcd server-info/topology sessions and advertised
status endpoint duplicate detection. Rust has no dependency-closed TiDB domain
or etcd owner, so this remains Go-native and no Rust facade is introduced.

## Validation gate

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/domain/serverinfo -count=1
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
make bazel_prepare
```

The Bazel gate is mandatory because `status_endpoint_claim.go` is new and the
serverinfo test target/import shape changed; the local executable is absent.

## Outcome

The complete inventory, exact Go-master hashes, and Go-only ownership boundary
are recorded in `rust/testport/receipts/domain_serverinfo.md`. Publication and
remote synchronization remain before the next package audit.
