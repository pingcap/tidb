# `pkg/ttl/client` parity receipt

Status: Audited; no dependency-closed Rust implementation was added. This
receipt covers the complete Go package inventory and its current boundary; it
is not a repository-wide parity claim.

Comparison source: Go `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
Rust owner: none in the hparser-integration workspace.

## Complete Go inventory

All four tracked artifacts in `pkg/ttl/client` were read in full before
editing: 735 lines total, including production code, tests, and Bazel
metadata. There is no package `doc.go`, fixture or `testdata` directory,
generated source or input, platform/build-tag variant, benchmark, fuzz target,
README, or ownership artifact.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 33 | Go library/test targets |
| `command.go` | 456 | etcd TTL command protocol and in-memory mock |
| `notification.go` | 107 | etcd notification protocol and mock |
| `command_test.go` | 139 | etcd/in-memory command round-trip test |

The Go production source, test, and BUILD metadata are byte-identical to
current Go master. The source test exercises both a live embedded etcd v3
cluster and the in-memory mock client.

## Parity finding and boundary decision

No dependency-closed Rust owner exists for this package. `CommandClient` and
`NotificationClient` depend on etcd v3 leases, prefix watches, event
channels, JSON payloads, and `pkg/ddl/util.PutKVToEtcd`; faithfully porting
them requires a complete Rust etcd client/transport and the shared DDL utility
semantics. The existing Rust workspace has no TTL command or notification
module, so no Rust-only substitute or speculative in-memory protocol was
introduced. The Go implementation and its integration test remain the
authority.

## Validation

Profile: **Ready** for this no-code audit.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/ttl/client -count=1` — passed (1.045s).
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — repository Ready gate.
- `git diff --check` — passed.

No Rust owner or Go source changed, so Rust cargo checks and
`make bazel_prepare` were not applicable to this documentation-only audit.

## Risks and unverified scope

- Correctness risk is unchanged: etcd command/notification behavior remains
  in the Go source and its live/mock test.
- Compatibility risk is limited to the explicit unported etcd/DDL utility
  boundary; no API or wire format changed.
- Performance is unchanged.
- Not verified locally: cross-runtime etcd interoperability, lease expiry and
  watch-reconnect behavior beyond the Go test, non-host platforms, and
  repository-wide integration suites.

The rolling repository audit continues with the remaining package checklist.
