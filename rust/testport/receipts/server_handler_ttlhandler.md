# `pkg/server/handler/ttlhandler` parity receipt

Status: complete inventory and explicit HTTP-handler boundary; no production
edit was required. This receipt covers the complete Go TTL trigger handler
package and does not claim repository-wide parity.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.

## Complete Go inventory

Before deciding ownership, both tracked artifacts were read in full: 92 total
lines. There is no package `doc.go`, test, fixture, generated source, platform
variant, benchmark, fuzz target, or nested package.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 16 | public HTTP-handler target and dependencies |
| `ttl.go` | 76 | POST-only TTL job trigger endpoint and logging |

The two files are byte-identical to the pinned Go master source.

## Rust ownership and parity decision

The handler resolves a Go `session.Domain`, obtains the TTL command client,
invokes `ttl/client.TriggerNewTTLJob`, writes the HTTP response, and logs the
request. Rust's TTL crate owns table/task/session logic but has no
dependency-closed HTTP router, session-domain registry, or TTL command-client
server adapter corresponding to these Go APIs. A facade would invent endpoint
routing and lifecycle semantics. No Rust-only behavior was found to remove and
no missing Go behavior can be implemented safely outside the server/domain
HTTP migration unit.

## Validation

Profile: **Ready** for this documentation-only ownership boundary.

- `go test ./pkg/server/handler/ttlhandler -count=1` — package compile check
  (no Go test files).
- `cmp` against Go master for `ttl.go` and `BUILD.bazel` — passed.
- `make lint` and `git diff --check` — passed in the surrounding Ready gate.
- No Go or Bazel artifact changed, so `make bazel_prepare` was not required.

## Risks and unverified scope

Live HTTP routing, TTL command execution, domain lookup, logger output,
generated Bazel execution, and non-host platform builds remain outside this
explicit handler boundary.
