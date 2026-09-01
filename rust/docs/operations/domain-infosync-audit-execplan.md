# `pkg/domain/infosync` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Purpose / Big Picture

`pkg/domain/infosync` is the Go domain bridge that registers TiDB server
information, coordinates etcd/PD state, and exposes placement, schedule,
resource-manager, and TiFlash operations. After this audit, callers can pass
the completed server-info claim policy through `GlobalInfoSyncerInit`, and the
package's PD resource-manager mock satisfies the current metastore interface.
The focused regression and full failpoint-aware package test make these seams
observable without starting a cluster.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9` and read all twelve package
  artifacts (3,618 Go-master lines), including BUILD metadata and every test;
  confirmed there are no fixtures, generated inputs, platform variants, or
  package-local `doc.go`.
- [x] (2026-09-02) Restored the variadic `serverinfo.SyncerOption` API and
  forwarding, added the focused constructor-option regression, and restored
  mock resource-manager `Get`/`Put` methods.
- [x] (2026-09-02) Captured fail-before compile evidence and passed the focused
  and complete failpoint-aware infosync suites; the wrapper returned failpoints
  to refcount zero.
- [x] (2026-09-02) Ran the Ready gates: `make lint`, Rust formatting, and
  `git diff --check` passed. The mandatory `make bazel_prepare` attempt was
  blocked because the local `bazel` executable is unavailable.
- [ ] Publish one scoped commit, push it to `hparser-integration`, pull the
  remote tip, and continue the rolling package audit.

## Scope and decision

The atomic unit is the complete Go package: all nine production files, both
tests, and BUILD metadata. The package owns Go-native etcd/PD/session/TiFlash
integration. Rust has no dependency-closed replacement, so no partial Rust
port or speculative adapter is appropriate.

## Implementation milestones

1. Inventory: compare every tracked artifact against the pinned Go-master
   worktree, record hashes and line counts, and read each file before edits.
2. API and mock parity: forward `SyncerOption` values and add the two
   metastore methods, preserving the branch's older kvproto test literal.
3. Regression and validation: prove the option call fails before the API fix,
   then run the focused and complete failpoint-aware suites plus Ready gates.
4. Publication: stage only infosync source/test and this package receipt/plan,
   create one meaningful batch commit, push `HEAD:hparser-integration`, fetch,
   and fast-forward pull; verify local and remote SHAs match.

## Validation gate

Run from the repository root with the pinned Go toolchain:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    ./tools/check/failpoint-go-test.sh ./pkg/domain/infosync -run '^TestGlobalInfoSyncerInitServerInfoOptions$' -count=1
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    ./tools/check/failpoint-go-test.sh ./pkg/domain/infosync -count=1
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    make lint
    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    git diff --check
    make bazel_prepare

The first four commands must pass. Bazel preparation remains a mandatory
attempt because the new top-level Go test changes the package test surface;
on this machine it is expected to report that `bazel` is unavailable.

## Surprises & Discoveries

Go master has already introduced `serverinfo.SyncerOption`, but all existing
infosync callers remain valid because the option is variadic. The branch pins
an older kvproto revision, so only a test literal differs from Go master's
oneof syntax; changing module dependencies would expand the package scope
without changing runtime behavior.

## Decision Log

- 2026-09-02: Treat the missing option forwarding and metastore mock methods as
  one infosync package batch because both are direct Go-master surface gaps.
- 2026-09-02: Keep the branch-compatible keyspace protobuf literal rather than
  upgrading kvproto solely to match newer generated-field syntax.

## Outcomes & Retrospective

The production infosync source now matches Go master for the audited diffs, the
new constructor seam has source-derived regression coverage, and the complete
package suite passes with failpoint cleanup. The package remains explicitly
Go-native; no Rust behavior was removed or duplicated. Bazel readiness is the
only locally unverified required gate because the executable is absent.
