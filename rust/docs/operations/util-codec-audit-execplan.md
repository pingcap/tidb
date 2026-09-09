# `pkg/util/codec` parity audit ExecPlan

This living ExecPlan records the complete Go-package audit and the
non-collating value/hash-path restoration. The repository-wide rolling audit
continues after this package.

## Purpose / Big Picture

Keep comparable-key encoding separate from compact value and lossless hash
encoding, matching current Go `master` and the Rust `tidb-codec` owner. A
collation mode may affect comparable string keys only; value and hash bytes
must preserve the original datum bytes.

## Progress

- [x] (2026-09-02) Read and inventoried all 12 Go artifacts at
      `origin/master` `c6054025ed4c32ab3672a2a24ea46892714d21ec`: five
      production files, six test/benchmark/harness files, and `BUILD.bazel`
      (4,542 authoritative lines). No docs, fixtures, generated/platform
      variants, or extra build inputs exist.
- [x] (2026-09-02) Compared every production/test artifact and all in-tree
      encoder call sites. Current Go master makes `Encoder` key-oriented,
      routes value/hash encoding through package-level functions, and removes
      the obsolete method-based hash assertion.
- [x] (2026-09-02) Restored the source-shaped non-collating implementation in
      Go while retaining deprecated method wrappers for existing tablecodec and
      benchmark callers; added a value-path compatibility assertion.
- [x] (2026-09-02) Ran focused and full Go codec suites, repository lint, and
      diff hygiene. Existing Rust owner and consumer validations remain in the
      package receipt.
- [x] (2026-09-02) Push this batch to `origin/hparser-integration`, verify local/remote
      SHAs, and fetch the newest target branch before the next package.

## Surprises & Discoveries

- Current Go master removes the encoder methods, but this hparser checkout has
  in-tree tablecodec and benchmark callers that still use them. Removing the
  methods atomically would require a separate complete `pkg/tablecodec` audit;
  compatibility wrappers preserve source compatibility while delegating to the
  current non-collating behavior.
- The Rust owner already removed its Rust-only encoder methods and uses free
  value/hash functions, so no Rust production change is needed for this Go
  follow-up.

## Decision Log

- Decision: make package-level `EncodeValue` and `HashCode` direct, explicit
  non-collating paths and keep deprecated Go method wrappers temporarily.
  Rationale: this matches current behavior, avoids breaking un-audited
  cross-package callers, and keeps the migration dependency-closed. Date:
  2026-09-02, Codex.

## Validation

Run from the repository root:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/util/codec -run '^TestEncoderNewCollationEnabled$' -count=1
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex go test ./pkg/util/codec -count=1
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
    git diff --check

Expected results are passing focused/full Go codec tests, passing lint, and
clean diff hygiene. No new Go file, import section, Bazel target, or module
dependency changed, so `make bazel_prepare` is not required.

## Risks

- Correctness: comparable keys retain collation-aware ordering; compact values
  and hashes now explicitly retain raw bytes regardless of mode.
- Compatibility: deprecated method wrappers remain for un-audited callers and
  are behaviorally delegated to the package-level functions.
- Performance: the package-level paths avoid constructing a temporary Encoder;
  no wire bytes change.

## Outcomes & Retrospective

Go codec value/hash behavior now has one non-collating implementation matching
current master and the Rust owner. Exact removal of legacy methods is deferred
until the owning tablecodec consumer package is audited atomically.
