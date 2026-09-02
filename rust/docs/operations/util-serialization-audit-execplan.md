# `pkg/util/serialization` rolling Go-master audit

This ExecPlan is a living document for the complete utility package. It
includes its production, build, and focused test artifacts; no executor package
is claimed here.

## Progress

- [x] (2026-09-02) Inventoried and read all four Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`; there is no package doc,
  fixture, generated/platform variant, benchmark, fuzz target, or source test.
- [x] (2026-09-02) Compared every artifact with Go master; the production
  `SerializeVectorFloat32` and `DeserializeVectorFloat32` methods already match
  the current authority. Go master removes only the branch-only regression test
  and its Bazel test target.
- [x] (2026-09-02) Kept the branch-only focused empty/non-empty round-trip
  regression and test target as protection for the restored production methods;
  its pre-fix undefined-function failure and post-fix pass are retained as
  historical evidence.
- [x] (2026-09-02) Ran package normal/race tests, the Rust serialization owner
  test, Rust formatting, repository lint, and diff hygiene. Required
  `make bazel_prepare` was attempted and is blocked only by missing Bazel.
- [x] (2026-09-02) Publish this scoped batch to `hparser-integration`, fetch/pull the latest
  tip, and verify local and remote SHAs.

## Decisions and ownership

The Go methods retain the existing native-width length prefix and copy the
payload before invoking `types.ZeroCopyDeserializeVectorFloat32`, exactly as
Go master. The Rust `tidb-util::serialization` owner already implements the
same framing and vector edge cases, so no Rust production change is needed.

The new Go test is intentionally package-local and deterministic. It covers
the zero vector special case, a non-empty vector, value bytes, and complete
`PosAndBuf` consumption without introducing a second serialization format.

## Validation and risks

The Ready profile consists of focused/full/race Go tests, the Rust owner
serialization test, repository lint, Rust formatting, and `git diff --check`.
This receipt refresh changes only documentation; no new Bazel preparation is
required.

Correctness risk is limited to vector spill framing and decoder cursor
advancement. Compatibility and performance are unchanged for all existing
scalar serializers; the two vector functions now match current Go master.
