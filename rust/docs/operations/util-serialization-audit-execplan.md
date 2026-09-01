# `pkg/util/serialization` rolling Go-master audit

This ExecPlan is a living document for the complete utility package. It
includes its production, build, and focused test artifacts; no executor package
is claimed here.

## Progress

- [x] (2026-09-02) Inventoried and read all three pre-change production files
  and `BUILD.bazel`; there is no package doc, fixture, generated/platform
  variant, benchmark, fuzz target, or existing test file.
- [x] (2026-09-02) Compared every artifact with Go master at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`; the checkout omitted exactly
  `SerializeVectorFloat32` and `DeserializeVectorFloat32`.
- [x] (2026-09-02) Added the exact length-prefixed vector encoder/decoder and a
  focused empty/non-empty round-trip regression. The test failed before the
  fix with undefined functions and passes afterward.
- [x] (2026-09-02) Ran package normal/race tests, the Rust serialization owner
  test, Rust formatting, repository lint, and diff hygiene. Required
  `make bazel_prepare` was attempted and is blocked only by missing Bazel.
- [ ] Publish this scoped batch to `hparser-integration`, fetch/pull the latest
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
Because a new Go test target and Bazel metadata were added, `make bazel_prepare`
was run and failed only because the local `bazel` binary is unavailable.

Correctness risk is limited to vector spill framing and decoder cursor
advancement. Compatibility and performance are unchanged for all existing
scalar serializers; the two vector functions now match current Go master.
