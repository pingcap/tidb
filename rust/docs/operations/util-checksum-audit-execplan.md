# `pkg/util/checksum` parity audit ExecPlan

## Objective

Keep the complete Go-master CRC framing package aligned with the Rust checksum
owner, encryption/spill consumers, and Go's ignored-return API behavior.

## Progress

- [x] Read all four Go-master artifacts at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec`: `BUILD.bazel`, `checksum.go`,
  `checksum_test.go`, and `main_test.go` (786 lines; writer/reader APIs, ten
  source tests, helpers, and goleak harness).
- [x] Confirm there are no package docs, fixtures/testdata, generated or
  platform Go variants, benchmarks, fuzz targets, examples, or nested
  packages.
- [x] Compare the Rust checksum/layered-I/O owner, encryption and chunk spill
  consumers, and benchmark target: block geometry, CRC framing, sticky and
  short-write errors, positional reads, cache overlays, pooled buffers, and
  close cascade match Go. Removed the Rust-only `underlying` accessor and its
  consumer reach-through.
- [x] Remove six Rust-only `#[must_use]` diagnostics from constructors and
  cache accessors; `TestReturnValuesMayBeIgnoredLikeGo` failed before the fix
  with six lint errors and passes afterward. The ten source test identities
  remain intact.
- [x] Revalidate current and exact detached Go tests, all eleven Rust owner
  tests, spill consumer tests/checks, formatting, diff quality, and the pinned
  detached `make lint` Ready gate.
- [x] Commit, push, pull, and verify `origin/hparser-integration`.

## Validation gate

This is a focused Ready parity fix. No Go or Bazel file changed, so
`make bazel_prepare` is not required. Exact commands, pre-fix failure, and
consumer boundaries are recorded in `rust/testport/receipts/util_checksum.md`.

## Next boundary

Any future checksum change must preserve 1,024-byte framing, 1,020-byte
payloads, CRC-32/IEEE little-endian fields, sticky errors, positional counts,
encrypted cache overlays, pooled buffers, explicit close ordering, and Go's
ability to ignore non-error return values.
