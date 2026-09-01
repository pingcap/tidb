# `pkg/util/skip` — Go-master package boundary receipt

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
byte-for-byte unchanged from extraction pin
`e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

| Artifact | Lines | Blob | SHA-256 | Inventory |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 9 | `945e45b1f7c9fd4ad7b92bcda4992dac5fb1d9fc` | `819beca8e6f5807d87815d3bb1c0f8baea06cb3c0f8dcc5aabef12034a7cf298` | public library target and `testkit/testflag` dependency |
| `skip.go` | 37 | `8945c747d8fb6169849a0cc4da4d32b9ad60647b` | `3a44a789fd39a58ec1471f935589977a1087c7c071929651d4dc765cb0b94f7b` | `UnderShort` and `NotUnderLong` testing helpers, including helper marking and variadic skip reasons |

There is no `doc.go`, production runtime variant, source test, fixture,
generated/platform file, benchmark/fuzz target, or nested package. The
package has 46 Go lines and two test-only functions; its only consumer role is
to control Go test selection.

## Rust ownership and decision

Rust has ordinary `#[test]`/`#[ignore]` attributes and an `intest` feature,
but no runtime or build consumer for Go's `testing.Short` and repository
`testflag.Long` helpers. A Rust helper that mirrored these names would be a
second test policy, not SQL behavior, and would not control the Go suites that
currently import this package. No Rust-only behavior was found and no
dependency-closed missing Go behavior can be ported here. This package remains
explicitly unclaimed as Go-only test infrastructure.

## Validation

Profile: WIP for the continuing repository audit; no source or build artifact
changed.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/util/skip -count=1` — passed (`[no test files]`).
- `git diff --stat e2788410d8d696605e8cb002585877a063ccc909..origin/master -- pkg/util/skip` — empty; source is unchanged at Go master.

No Go or Bazel file changed, so `make bazel_prepare` is not required. No
Rust test was added because there is no Rust production owner or behavior to
exercise.

## Risks and unverified scope

- Correctness: future test-framework migration must preserve exact short/long
  skip semantics and variadic reason ordering.
- Compatibility: this package has no runtime SQL effect; changing it only
  changes which Go tests execute.
- Performance: no production path changed.
- Not verified locally: every downstream test's chosen short/long flag matrix
  and CI-specific `testflag` configuration.
