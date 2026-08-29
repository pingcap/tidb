# Complete `pkg/util/sem/v2` and `pkg/util/sem/compat`

This ExecPlan is a living document maintained under `PLANS.md`.

## Purpose

Transcreate the complete pinned Go packages `pkg/util/sem/v2` and
`pkg/util/sem/compat` at `e2788410d8d696605e8cb002585877a063ccc909`.
The result must select SEM v2 from `[security] sem-config`, expose v2 policy
through every compatibility predicate, reject configured SQL and restricted
privilege grants at Go's privilege boundary, enforce read-only variables, and
strip configured optimizer hints with a warning.

## Progress

- [x] Read every production, test, test-support, and Bazel artifact in both Go packages.
- [x] Read the pinned external `coreos/go-semver` implementation and the `net/url` / `objstore.IsLocal` behavior the package composes.
- [x] Remove Rust-only exported internals and supplementary tests.
- [x] Wire startup selection, sysvar defaults, compatibility predicates, restricted SQL, GRANT/REVOKE, and restricted hints.
- [x] Restore the five direct compatibility tests and the restricted-SQL integration behavior.
- [x] Complete WIP validation and package receipts.
- [x] Complete self-review.
- [x] Commit and push.

## Decisions and discoveries

- The AST-to-SEM dependency points downward in Go but cannot do so in Rust;
  `StmtView` is the narrow boundary built by `tidb-session` immediately before
  the common execution/planning funnel.
- Go's `url.Parse` normalizes scheme spelling, accepts percent signs in raw
  queries, and rejects malformed paths, fragments, authorities, and first
  relative path segments containing a colon. The local-file rule preserves
  those observable decisions rather than using a WHATWG URL parser.
- A plan binding injects hints after the direct statement is parsed, so the
  SEM hint filter runs both at the ordinary AST boundary and immediately after
  binding injection.
- The pinned Go tests cannot compile in this checkout because of the existing
  missing `hack.checkMapABI` selection and gRPC `http2.TrailerPrefix` mismatch.
- The session integration test runs its global SEM policy body in a child test
  process, because the Rust unit-test harness otherwise exposes the enabled
  process policy to unrelated concurrently running session tests.

## Validation

Use the WIP profile because this is one package unit in the continuing
repository-wide parity audit. Run the six v2 tests, five compat tests, the
session integration regression, the server startup/config test, full owning
crate tests, formatting, targeted clippy, and checks for every changed crate.
No Go or Bazel source changes are present, so `make bazel_prepare` is not
required.
