# `pkg/testkit` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for `pkg/testkit` and record the
dependency-closed Rust ownership boundary. Read every Go production, test,
support, build-tag, nested Bazel, and build artifact before editing; preserve
the package-atomic rule without inventing a second Rust test harness.

## Completed this batch

1. Inventoried all 38 tracked Go artifacts (4,202 lines), including the root
   TestKit/mock-store/session helpers, `database/sql` adapter, result and
   stepped runners, testdata/failpoint/environment utilities, nested helpers,
   all source tests, and every Bazel target. No package doc, fixture directory,
   generated output, benchmark, fuzz target, or platform variant exists; all
   `!codes` source variants were included.
2. Compared the complete support surface with the Rust workspace. Rust has
   crate-local test fixtures and captured SQL tests but no dependency-closed
   owner for Go's mock-store/domain lifecycle, SQL driver, TestKit API, result
   assertions, async/stepped execution, recording, or logging helpers.
3. Found no Rust-only behavior to remove and no missing Go production behavior
   that can be implemented safely without first establishing that owner.
4. Recorded the complete inventory, hashes, validation evidence, and explicit
   boundary in `rust/testport/receipts/testkit.md`.

## Validation gate

- [x] Complete Go source/support/Bazel inventory and owner comparison.
- [x] `go test -tags=intest ./pkg/testkit/... -count=1` (all packages pass).
- [x] Root package compile-only check without test execution.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
      `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

The Go package remains a test infrastructure boundary rather than a Rust
production package. Session/bootstrap lifecycle, SQL-driver integration,
testdata recording, and the broad integration-test ecosystem remain explicit
gaps. The repository package loop continues after this receipt; this plan does
not claim whole-repository completion.
