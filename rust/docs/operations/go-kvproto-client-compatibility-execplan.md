# Go kvproto/client compatibility ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every artifact in each touched Go package, restore Go-master behavior
at the kvproto/client-go compatibility boundary, add focused compile and
behavior checks, and publish one bounded receipt without claiming a partial
package transcreation is complete.

## Progress

- [x] (2026-09-02) Pulled the latest `origin/hparser-integration` tip and pinned
  the Go comparison to `origin/master`.
- [x] (2026-09-02) Read the complete package inventories (production, tests,
  fixtures, generated/platform variants, and BUILD inputs) before editing;
  counts and absence decisions are recorded in the receipt.
- [x] (2026-09-02) Restored the current KeyspaceMeta oneof/GetId API across all
  discovered fixtures and production call sites.
- [x] (2026-09-02) Restored Go-master mockstore, unistore, service URL, testkit,
  and client compatibility behavior and synchronized Go module/Bazel metadata.
- [x] (2026-09-02) Ran targeted compile probes and `go mod tidy -diff`.
- [ ] Run the Ready profile (`make lint`, focused behavioral tests, diff check),
  commit this batch, push to `origin/hparser-integration`, verify the remote
  SHA, and fast-forward pull the latest tip.
- [ ] Continue the rolling audit with the next unrecorded Go package.

## Validation and constraints

Go source, tests, BUILD files, and module metadata changed, so `make
bazel_prepare` is mandatory; it is currently blocked because no `bazel`
executable is installed in the local environment. The missing Bazel run is a
known validation gap, not a reason to invent generated metadata.

## Outcome

The compatibility receipt is `rust/testport/receipts/go-kvproto-client-compatibility.md`.
This plan remains open while the rolling package audit continues.
