# `pkg/metaservice` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory every meta-service production, test, fixture, and build artifact;
restore Go-master keyspace-aware endpoint behavior; add focused regressions;
and publish one verified batch.

## Progress

- [x] (2026-09-02) Pulled the latest branch tip and read all 6 package artifacts
  and 708 lines before editing; no fixture or generated/platform variant was
  present.
- [x] (2026-09-02) Restored keyspace meta-service group resolution, namespaced
  etcd dialing, PD URL parsing, and the `GetPDServiceURLs` interface.
- [x] (2026-09-02) Added and ran focused endpoint, namespace, and missing-meta
  regressions plus compile probes.
- [x] (2026-09-02) Ran the remaining Ready gates, staged only this package and
  its receipt, committed, pushed to `origin/hparser-integration`, verified the
  remote SHA, and fast-forward pulled.
- [ ] Continue the rolling audit with the next unrecorded Go package.

## Constraints

The PD/etcd endpoint and keyspace namespace contract is compatibility-sensitive
and must stay aligned with the versioned client APIs. Bazel metadata generation
is required by repository policy but cannot run without a local Bazel binary.

## Outcome

Evidence is recorded in `rust/testport/receipts/metaservice.md`; no Rust
package-completion claim is made.
