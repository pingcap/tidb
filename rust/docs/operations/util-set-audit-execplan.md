# `pkg/util/set` parity audit ExecPlan

## Objective

Keep the complete Go-master set package aligned with its native Rust owner,
including concrete memory accounting, map iteration, and keyed-set contracts.

## Completed

- Read all 12 Go-master artifacts in full: one Bazel target, five production
  files, five test/benchmark files, and the test harness (1,001 lines, 60
  declarations, seven unit tests, and three benchmarks). No fixtures,
  generated/platform variants, examples, or ownership files exist.
- Confirmed the package is source-identical at Go master
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- Preserved the dependency-closed `tidb-util` owner and prior focused fixes:
  concrete memory-aware types and tracker rules, hash-map iteration, free
  keyed-set operations, current-key clone/order behavior, and HashAgg use of
  `StringSetWithMemoryUsage`.
- No new source change is justified in this refresh; the Go test probe remains
  blocked by the workspace grpc dependency mismatch recorded in the receipt.

## Validation gate

- [x] Complete 12-artifact inventory and current-authority receipt recorded in
      `rust/testport/receipts/util_set.md`.
- [x] Existing Rust owner, benchmark, and HashAgg regressions pass per receipt.
- [x] Ready formatting, clean-tree repository lint, and diff checks pass for
      the audit batches.
- [ ] Resolve the external Go dependency mismatch and rerun the complete Go
      package suite.
- [ ] Push this receipt/ExecPlan batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next boundary

Any future set change must retain all seven Go test identities, three benchmark
families, concrete type layout/accounting, unspecified map iteration, and
current-key clone behavior. Do not restore generic or sorted Rust-only APIs.
