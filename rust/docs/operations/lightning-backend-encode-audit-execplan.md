# `pkg/lightning/backend/encode` parity audit ExecPlan

## Objective

Inventory the complete Go-master encoding-backend contract and determine
whether Rust has a dependency-closed owner before implementing any behavior.

## Completed

- Read both Go-master artifacts in full: the 15-line BUILD target and the
  92-line `encode.go` interface/configuration file.
- Confirmed there are no tests, fixtures, generated/platform variants,
  benchmarks, package docs, or extra build inputs.
- Mapped all seven exported contracts and their methods, plus every BUILD
  dependency (`log`, `verification`, `parser/mysql`, `table`, and `types`).
- Searched all Rust crates and found no encoding owner or call site. Adjacent
  Lightning modules are separate package claims, so no Rust-only behavior was
  removed and no speculative facade was introduced.
- The focused Go compile/test command passes with no test files.

## Validation gate

- [x] Complete origin/master inventory and package boundary.
- [x] Focused Go package check passes.
- [x] Rust formatting, repository lint, and diff checks pass for the receipt
      batch.
- [ ] Push the receipt/ExecPlan batch to `origin/hparser-integration`, verify
      equal local/remote/advertised SHAs, and pull the explicit branch ref.

## Remaining boundary

Implementing `EncodingBuilder`, encoded rows, and backend writers requires an
atomic dependency closure across table metadata, datum conversion, tablecodec,
duplicate detection, checksums, and Lightning backend storage. Keep this
package as an explicit contract boundary until that closure is available.
