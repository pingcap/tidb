# Rust parity work and validation

Use [the current audit](rust/docs/parity/current-audit/README.md), [finding register](rust/docs/parity/current-audit/structural-findings.md), and [structural batches](rust/docs/parity/current-audit/remaining-batches.md). They own current status, shared prerequisites and grouped validation. Root AGENTS.md owns required checks.

Follow freshly fetched Go master and its selected external modules. Repair connected production owners and callers before removing duplicate implementations. Preserve meaningful Go results, errors, rollback and lifecycle tests. Source-file length and historical test counts are not correctness gates.

The current instruction is **no push**. Work in the existing Cloud checkout, preserve concurrent changes, and run the actual locked server build through the normal commit hook. Use the [Cloud instructions](rust/docs/parity/current-audit/README.md#cloud-development-and-validation); do not revive old toolchain overrides or create competing continuations.

The superseded seven-item workflow and historical failure descriptions remain available with `git show 880246825c:BLOCKER_RESOLUTION.md`. This replaces stale scheduling and source-size policy; it does not mark those historical SQL/Go/Bazel failures repaired. Each original assertion still needs source-based reconciliation before acceptance.
