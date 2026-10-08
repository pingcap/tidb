# Historical PD transport experiments

The detached transport probes and candidate patches were retired in the PD artifact cleanup.
Their immutable [original bundle](https://github.com/pingcap/tidb/tree/5826ee36e1bc08f66bd20fa9f471256cb8b20f05/rust/docs/parity/current-audit/pd-grpcutil-contract)
retains every script, test, patch, result and design decision. They are not current startup commands.
The runner required native commit `6163ecfc587b248dcbf0e30c1c9d905b4bc5a665`;
its tonic 0.12.3 observations do not validate the current tonic 0.14 client.
The rejected accessor had eight passes and two failures; the separate preface
candidate had fourteen passes. Neither was integrated or accepted as a package.

[inventory.json](inventory.json) preserves the dated source/support/platform inventory.
Original pinned PD `pkg/utils/grpcutil` source and `TestGetCallerID` remain upstream;
all original test, generated/platform/build and shared testutil obligations remain.
Initial dialing/backoff, blocking HTTP/2 readiness, TLS, default-policy Idle/demand
reconnection, cancellation, interceptor ordering and complete package acceptance
remain open where not covered by a later production receipt. Archive removal closes none.

Current implementation and residuals live in [the audit index](../README.md),
[the finding register](../structural-findings.json) and [the batch map](../remaining-batches.md).
Actual client socket, TLS, wire-encoding, cancellation and joined-close tests remain active.
