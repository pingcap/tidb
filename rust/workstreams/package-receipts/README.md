# Package completion receipts

Successful schema-2 campaign close writes one immutable `<owner>.json` receipt
here. It records the owner and campaign, content-addressed source/test/support
inventory, reviewed support dispositions, Rust targets and paths, and the exact
shared-gate claim/workspace result.

Covered packages and downstream dependencies fail validation when their receipt
is missing or stale. Campaign close never overwrites an existing owner receipt.
