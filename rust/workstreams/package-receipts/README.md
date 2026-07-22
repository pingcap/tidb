# Package completion receipts

Successful schema-2 campaign close writes one immutable `<owner>.json` receipt
here. It records the owner and campaign, content-addressed source/test/support
inventory, reviewed support dispositions, Rust targets and paths, and the exact
shared-gate claim/workspace result.

Covered packages and downstream dependencies fail validation when their receipt
is missing or stale. Campaign close never overwrites an existing owner receipt.

When a covered package needs repair, do not edit or delete its receipt by hand.
Run `python3 scripts/work-unit-queue.py reopen-package --owner <package-slice>`
with no active claims or integration gate. The checked transaction validates
the exact current receipt, refuses covered downstream dependents, removes the
receipt, and returns the complete package manifest to `ready`. It deliberately
preserves prior campaign manifests and integrated membership as historical
evidence; a new repair campaign must produce the next completion receipt.
