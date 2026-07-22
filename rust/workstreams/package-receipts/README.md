# Package completion receipts

Successful schema-2 package close writes one immutable `<owner>.json` receipt
here. Receipt schema 2 records whether the close was derived directly from one
package claim or from an inseparable multi-package campaign, plus the
content-addressed source/test/support inventory, reviewed support dispositions,
Rust targets and paths, and exact gate claim/workspace result. Legacy schema-1
campaign receipts remain valid.

Covered packages and downstream dependencies fail validation when their receipt
is missing or stale. Package close never overwrites an existing owner receipt.

When a covered package needs repair, do not edit or delete its receipt by hand.
Run `python3 scripts/work-unit-queue.py reopen-package --owner <package-slice>`
with no active claims or integration gate. The checked transaction validates
the exact current receipt, refuses covered downstream dependents, removes the
receipt, and returns the complete package manifest to `ready`. It deliberately
preserves prior campaign manifests and integrated membership as historical
evidence. An ordinary repair uses a new direct package close; a campaign is
needed only when the repair is inseparable from other packages.
