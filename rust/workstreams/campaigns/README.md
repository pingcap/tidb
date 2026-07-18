# Checked rewrite campaigns

A campaign is the expensive-gate unit. It groups dependency-ready vertical
slices whose Go source anchors, original test obligations, and declared Rust
write sets are pairwise disjoint. `work-unit-queue.py check` rejects unknown or
overlapping members and planned/active campaigns below nine production files
or fifty original test/support obligations. Those floors are admission rules;
later ownership transfers may shrink the live manifests referenced by a
historical campaign without changing what its gate validated.

Feature agents still claim one slice at a time. Root keeps the campaign record
active across agent rotations and runs one 12-job integration gate only after
all members freeze. The gate receipt hashes campaign membership as well as the
slice contracts, so the batch cannot be silently shrunk after validation.
After the receipt is consumed, `integrated-members.tsv` preserves that exact
membership and the queue rejects any later addition, removal, or substitution.
