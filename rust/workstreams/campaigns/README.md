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

Close a frozen planned or active campaign through the transactional steward
command once its exact member claims are active:

```sh
# Read-only: validate members, claims, transfer chains, terminal evidence, and
# the exact predecessor-fragment row edits.
python3 scripts/campaign_close.py --campaign <campaign>

# Apply bookkeeping and regenerate source/test inventories plus STATUS.md.
python3 scripts/campaign_close.py --campaign <campaign> --apply

# Apply, run exactly one shared integration gate, receipt-release every exact
# member, and regenerate the final post-release STATUS.md.
python3 scripts/campaign_close.py --campaign <campaign> --gate
```

The command removes only transferred rows from predecessor evidence fragments,
deletes a fragment only when no evidence rows remain, and treats a transfer's
`retired_artifacts = "-"` as an empty set rather than a path. Preflight is
read-only. If unrelated rows keep a predecessor fragment alive, close also
removes that surviving path from the transfer's retired-artifact field; only
artifacts absent after the transaction remain declared retired. Apply snapshots
every file the generators, campaign archive, gate, and releases can mutate and
restores them if any step fails.

Campaigns form a pipeline, not a sequence of planning pauses. Before the
current campaign consumes its final ready batch, root freezes the next
campaign's consumer boundary, public interfaces, original obligations, and
disjoint write sets. At least one complete successor campaign remains `planned`
and ready for dispatch during implementation of the current campaign.
