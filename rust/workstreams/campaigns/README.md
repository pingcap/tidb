# Checked multi-package campaigns

An ordinary whole package does not need a campaign. Claim it, implement it, and
close it directly:

```sh
python3 scripts/campaign_close.py --package <owner>
python3 scripts/campaign_close.py --package <owner> --gate
```

Create a tracked campaign only when two or more packages are genuinely
dependency-inseparable and must share one acceptance decision. Its source,
test/support, and Rust write sets must be pairwise disjoint, and its exact
members must equal the active schema-2 claim set.

```sh
python3 scripts/campaign_close.py --campaign <campaign>
python3 scripts/campaign_close.py --campaign <campaign> --gate
```

Preflight is read-only. The gated close snapshots every mutable surface and
restores it if preparation, validation, receipt creation, or release fails. A
successful campaign close marks every member covered together and appends the
exact membership to `integrated-members.tsv`. Historical campaign records stay
immutable; they are audit evidence, not a queue that ordinary package work must
maintain.
