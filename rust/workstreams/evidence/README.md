# Evidence and workspace workstream

Owns generated upstream-test accountability, differential-package boundaries,
checked parser/plan snapshots, Cargo workspace membership, and final merged
validation. This is a stewarded integration seam: feature agents consume a
generated source queue and own their leaf corpus or selector plus one stable
owner-named source/test evidence fragment. They do not concurrently edit
generated inventories, snapshots, parser manifests, or Cargo roots.

Campaign throughput is reported as production and original-test obligations
promoted per full gate, not as wave count or Rust LOC. Ordinary batches should
reach nine production files or fifty original obligations unless a named
correctness/dependency boundary requires an earlier gate.

A package is never promoted one member or evidence row at a time. Every
expanded source and original-test row must already be owned by the package at
`COVERED`, with exact checked evidence, before campaign close can start. The
steward closes the complete schema-2 campaign through one command:

```sh
scripts/campaign_close.py --campaign <campaign> --gate
```

The integration gate runs while every campaign package claim remains active.
Only its exact claim/workspace receipt authorizes the locked transaction that
marks all package manifests covered, integrates the campaign, archives its
membership, emits durable package receipts, and releases the claims. Any gate,
receipt, validation, or dashboard failure restores the pre-gate bookkeeping.
Schema-1 campaign history remains readable but cannot be closed again.

Every upstream obligation remains generated in
`difftests/corpus/coverage/go_test_inventory.tsv`. Dispatch exact,
non-overlapping source units with:

```sh
cargo run --locked -j 12 -p difftest --bin go_test_ledger -- --queue <ring>
```

The normal fast path reads the checked source, test, and package-support
inventories without invoking Cargo. It presents complete Go packages; a
steward records the package and its complete Rust crate/write set in a schema-2
slice before claiming it:

```sh
scripts/work-unit-queue.py queue --target <crate> --ring <ring> --limit 3
scripts/work-unit-queue.py packages --target <crate>
scripts/work-unit-queue.py claim-slice --owner <slice> --slice <slice>
scripts/work-unit-queue.py check
```

Expansion includes every direct production source, every original obligation
assigned to the nearest package, and every path-plus-digest row in
`go_package_support_inventory.tsv`. It is non-recursive across real nested Go
packages but recursively owns `testdata`, fixtures, results, build metadata,
embed/generator inputs, and other package support. Claims are exact immutable
snapshots, so new inventory makes an active claim stale. `claim` and `amend`
remain legacy evidence-repair tools and are not feature integration paths.

Every expanded support anchor also has exactly one reviewed disposition in
`workstreams/package-evidence/<owner>-support.tsv`:

```text
support_path<TAB>sha256<TAB>disposition<TAB>evidence_artifact<TAB>note
```

Accepted dispositions are `runtime-transcreated`, `test-transcreated`,
`build-metadata-reviewed`, and `generated-input-reviewed`. Build-only files may
use `build-metadata-reviewed`; they still require their frozen path and digest,
an existing evidence artifact, and a concrete review note. Missing, duplicate,
foreign, or stale dispositions fail package close and make a covered dependency
invalid.

Successful close writes one immutable
`workstreams/package-receipts/<owner>.json`. It content-addresses the package
manifest, every source/test/support evidence artifact, support dispositions,
Rust targets and paths, and the exact gate claim/workspace result. A covered
package and every downstream dependency must validate against this receipt;
campaign close refuses to overwrite an existing receipt.

Production ownership is a separate generated dimension. Select its exact
target-crate queue from `difftests/corpus/coverage/go_source_inventory.tsv`
before pairing it with original tests:

```sh
cargo run --locked -j 12 -p difftest --bin go_source_ledger -- --queue <target-crate>
```

Active cross-layer domains add a checked, sparse coordination layer; they do
not replace either generated ledger. Verify the records before assigning one,
and read the record for its exact source/evidence paths and focused commands:

```sh
cargo run --locked -j 12 -p difftest --bin domain_queue -- --check
cargo run --locked -j 12 -p difftest --bin domain_queue -- --summary
```

`workstreams/claims/<owner>.claim.json` is an ignored local lease for agents
working concurrently. Schema-2 claims freeze the expanded source, test,
support, module, and Rust write-path sets. The fast checker rejects live overlap
and stale anchors, while the generated ledgers deliberately do not consume the
lease: coordination cannot hide or discharge an obligation.

The source ledger's `unassigned` and `eliminated-go-runtime` routes are visible
architecture decisions, not coverage statuses. Move a row only after reading
the owning package/source and its consumers; never map mixed `pkg/util` paths
to a generic crate merely to reduce the count.

The minimum transcreation and feature-claim unit is one complete Go package.
One Go package may map to several Rust crates when that produces a cleaner
Rust-native structure; the package claim and integration receipt remain atomic.
The `packages` view reports package source/line/obligation totals, aggregate
status, complete declared Rust target set, and any active owner. Test-only
packages remain explicit rather than disappearing behind a missing source row.

For a genuinely shared Go test file, add exact top-level test anchors to
`difftests/corpus/coverage/go_test_domain_manifest.tsv`; do not create a
package-wide or range ownership record. The ledger routes each claimed anchor
and its subtest obligations to that test domain, keeps every unclaimed anchor
as an explicit generated `UNTRIAGED` inventory unit, and places file hooks/fixtures in a separate
`#shared-support` queue unit. A coverage evidence row in a split file without
one exact manifest claim is rejected.

An agent that proves a bounded source slice writes the exact source anchor,
status, owner, existing evidence artifact, and conservative reason to only its
`difftests/corpus/coverage/evidence/source/<owner>.tsv` and/or
`evidence/tests/<owner>.tsv` leaf. The evidence steward reviews the row, runs
the merged generators, and owns `go_source_inventory.tsv` and
`go_test_inventory.tsv`; it no longer serializes every feature by copying rows
through a monolithic overlay. `COVERED` still requires every original case and
required differential ring for that anchor to be audited. `PARTIAL` remains
open work. Duplicate anchors across fragments, stale anchors, missing
artifacts, invalid statuses, and non-TSV directory entries fail the ledger.

Parser agents also own exactly one
`difftests/corpus/coverage/evidence/parser/<source-stem>.tsv` leaf per top-level
Go parser source and may run `parser_translation_manifest --check-fragments`
without rewriting shared outputs. The canonical parser translation manifest
and summary, integration parser inventory/golden/queue, plan inventory, and
four differential Cargo package manifests remain stewarded generated seams.
Snapshot counters change only with a reviewed semantic delta; never normalize
a regression into a new baseline.

When a larger source family absorbs an older partial leaf, preserve the
ownership history under `difftests/corpus/coverage/evidence/transfers/`.
`work-unit-queue.py check` rejects a transfer unless the generated ledgers
name the replacement owner, replacement artifacts exist, and retired duplicate
artifacts no longer exist. Do not silently delete or overwrite evidence
fragments during consolidation.
