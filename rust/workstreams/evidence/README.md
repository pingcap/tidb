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

A completed campaign root does not wait for the whole campaign before its
exact evidence prerequisites become usable. Preflight and then atomically
promote that member while retaining its active claim:

```sh
scripts/campaign_close.py --campaign <campaign> --promote-member <slice>
scripts/campaign_close.py --campaign <campaign> --promote-member <slice> --apply
```

The member transaction requires exact PARTIAL-or-better evidence for every
frozen obligation, applies only checked ownership transfers for that member or
members promoted earlier, regenerates the ledgers under the claim lock, and
leaves campaign close and claim release untouched. The campaign still ends
with one `campaign_close.py --campaign <campaign> --gate` shared gate and its
exact-membership receipt.

Every upstream obligation remains generated in
`difftests/corpus/coverage/go_test_inventory.tsv`. Dispatch exact,
non-overlapping source units with:

```sh
cargo run --locked -j 12 -p difftest --bin go_test_ledger -- --queue <ring>
```

The normal fast path reads those checked inventories without invoking Cargo
and presents paired source/test candidates before an agent creates an atomic
local claim:

```sh
scripts/work-unit-queue.py queue --target <crate> --ring <ring> --limit 3
scripts/work-unit-queue.py claim --owner <owner> --source <go-path> \
  --test <go-test-path>:<line>:<name>
scripts/work-unit-queue.py check
```

The queue's same-directory/stem relationship is candidate discovery only.
Evidence and coverage still require direct inspection of the authoritative Go
source, test declarations, hooks, generated subtests, fixtures, and support
artifacts. Add a directly owned source or test discovered outside the original
candidate to the active transaction with `work-unit-queue.py amend --owner
<owner> --source <go-path> --test <path>:<line>:<name>` (using either addition
independently when appropriate) before editing its Rust evidence.

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
working concurrently. The fast checker reads it to reject live overlap and
stale anchors, while the generated ledgers deliberately do not consume it: a
local lease can coordinate work but cannot hide or discharge an obligation.

The source ledger's `unassigned` and `eliminated-go-runtime` routes are visible
architecture decisions, not coverage statuses. Move a row only after reading
the owning package/source and its consumers; never map mixed `pkg/util` paths
to a generic crate merely to reduce the count.

The default unit is one Go test file with all of its tests, lifecycle hooks,
static `t.Run` children, dynamic `t.Run` generator obligations, and exact
checked-in fixture references; one concrete Bazel or Make target
identified as `<definition file>#<target>`; one shell test program; one SQL
input with all of its result variants; or one exact `testdata`/test-suite
support artifact. Extensionless and Python suite runners remain visible as
suite artifacts rather than being inferred by filename. Use
the optional `package` view only for a coarse backlog summary; packages such
as `pkg/expression` and `pkg/util` are not valid single-agent ownership units.

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
