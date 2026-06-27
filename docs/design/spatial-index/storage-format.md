# Design Note: Spatial Storage & Encoding Format (pre-GA)

**Decide before GA.** Two encodings are hard to change once a release ships — user
clusters will hold data in whatever we picked, so changing them later needs an
upgrade/migration path: (1) the on-disk encoding of `GEOMETRY` **values**, and (2)
the index **cell-key** curve. The code-level optimizations below are *not* format-
gated and can land anytime; these two formats should be settled now. Companion to
the spatial-index design ([#69473]) and `review-plan.md` (milestone 1, the `types`
PR).

[#69473]: https://github.com/pingcap/tidb/pull/69473

## Constraint: the internal format is free (but must be lossless)

I/O compatibility — `ST_AsBinary`, `mysqldump`/import, the wire/replication
representation — is handled by **conversion at the boundary**, not by making the
stored bytes match MySQL. So the internal format is ours to choose, with two hard
rules:
1. **Lossless** — exact `f64` coordinates and full geometry structure preserved (no
   TWKB/float32-style precision loss).
2. Round-trips every supported type (point/line/polygon/multi*/collection) + SRID.

Today we store EWKB (`<srid u32 LE><OGC WKB>`), which is byte-identical to MySQL's
internal format. Given the constraint above, that compatibility is no longer a
reason to keep it — and it carries redundancy: a per-row SRID, a per-(sub)geometry
byte-order flag, and WKB framing.

## What the format can actually win (measured)

Microbenchmark of the hot paths (in-memory CPU; ns/op, allocs):

| Operation | ns/op | allocs |
| --- | --- | --- |
| decode point | 53 | 2 |
| decode polygon(50) | 310 | 5 |
| **write: point-index gen-cols, current (3 independent decodes)** | **210** | 7 |
| **write: point-index gen-cols, decode-once** | **82** | 3 |
| read: refine `Within(point, poly)` | 2,862 | 77 |
| read: refine `Within(poly50, poly)` | 26,705 | 446 |
| Morton `EncodePoint` | 14 | 1 |

Findings:
- **Write path (spatial-index maintenance) is decode-bound: ~76%** of the 210 ns is
  decode, and **2/3 of that is redundant** — the point index's `tidb_spatial_key`,
  `ST_X`, `ST_Y` each decode the same value. Without an index there is *no* decode on
  write (the bytes are just stored), so this cost is entirely index maintenance.
- **Read path (refine), isolated:** decode *looks* like ~5% of a single `Relate`
  call. But that treats `relate` as a black box — the end-to-end profile below shows
  `relate` **re-parses WKB internally**, so the true parse cost is much higher.

### End-to-end profile (the full SQL pipeline)

The table above is the *isolated* library cost. Profiling the real pipeline
(testkit/unistore CPU profile: a 160k-row indexed bulk load, and 300× a
`ST_Within(p, const)` refine over 100k rows) puts it in context — and **corrects the
read-path reading**:

- **Write (160k INSERT into an indexed table):** geometry decode for index
  maintenance (`decodeEWKB`) is only **~1.8%** of insert CPU; the input WKT parse
  (`ST_GeomFromText` on the literals) ~7%; the rest is SQL parsing, allocation, the
  row codec, and KV writes. End-to-end the stored-format decode is a *minor* write
  cost — decode-once saves ~2/3 of ~1.8% ≈ **~1%**.
- **Read (refine; pushdown active — the predicate runs in the cop, ~58% of query
  CPU):** WKB parsing is **~27% of the query** (≈ half the refine) — far more than the
  isolated bench's "~5%" implied. It splits two ways:
  - **~13% explicit operand decode** (`geomrel.decodeEWKB` → `UnmarshalWKB`) — parses
    our stored EWKB, so a leaner stored format helps here; ~half of it is the query
    **constant re-decoded per row** (a decode-once-per-query *code* fix).
  - **~14% a relate-*internal* WKB re-parse** — simplefeatures' `Relate` (the JTS
    port) serializes to WKB and re-reads it (`jts.Io_WKBReader`, confirmed 100% under
    `jtsRelateNG`) on every call, *independent of our stored format*. Removing it
    means handing `relate` the already-decoded geometry (a code/library fix).
  
  The DE-9IM matrix is ~24%; scan/codec/agg/cop framing ~21%.

So "reads are relate-bound, decode ~5%" was an artifact of treating `relate` as a
black box — ~14% of the query is WKB re-parsing *inside* it. Net: on **writes** the
format/decode is even smaller than the isolated bench (~2% e2e, envelope-dominated);
on **reads** parsing is *larger* (~27%), but the biggest read-parse win is a **code**
fix (decode the constant once + drop the relate-internal WKB round-trip), with the
stored format helping only the residual per-row column decode.

## Ranked levers (by end-to-end impact, so the format isn't over-sold)

1. **Read parse code-fixes** — decode the query constant once per query, and avoid
   simplefeatures' relate-internal WKB round-trip (hand `relate` the decoded
   geometry). Together ~20% of read CPU; **code, not format.**
2. **Cut the DE-9IM cost** — the ~24% matrix + its 77–446 allocations/call; the
   algorithmic core; code/library.
3. **Point covering-index** — read win is the table random-read *I/O* (not visible in
   this CPU profile) + skipping decode; code, not format.
4. **Decode-once on writes** — ~1% end-to-end (the insert is envelope-bound); code.
5. **Leaner stored format** — helps only the residual per-row column decode (~5–6% of
   reads after the constant fix; ~2% of writes). Modest perf, but it is the one
   **pre-GA lock-in**, so decide it now regardless.

## Proposed format direction (for design review)

Two tiers, in increasing intrusiveness:

- **Tier A — strip redundancy (low-risk, do now):**
  - **Add a 1-byte format-version tag** as byte 0. This is the key de-risker: it
    lets a future release read old values and write new ones (or background-rewrite),
    so the format stays *evolvable* and the "hard to change later" worry is bounded.
  - **Drop the per-row SRID** for SRID-restricted columns (store it in column
    metadata; re-add at the I/O boundary). Keep it only for unrestricted `GEOMETRY`
    columns, which may legally hold mixed SRIDs per row.
  - **Drop the byte-order flag(s)** — we always encode canonical little-endian, so
    every per-(sub)geometry flag is a constant.
- **Tier B — parse-cheap layout (bigger, benchmark-gated):**
  - Flat, aligned `f64` coordinate arrays with minimal framing (type byte + counts),
    so decode is closer to a `memcpy` than a parse — most valuable for **points**
    (the common + indexed case).
  - Optionally an inline MBR (4×`f64`) in a header for bbox without a full parse
    (weigh against the fact that the index already carries bbox columns).

Caveat: TiDB (Go `simplefeatures`) and TiKV (Rust `geo`) use different in-memory
structures, so no single format is zero-copy for both — the goal is a *cheap parse*
into each, not a shared mmap.

## Recommendation

- **Lock the format before GA**, and at minimum ship **Tier A** now — a
  **format-version byte** (makes the format evolvable, largely defusing the lock-in
  risk) plus stripping the redundant SRID and byte-order. These are lossless,
  compat-safe (I/O converts), and cheap.
- Treat **Tier B** (flat-coord layout) as the format-version's next revision, gated
  on a benchmark showing point-write throughput matters for the target workload.
- Do the bigger, non-format wins (**decode-once**, **covering-index**,
  **relate-alloc reduction**) as ordinary follow-ups — they are larger than the
  format change but can land any time.

## Index cell-key encoding: Morton vs Hilbert (SRID 0) — also pre-GA

The same lock-in applies to the **index** key encoding (the stored
`tidb_spatial_key` / `tidb_spatial_keys` cell IDs). Once an index exists its keys are
on disk, so changing the curve means rebuilding every spatial index — more tractable
than the value format (an index can be `DROP`/`CREATE`d), but still a migration.

Today: **SRID 0 (planar) uses a Morton / Z-order curve; SRID 4326 uses S2**, which is
Hilbert-based internally. So 4326 already gets Hilbert-quality locality; **SRID 0
does not** — an inconsistency worth resolving before GA.

Trade-off:
- **Morton (Z-order):** cheap to encode (bit interleaving), but worse spatial
  locality — the Z curve has long "jumps," so a query rectangle maps to **more
  disjoint 1D key ranges** → more index ranges to scan and more covering false
  positives.
- **Hilbert:** better locality (2D-adjacent cells stay 1D-adjacent far more often),
  so a query rectangle maps to **fewer, longer ranges** → less scanning / tighter
  covering — at a higher encode-CPU cost.

**Benchmark before GA** (for SRID 0): Hilbert vs Morton on (a) ranges-per-query and
covering false-positive ratio (pruning quality) and (b) encode ns/op (write cost).
`pkg/util/spatial` already has `BenchmarkEncodePoint` / `BenchmarkCoverRect` and a
false-positive-ratio test to extend. If Hilbert's pruning win exceeds its encode
cost for the target workloads, adopt it for SRID 0 too (consistency with the S2
4326 path is a bonus). Gate the curve behind the format-version tag / index metadata
so it stays evolvable.

## Open questions for design review

1. Custom internal value format vs. keep EWKB + only decode-once/covering-index? (The
   format win is modest; the *reason to decide now* is the post-GA lock-in.)
2. Tier A only (version byte + strip SRID/byte-order) for v1, with Tier B deferred?
3. The version-byte scheme: how many bytes, and the read-old/write-new vs.
   background-rewrite migration policy.
4. SRID-0 cell-key curve: keep Morton, or switch to Hilbert (pending the benchmark)?
   Either way, record the curve in the index metadata / a key-version so it can change.
