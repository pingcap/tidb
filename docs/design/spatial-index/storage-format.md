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
- **Read path (refine) is relate-bound, not decode-bound:** decode is ~5% of the
  point/poly refine and ~1.5% of poly/poly; the DE-9IM `relate` (and its 77–446
  allocations) dominates. A faster decode format does ~nothing here.

So a leaner format is a **modest, write-skewed, point-favoring** win: after
decode-once, the remaining single point decode (53 ns) could drop to ~15 ns with a
flat no-framing layout, taking a point-heavy bulk load's per-row geometry CPU from
~210 → ~50 ns (~4×) — but **decode-once does most of that**, and reads barely move.

## Ranked levers (so the format isn't over-sold)

1. **Decode-once on writes** — code, not format; ~2.5× on index maintenance; free.
2. **Point covering-index** — read win is the table random-read *I/O* + skipping
   decode; code, not format.
3. **Cut `relate` allocations** (77–446/call) — the real read-CPU lever; code.
4. **Leaner stored format** — the only *format* item, and the only one hard to
   change post-GA. Modest perf, but decide now.

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
