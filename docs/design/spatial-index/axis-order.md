# Spatial axis order (latitude / longitude) — TiDB/MySQL vs PostGIS

> **Status:** design note. Distill into the public user docs when spatial support
> ships — users migrating from PostGIS (or comparing to it) will hit this.

## The convention

SRID 4326 (WGS 84) has an *axis-order ambiguity*, and the two ecosystems resolve it
differently:

- **TiDB / MySQL** follow the **EPSG/SRS-defined axis order**. EPSG:4326 officially
  defines the order as **(latitude, longitude)**, so the **first** WKT/WKB coordinate
  is the **latitude**. `ST_GeomFromText('POINT(30 50)', 4326)` → latitude 30,
  longitude 50.
- **PostGIS** (and the traditional OGC "x, y" tooling) uses **(longitude, latitude)** —
  x = longitude, y = latitude. The same `POINT(30 50)` → longitude 30, latitude 50.

So there is **no single standard** both follow: MySQL honors the authority/EPSG order
(lat-first); PostGIS uses the traditional GIS order (lng-first). They are **opposite**
for the same literal `POINT(...)`.

The divergence is entirely at the **coordinate-order I/O boundary**: any function that
reads or writes a raw coordinate *by position* disagrees; functions that operate on an
already-built geometry, or that name latitude/longitude explicitly, agree.

## Which functions DIFFER (MySQL/TiDB = lat-first, PostGIS = lng-first)

- **WKT text I/O** — `ST_GeomFromText`, the typed `ST_PointFromText` /
  `ST_LineStringFromText` / `ST_PolygonFromText`; `ST_AsText` / `ST_AsWKT`.
- **WKB binary I/O** — `ST_GeomFromWKB`, `ST_AsBinary` / `ST_AsWKB` (WKB is raw
  doubles; the interpretation flips).
- **Positional accessors** — `ST_X` / `ST_Y`. `ST_X` returns the *first* coordinate,
  which is **latitude in MySQL/TiDB** but **longitude in PostGIS** (and vice-versa for
  `ST_Y`).

## Which functions are the SAME (no disagreement)

- **GeoJSON I/O** — `ST_AsGeoJSON` / `ST_GeomFromGeoJSON`. RFC 7946 *mandates*
  `[longitude, latitude]`, and MySQL honors that for GeoJSON even though its WKT is
  lat-first — so both systems use lng-first here. (The one case where MySQL agrees
  with PostGIS *because* it defers to a spec that fixes the order.)
- **Explicit lat/lng functions** — `ST_Latitude` / `ST_Longitude` (MySQL-specific):
  they name the quantity, so there is no order ambiguity. PostGIS exposes the same
  values via `ST_Y` / `ST_X` on a geography (where `ST_X` = longitude).
- **Geometry-operating functions are order-agnostic at the function level** —
  predicates (`ST_Within`, `ST_Contains`, `ST_Intersects`, …), measurements
  (`ST_Distance`, `ST_Distance_Sphere`, `ST_Area`, `ST_Length`), and transforms return
  the *same geographic answer* for the same geographic input. Any observed difference
  comes from how the inputs were *constructed/read* at the boundary above, not from the
  function itself.

## Per-SRID axis order: which SRIDs actually differ (the full picture)

The flip is **not** limited to 4326, and it is **not** a clean "geographic differs,
projected agrees" split. The rule is simply:

- **PostGIS** uses one **fixed** order for *every* SRID — it ignores the SRS's
  authority axis order and always reads/writes WKT/WKB as `(x, y)` =
  `(easting/longitude first, northing/latitude second)`.
- **TiDB/MySQL** honor each SRID's **authority-defined** axis order.

Counting MySQL's catalog (`information_schema.ST_SPATIAL_REFERENCE_SYSTEMS`, **5238**
entries; parsed from each SRS's top-level `AXIS` order and validated against
`ST_Latitude`):

| MySQL's first axis | count | vs PostGIS |
| --- | ---: | --- |
| projected, easting-first | 3464 | **agree** |
| geographic, **lon-first** | 8 | **agree** |
| geographic, lat-first | 537 | **differ** |
| projected, northing-first | 1169 | **differ** |
| projected, westing-first | 30 | **differ** |
| projected, southing-first | 29 | **differ** |
| (no explicit axis) | 1 | — |

So **~1765 of 5238 SRIDs (~34%) disagree with PostGIS**, spanning *both* families:

- **Most geographic SRIDs are lat-first** (differ) — incl. **4326**, 4258 (ETRS89),
  and the bulk of geographic CRSs. The **8 geographic exceptions that agree with
  PostGIS** are lon-first frames: EPSG **7035 RGSPM06, 7037 RGR92, 7039 RGM04,
  7041 RGFG95, 7084 RGF93, 7086 RGAF09, 7133 RGTAAF07, 8902 RGWF96** (French overseas,
  defined "(lon-lat)").
- **Most projected SRIDs are easting-first** (agree), but **~1228 differ**
  (northing/southing/westing-first): Gauss-Krüger / Gauss-Boaga zones, Krovak
  (S-JTSK), Argentina (POSGAR / Campo Inchauspe), Korean belts, and many national
  grids.

Empirical check (MySQL 9.7): `ST_Latitude(POINT(30 50))` returns `30` for 4326 and
4258 (latitude is the **first** coordinate → lat-first) but `50` for 7035 RGSPM06
(latitude is the **second** → lon-first).

### Migration guidance (PostGIS → TiDB) — for the public docs

You **cannot** assume "swap coordinates for geographic, leave projected alone." The
swap is needed **per-SRID**, wherever MySQL's authority order is not easting/lon-first
— ~1/3 of all SRIDs, across *both* geographic and projected. Practically: a WKT/WKB
`POINT(a b)` ingested under one of the ~1765 "differ" SRIDs has its two coordinates in
the opposite order between PostGIS and TiDB. **GeoJSON** (always `[lon, lat]`,
RFC 7946) and the explicit **`ST_Latitude`/`ST_Longitude`** accessors carry over
unchanged. Reproduce the table above with: `SELECT SRS_ID, DEFINITION FROM
information_schema.ST_SPATIAL_REFERENCE_SYSTEMS` and inspect the top-level `AXIS[...]`
order.

## PoC-specific notes (verified)

- The roadmap-#2 axis fix unified the **S2 covering, `ST_Distance_Sphere`, and the
  cap/rect cover** onto MySQL's lat-first order; the **accessors** (`ST_Latitude` /
  `ST_Longitude`, `ST_X` / `ST_Y`) were already correct.
- **GeoJSON I/O is handled**: `ST_AsGeoJSON` / `ST_GeomFromGeoJSON` swap the axis for
  4326 so they emit/read **lng-first** (RFC 7946), matching MySQL — verified
  (`ST_AsGeoJSON(POINT(30 50),4326)` → `[50,30]`, round-trip preserves lat 30 / lng 50).
  SRID 0 is left as-is. (The #2 flip briefly regressed this to lat-first; fixed in
  `dbaa773a02`, `TestPOCSpatial4326GeoJSONAxis`.)
- **WKB I/O matches MySQL**: `ST_AsBinary(POINT(30 50),4326)` is byte-identical to
  MySQL's lat-first WKB (verified hex).
- Migration guidance for the user docs: a `POINT(a b)` literal (WKT) that worked in
  PostGIS must have its coordinates **swapped** for TiDB at 4326 (and vice-versa);
  GeoJSON payloads and WKB carry over per their own fixed conventions.
