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

## PoC-specific notes (verify before publishing)

- The roadmap-#2 axis fix unified the **S2 covering, `ST_Distance_Sphere`, and the
  cap/rect cover** onto MySQL's lat-first order; the **accessors** (`ST_Latitude` /
  `ST_Longitude`) were already correct.
- The fix did **not** touch GeoJSON I/O. Confirm that the PoC's
  `ST_AsGeoJSON` / `ST_GeomFromGeoJSON` emit/read **lng-first** (matching RFC 7946 and
  PostGIS). If they pass coordinates straight through, they would currently be
  lat-first — i.e. **wrong vs both** the GeoJSON spec and PostGIS.
- Migration guidance for the user docs: a `POINT(a b)` literal that worked in PostGIS
  must have its coordinates **swapped** for TiDB at 4326 (and vice-versa), but GeoJSON
  payloads carry over unchanged.
