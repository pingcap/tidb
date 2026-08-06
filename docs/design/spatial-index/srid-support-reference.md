# SRID & spatial-function support: MySQL vs PostGIS (empirical reference)

Reference data for the spatial-index work: which SRIDs and `ST_*` functions the
production engines actually *compute* with (not merely store), measured directly
against real servers. Use it to scope PoC compatibility claims and to pick
cross-checkable expected values for tests.

All numbers below were produced by running throwaway Docker containers and
querying them; the method is at the end so anyone can reproduce or refresh this.

Measured 2026-06-26 against:

- MySQL `8.0.46`, `8.4.10`, `9.7.1` (Docker `mysql:8.0.46` / `mysql:8.4` / `mysql:9.7`)
- PostGIS `3.5.2` on PostgreSQL 17, GEOS 3.9.0, PROJ 7.2.1 (`postgis/postgis:latest`)

## TL;DR

- "Supported SRID" is not a list; it is a combination of (a) the SRS catalog and
  (b) what coordinate-system class each function implements.
- **MySQL** decides planar-vs-geodesic automatically from the **SRS class**
  (SRID 0 / projected = Cartesian; geographic like 4326 = geodesic). A handful of
  functions are Cartesian-only and raise an error on geographic input.
- **PostGIS** decides it from the **type**: `geometry` is always planar (any SRID,
  units = coordinate units), `geography` is geodesic (metres). No "not implemented
  for geographic" errors, because `geometry` functions always compute in the plane.
- Across MySQL 8.0.46 -> 8.4.10 -> 9.7.1 the spatial **function-support matrix is
  identical**; only the catalog size grew (and one cosmetic `ST_Buffer` change).

## SRS catalogs

MySQL exposes `INFORMATION_SCHEMA.ST_SPATIAL_REFERENCE_SYSTEMS` (fixed EPSG import
plus SRID 0). PostGIS exposes `spatial_ref_sys` (EPSG, user-extensible, no SRID 0).

| Engine / version | Total SRSs | Geographic (GEOGCS) | Projected (PROJCS) | Geocentric / other | SRID 0 in catalog |
| --- | ---: | ---: | ---: | ---: | :--: |
| MySQL 8.0.46 | 5,152 | 483 | 4,668 | 0 | yes |
| MySQL 8.4.10 | 5,238 | 545 | 4,692 | 0 | yes |
| MySQL 9.7.1 | 5,238 | 545 | 4,692 | 0 | yes |
| PostGIS 3.5.2 | 8,500 | 934 | 6,884 | 682 (not sub-classified) | no (SRID 0 = "undefined") |

- **MySQL ships only 2D geographic + 2D projected** (plus SRID 0): the counts sum
  exactly (`483 + 4,668 + 1 = 5,152`; `545 + 4,692 + 1 = 5,238`), so the geocentric/
  compound/vertical count is a derived-exact **0**.
- **PostGIS's 682 "other"** (`8,500 - 934 - 6,884`) is geocentric (`GEOCCS`) +
  compound (`COMPD_CS`) + a few vertical/local; this was not sub-classified (only
  `GEOGCS`/`PROJCS` were matched). Refresh with `srtext LIKE 'GEOCCS%'` to split it.

MySQL's catalog grew from 8.0 to 8.4 (mostly more geographic SRSs) and is flat
8.4 -> 9.7. PostGIS's is larger and editable; SRID 0 is not a row but is still
usable as a unitless planar geometry.

## Coordinate ranges (measured, MySQL 8.0.46)

| SRS | X / Y range enforced | Out-of-range behavior |
| --- | --- | --- |
| SRID 0 | none -- full finite IEEE-754 double (approx +/-1.7976931348623157e308) | `DBL_MAX` and `-1e308` round-trip exactly; overflow to Inf (`1e400`) -> ERROR 3037 "Invalid GIS data" |
| EPSG:4326 | latitude in [-90, 90], longitude in (-180, 180] | lat 100 -> ERROR 3617; long 200 -> ERROR 3616 |

The 4326 error wording ("Latitude 100.000000 is out of range" for `POINT(100 0)`)
also confirms MySQL's **lat/long axis order** -- the first coordinate is latitude.

SRID 0 is an infinite, unitless Cartesian plane with **no range checking**; a
geographic SRS instead enforces the bounds in its definition. This directly
constrains the PoC coverer (see "Scaling" below): SRID 0 spans the whole double
range, but the planar coverer's default domain is only +/-2.1e9 per axis.

## Function support on a geographic SRS (EPSG:4326)

Identical across MySQL 8.0.46 / 8.4.10 / 9.7.1, so shown once. PostGIS is split by
type because that is what selects planar vs geodesic.

| Function on 4326 | MySQL (8.0-9.7) | PostGIS `geometry` | PostGIS `geography` |
| --- | :--: | :--: | :--: |
| `ST_Distance` | OK (geodesic m) | OK (planar, **degrees**) | OK (geodesic m) |
| `ST_Length` | OK (m) | OK (degrees) | OK (m) |
| `ST_Area` | OK (m^2) | OK (deg^2) | OK (m^2) |
| `ST_Within` / `ST_Contains` / `ST_Intersects` | OK | OK (planar) | OK |
| `ST_IsValid` | OK | OK | OK |
| `ST_Buffer` | OK | OK (planar) | OK |
| `ST_Transform` | OK | OK | n/a |
| `ST_Centroid` | **ERR 3618** (Cartesian-only) | OK (planar) | n/a |
| `ST_ConvexHull` | **ERR 3618** (Cartesian-only) | OK (planar) | n/a |
| `ST_Distance_Sphere` / `ST_DistanceSphere` | OK (sphere m) | OK (sphere m) | n/a |

MySQL error for the Cartesian-only functions:

    ERROR 3618: st_centroid(POLYGON) has not been implemented for geographic
    spatial reference systems.

PostGIS error when forcing `geography` onto a projected SRS:

    ERROR: Only lon/lat coordinate systems are supported in geography.

`geography` is **not** 4326-only: `ST_Distance` on EPSG:4269 (NAD83) geography
also computes (110574.39 m), so any geographic SRS works.

## Measured values (the cross-checkable numbers)

Distance/length/area for a 1-degree step and a unit box. The same query gives
different distances on MySQL vs PostGIS purely because of **axis order** (see
below); both are correct geodesics measuring different arcs.

| Quantity (4326, 1 degree) | MySQL 8.0-9.7 | PostGIS |
| --- | ---: | ---: |
| `ST_Distance` geodesic (ellipsoid) | 111319.49 m (1 deg longitude @ equator) | 110574.39 m (1 deg latitude) |
| `*_Sphere` (great-circle) | 111195.08 m | 111195.08 m |
| `*_Spheroid` (ellipsoid) | (= ST_Distance) | 110574.39 m |
| `ST_Length` geodesic | 111319.49 m | 110574.39 m |
| `ST_Area` of 1deg box geodesic | 12308778368.75 m^2 | 12308778361.47 m^2 |
| `ST_Distance` planar SRID 0 / 3857 | 5 | 5 |
| `ST_Distance` planar `geometry` 4326 | n/a (always geodesic) | 1 (degrees) |

Sphere distance agrees across all engines (on a sphere 1 degree is the same
everywhere) -> a good invariant for testing `ST_Distance_Sphere`.

## Gotchas relevant to the PoC

- **Axis order.** MySQL EPSG:4326 is **lat/long**; PostGIS is **long/lat**. The
  111319 vs 110574 difference above is entirely this. Any "MySQL-compatible 4326"
  claim must use MySQL's lat/long ordering.
- **PostGIS `geometry` + 4326 silently returns degrees.** `ST_Distance` does not
  error; it just measures in the plane. MySQL never does this -- 4326 always means
  geodesic metres.
- **Per-function class checking is MySQL-only.** If the PoC follows MySQL
  semantics, geographic input to a Cartesian-only function should error rather
  than silently compute.
- **Version drift is minimal in MySQL.** Targeting 8.0 vs 8.4 vs 9.x makes no
  difference to the spatial function set; only the SRS catalog and a cosmetic
  `ST_Buffer` first-vertex change (8.0.46 emits `POLYGON((0.0000090...`, 8.4/9.7
  emit `POLYGON((-0.000009...`).

## Scaling the PoC from {0, 4326} to many SRIDs

The catalog has thousands of SRIDs, but they do **not** map to thousands of code
paths. Support is per coordinate-system *class* plus a metadata table, so the work
is bounded and mostly mechanical.

Already SRID-generic (no per-SRID work):

- **Storage & parsing.** EWKB carries the SRID prefix and the parser accepts
  `SRID n`. Any SRID stores and round-trips today.

Moderate, mostly metadata:

- **Projected / Cartesian SRSs (~4,700).** The existing planar quadtree coverer
  works for every one of them as-is -- they are just X/Y metres on a plane. The
  only per-SRS input is the coordinate **domain (bounds)** for the quadtree, which
  EPSG already publishes as each CRS's area-of-use. Add a bounds table and the same
  coverer covers all projected SRIDs. Tighter per-SRS bounds also *improve*
  selectivity versus one universal domain (see next point).
- **Geographic SRSs (~500).** The S2 coverer added for 4326 is effectively
  datum-agnostic for *covering*: WGS84 vs NAD83 differ by sub-metre, negligible
  against cell sizes, and the refine step re-checks with the exact predicate. One
  S2 coverer therefore handles practically all geographic SRIDs.

The selectivity coupling (why per-SRS bounds matter):

- SRID 0 spans the full double range (measured above) but the planar coverer's
  default domain is only `[-(1<<31), (1<<31)-1]` (approx +/-2.1e9). Points outside
  that cannot be covered correctly, yet widening the domain to cover all of SRID 0
  makes default cells too coarse to be selective (the trade-off recorded in
  `OVERNIGHT-PLAN.md`). Per-SRS bounds resolve both: each projected SRS gets a
  tight, correct domain and good cell selectivity at once.

The real cost is the function layer and metadata, not the index:

- **Exact predicates per class** for refine -- geodesic distance on the correct
  ellipsoid for geographic, planar for projected. Already routed through GEOS (+
  S2); extending to more ellipsoids is parameter work.
- **Per-SRS metadata**: axis order (MySQL's lat/long convention), units, valid
  bounds, ellipsoid -- ideally imported from EPSG the way MySQL populates
  `ST_SPATIAL_REFERENCE_SYSTEMS`.
- **Validation parity**: out-of-range coordinate errors and mixed-SRID errors.
- **`ST_Transform`** only if cross-SRID operations are wanted (needs PROJ).
- **Geocentric (3D X/Y/Z)** is not a 2D-index case; leave it out (and MySQL ships
  none anyway).

Bottom line: going from `{0, 4326}` to most of the catalog is **two coverers
(planar + S2) that already exist, plus an EPSG metadata/bounds table and per-class
exact predicates** -- not thousands of special cases. The fiddly parts are axis
order, per-SRS bounds (which also fix selectivity), and matching MySQL's
validation errors.

## Reproduction

Note: this Docker rejects single-character container names; use a >=2-char name.

MySQL (repeat per tag `8.0.46`, `8.4`, `9.7`):

    docker run --name mysqltest -e MYSQL_ROOT_PASSWORD=root -p 13306:3306 -d mysql:8.0.46
    # wait: docker exec mysqltest mysqladmin ping -uroot -proot --silent
    docker exec -e MYSQL_PWD=root mysqltest mysql -uroot -N -e \
      "SELECT COUNT(*) FROM INFORMATION_SCHEMA.ST_SPATIAL_REFERENCE_SYSTEMS;"
    docker exec -e MYSQL_PWD=root mysqltest mysql -uroot -N -e \
      "SELECT ST_Distance(ST_GeomFromText('POINT(0 0)',4326), ST_GeomFromText('POINT(0 1)',4326));"
    # classify catalog: DEFINITION LIKE 'GEOGCS%' (geographic) / 'PROJCS%' (projected)
    # coordinate ranges:
    #   SRID 0 (no bounds): ST_X(ST_GeomFromText('POINT(1.7976931348623157e308 0)',0))  -> round-trips
    #   SRID 0 overflow:    ST_GeomFromText('POINT(1e400 0)',0)                          -> ERROR 3037
    #   4326 lat bound:     ST_GeomFromText('POINT(100 0)',4326)                         -> ERROR 3617 (lat first)
    #   4326 long bound:    ST_GeomFromText('POINT(0 200)',4326)                         -> ERROR 3616

PostGIS:

    docker run --name postgistest -e POSTGRES_PASSWORD=root -p 15432:5432 -d postgis/postgis:latest
    # wait: docker exec postgistest pg_isready -U postgres
    docker exec -e PGPASSWORD=root postgistest psql -U postgres -tAc "CREATE EXTENSION IF NOT EXISTS postgis;"
    docker exec -e PGPASSWORD=root postgistest psql -U postgres -tAc \
      "SELECT ST_Distance(ST_GeomFromText('POINT(0 0)',4326)::geography, ST_GeomFromText('POINT(0 1)',4326)::geography);"
    # classify catalog: srtext LIKE 'GEOGCS%' / 'PROJCS%' / 'GEOCCS%' (geocentric)

Cleanup: `docker rm -f mysqltest postgistest`.
