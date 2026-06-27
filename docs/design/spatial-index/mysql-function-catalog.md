# MySQL Spatial Function Catalog

Reference for "all the geo functions MySQL has," for scoping the function milestones
in [`review-plan.md`](review-plan.md).

**Provenance:** the public MySQL reference manual, **validated by black-box probing
of running MySQL 9.7.1 and 8.0.46** (each candidate probed: a `1582` param-count
error = present, `1305`/`1046` = absent). Function identity is established from the
public manual and observable server behavior only.

**Key fact:** the spatial catalog is **identical in 8.0.46 and 9.7.1** — probing
both shows no spatial function present in one but not the other. (The "newer" ones —
`ST_FrechetDistance`, `ST_Collect`, `ST_Transform`, … — all arrived within 8.0.x, so
a late 8.0 already has them; 9.x added none.)

`✓` = implemented in the POC today. `M1`/`M3` = milestone (see review-plan).

## I/O — readers (from WKT)  · M1 generic + M3 typed
`ST_GeomFromText`✓ M1, `ST_GeometryFromText` M3, `ST_PointFromText` M3,
`ST_LineFromText`/`ST_LineStringFromText` M3, `ST_PolyFromText`/`ST_PolygonFromText` M3,
`ST_MPointFromText`/`ST_MultiPointFromText` M3,
`ST_MLineFromText`/`ST_MultiLineStringFromText` M3,
`ST_MPolyFromText`/`ST_MultiPolygonFromText` M3,
`ST_GeomCollFromText`/`ST_GeomCollFromTxt`/`ST_GeometryCollectionFromText` M3.

## I/O — readers (from WKB)  · M1 generic + M3 typed
`ST_GeomFromWKB`✓ M1, `ST_GeometryFromWKB` M3, `ST_PointFromWKB` M3,
`ST_LineFromWKB`/`ST_LineStringFromWKB` M3, `ST_PolyFromWKB`/`ST_PolygonFromWKB` M3,
`ST_MPointFromWKB`/`ST_MultiPointFromWKB` M3, `ST_MLineFromWKB`/`ST_MultiLineStringFromWKB` M3,
`ST_MPolyFromWKB`/`ST_MultiPolygonFromWKB` M3,
`ST_GeomCollFromWKB`/`ST_GeometryCollectionFromWKB` M3.

## I/O — GeoJSON + writers  · M1
`ST_GeomFromGeoJSON`✓, `ST_AsText`✓, `ST_AsWKT`✓, `ST_AsBinary`✓, `ST_AsWKB`✓,
`ST_AsGeoJSON`✓.

## Constructors (function-call syntax)  · M1
`Point`✓, `LineString`✓, `Polygon`✓, `MultiPoint` M3, `MultiLineString` M3,
`MultiPolygon` M3, `GeometryCollection` M3.

## Accessors  · M1 core + M3 niche
`ST_X`✓ M1, `ST_Y`✓ M1, `ST_Latitude`✓ M1, `ST_Longitude`✓ M1, `ST_SRID`✓ M1,
`ST_GeometryType`✓ M1, `ST_Dimension`✓ M1, `ST_Envelope`✓ M1, `ST_IsEmpty`✓ M1,
`ST_IsValid`✓ M1, `ST_StartPoint`✓ M1, `ST_EndPoint`✓ M1, `ST_PointN`✓ M1,
`ST_NumPoints`✓ M1, `ST_ExteriorRing`✓ M1, `ST_NumInteriorRings`✓ M1.
M3 niche: `ST_IsSimple`, `ST_IsClosed`, `ST_InteriorRingN`, `ST_NumInteriorRing`
(singular alias), `ST_GeometryN`, `ST_NumGeometries`, `ST_Centroid`✓.

## Measurement  · M1 core + M3 variants
`ST_Area`✓ M1, `ST_Length`✓ M1, `ST_Distance`✓ M1, `ST_Distance_Sphere`✓ M1.
M3: `ST_FrechetDistance`, `ST_HausdorffDistance`.

## Predicates — ST_ (DE-9IM)  · M1
`ST_Within`✓, `ST_Contains`✓, `ST_Intersects`✓, `ST_Equals`✓, `ST_Disjoint`✓,
`ST_Touches`✓, `ST_Crosses`✓, `ST_Overlaps`✓.
(MySQL has **no** `ST_Covers`/`ST_CoveredBy`/`ST_Relate` — those are PostGIS; below.)

## Predicates — MBR (bounding-box)  · M3
`MBRContains`, `MBRWithin`, `MBRIntersects`, `MBRDisjoint`, `MBREquals`,
`MBROverlaps`, `MBRTouches`, `MBRCoveredBy`.

## Processing / analysis  · M3
`ST_Buffer`, `ST_Buffer_Strategy`, `ST_ConvexHull`, `ST_Union`, `ST_Intersection`,
`ST_Difference`, `ST_SymDifference`, `ST_Simplify`, `ST_Validate`, `ST_MakeEnvelope`,
`ST_SwapXY`, `ST_LineInterpolatePoint`, `ST_LineInterpolatePoints`,
`ST_PointAtDistance`, `ST_Transform` (cross-SRID — travels with the SRID work),
`ST_Collect` (aggregate).

## Geohash  · M3
`ST_GeoHash`, `ST_PointFromGeoHash`, `ST_LatFromGeoHash`, `ST_LongFromGeoHash`.

## PostGIS extras (NOT in MySQL — verified absent in 9.7 & 8.0)
The POC already ships `ST_Covers`✓ / `ST_CoveredBy`✓ — these are PostGIS, not MySQL.
**Policy (see review-plan): keep a PostGIS extra only when it is index-supported.**
`ST_Covers`/`ST_CoveredBy` qualify — they are now index-eligible region predicates
(`Covers ⊇ Contains`, `CoveredBy ⊇ Within`). Other PostGIS-only candidates, *not*
implemented and only "if index-supported / by demand": `ST_Relate` (DE-9IM matrix),
`ST_PointOnSurface`, `ST_Boundary`, `ST_IsRing`, `ST_Perimeter`, `ST_Subdivide`,
`ST_ClosestPoint`, `ST_Azimuth`. Also gone in modern MySQL: the bare non-`ST_`
aliases (`GeometryType`, `GLength`, …).

## POC coverage today
~37 of MySQL's scalar functions + 3 constructors (`Point`/`LineString`/`Polygon`) +
the 2 PostGIS extras (`ST_Covers`/`ST_CoveredBy`). The M3 tail to reach full MySQL
parity is the remainder (typed I/O variants, MBR family, processing/analysis,
geohash, niche accessors, `ST_Transform`, `ST_Collect`).
