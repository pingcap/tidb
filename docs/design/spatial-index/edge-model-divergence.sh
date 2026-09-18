#!/usr/bin/env bash
# Cross-engine check: MySQL and PostGIS disagree on the same point-in-polygon
# query, and the edge model is the only cause.
#
# One triangle, POLYGON((0 0, 80 0, 0 80, 0 0)) at SRID 4326, probed along
# longitude 70. Each engine draws the hypotenuse differently, so each puts the
# boundary at a different latitude:
#
#   planar, straight in lat/long   latitude 10          PostGIS geometry
#   great circle, on a sphere      latitude 45.000000   PostGIS geography, S2
#   Andoyer, on the ellipsoid      latitude 45.070155   MySQL
#
# A point in the 7,797 m band between the last two is inside for MySQL and
# outside for PostGIS. It sits about 945 m from each boundary, far enough that
# no floating-point edge case is involved.
#
# ST_Intersects is the one predicate spelled the same and supported on both
# sides: MySQL has no ST_Covers/ST_CoveredBy (only the bounding-box MBRCovers),
# and PostGIS geography has no ST_Within.
#
# Mind the axis order. MySQL reads 4326 as latitude first and PostGIS as
# longitude first, so the same place is POINT(45.035 70) in one and
# POINT(70 45.035) in the other. Getting this wrong measures the axis
# convention instead of the edge model.
#
# Needs the two reference containers:
#   docker start mysql-9.7      # port 3397, root/root
#   docker start postgis-18     # port 5432, postgres/postgres, database gis
set -uo pipefail

MYSQL_C=${MYSQL_C:-mysql-9.7}
PG_C=${PG_C:-postgis-18}
fail=0

for c in "$MYSQL_C" "$PG_C"; do
  if ! docker exec "$c" true >/dev/null 2>&1; then
    echo "container '$c' is not running: docker start $c" >&2
    exit 2
  fi
done

my() {  # my <lat> <lon> -> 1 or 0
  docker exec -i "$MYSQL_C" mysql -uroot -proot -N -B 2>/dev/null <<SQL
SELECT ST_Intersects(ST_GeomFromText('POINT($1 $2)',4326),
                     ST_GeomFromText('POLYGON((0 0, 80 0, 0 80, 0 0))',4326));
SQL
}
pg() {  # pg <lat> <lon> <geography|geometry> -> t or f
  if [ "$3" = geography ]; then
    docker exec -i "$PG_C" psql -U postgres -d gis -tA 2>/dev/null <<SQL
SELECT ST_Intersects(ST_SetSRID(ST_MakePoint($2,$1),4326)::geography,
                     'SRID=4326;POLYGON((0 0, 0 80, 80 0, 0 0))'::geography);
SQL
  else
    docker exec -i "$PG_C" psql -U postgres -d gis -tA 2>/dev/null <<SQL
SELECT ST_Intersects(ST_SetSRID(ST_MakePoint($2,$1),4326),
                     ST_SetSRID('POLYGON((0 0, 0 80, 80 0, 0 0))'::geometry,4326));
SQL
  fi
}

check() {  # check <label> <lat> <lon> <want_mysql> <want_geog> <want_geom>
  local label=$1 lat=$2 lon=$3 wm=$4 wgg=$5 wgm=$6
  local gm gg gy
  gm=$(my "$lat" "$lon"); gg=$(pg "$lat" "$lon" geography); gy=$(pg "$lat" "$lon" geometry)
  printf '%-28s %-8s %-10s %-10s' "$label" "$gm" "$gg" "$gy"
  if [ "$gm" = "$wm" ] && [ "$gg" = "$wgg" ] && [ "$gy" = "$wgm" ]; then
    printf '  ok\n'
  else
    printf '  FAIL want %s/%s/%s\n' "$wm" "$wgg" "$wgm"; fail=1
  fi
}

printf '%-28s %-8s %-10s %-10s  %s\n' \
  "point (lat lon)" "MySQL" "PG geog" "PG geom" "result"
printf '%-28s %-8s %-10s %-10s  %s\n' \
  "" "Andoyer" "great circ" "planar" ""
check "deep inside   10 10"     10     10 1 t t
check "far outside   70 70"     70     70 0 f f
check "below both    44.5 70"   44.5   70 1 t f
check "IN THE BAND   45.035 70" 45.035 70 1 f f
check "above all     45.5 70"   45.5   70 0 f f

echo
echo "boundary latitude at longitude 70, bracketed:"
printf '  MySQL           45.070155 -> %s   45.070200 -> %s\n' "$(my 45.070155 70)" "$(my 45.070200 70)"
printf '  PostGIS geog    45.0000   -> %s   45.0001   -> %s\n' "$(pg 45.0000 70 geography)" "$(pg 45.0001 70 geography)"

if [ "$fail" -eq 0 ]; then
  echo
  echo "PASS: the band point is inside for MySQL and outside for PostGIS,"
  echo "      and the edge model is the only difference between them."
else
  echo
  echo "FAIL: see above"
fi
exit "$fail"
