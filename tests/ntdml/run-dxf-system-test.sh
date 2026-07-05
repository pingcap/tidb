#!/usr/bin/env bash
#
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK_DIR="${WORK_DIR:-$(mktemp -d -t tidb-ntdml-dxf.XXXXXX)}"
PROJECT="${PROJECT:-tidb-ntdml-dxf}"
TIDB_IMAGE="${TIDB_IMAGE:-tidb-ntdml-test:local}"
PD_IMAGE="${PD_IMAGE:-pingcap/pd:v8.5.0}"
TIKV_IMAGE="${TIKV_IMAGE:-pingcap/tikv:v8.5.0}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0}"
CURL_IMAGE="${CURL_IMAGE:-curlimages/curl:8.8.0}"

if docker compose version >/dev/null 2>&1; then
	COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
	COMPOSE=(docker-compose)
else
	echo "docker compose or docker-compose is required" >&2
	exit 1
fi

cleanup() {
	"${COMPOSE[@]}" -p "$PROJECT" -f "$WORK_DIR/docker-compose.yml" down -v --remove-orphans >/dev/null 2>&1 || true
	if [[ "${KEEP_WORK_DIR:-0}" != "1" ]]; then
		rm -rf "$WORK_DIR"
	fi
}
trap cleanup EXIT

if [[ "${BUILD_TIDB:-0}" == "1" ]]; then
	docker build -t "$TIDB_IMAGE" -f "$ROOT_DIR/Dockerfile" "$ROOT_DIR"
else
	echo "Using TIDB_IMAGE=$TIDB_IMAGE. Set BUILD_TIDB=1 to build this branch before running." >&2
fi

cat > "$WORK_DIR/docker-compose.yml" <<YAML
services:
  pd:
    image: ${PD_IMAGE}
    command:
      - --name=pd
      - --data-dir=/data/pd
      - --client-urls=http://0.0.0.0:2379
      - --advertise-client-urls=http://pd:2379
      - --peer-urls=http://0.0.0.0:2380
      - --advertise-peer-urls=http://pd:2380
      - --initial-cluster=pd=http://pd:2380
    healthcheck:
      test: ["CMD", "wget", "-q", "-O", "-", "http://127.0.0.1:2379/pd/api/v1/version"]
      interval: 2s
      timeout: 2s
      retries: 60
  tikv:
    image: ${TIKV_IMAGE}
    command:
      - --addr=0.0.0.0:20160
      - --advertise-addr=tikv:20160
      - --pd=pd:2379
      - --data-dir=/data/tikv
    depends_on:
      pd:
        condition: service_healthy
  tidb0:
    image: ${TIDB_IMAGE}
    command:
      - --store=tikv
      - --path=pd:2379
      - --host=0.0.0.0
      - --advertise-address=tidb0
      - -P
      - "4000"
      - --status
      - "10080"
      - --log-file=/tmp/tidb0.log
    ports:
      - "4000:4000"
      - "10080:10080"
    depends_on:
      - tikv
  tidb1:
    image: ${TIDB_IMAGE}
    command:
      - --store=tikv
      - --path=pd:2379
      - --host=0.0.0.0
      - --advertise-address=tidb1
      - -P
      - "4000"
      - --status
      - "10080"
      - --log-file=/tmp/tidb1.log
    ports:
      - "4001:4000"
      - "10081:10080"
    depends_on:
      - tikv
YAML

"${COMPOSE[@]}" -p "$PROJECT" -f "$WORK_DIR/docker-compose.yml" up -d
NETWORK="${PROJECT}_default"

sql() {
	docker run --rm -i --network "$NETWORK" "$MYSQL_IMAGE" \
		mysql --protocol=tcp -uroot -htidb0 -P4000 --default-character-set=utf8mb4 "$@"
}

scalar() {
	sql -N -B -e "$1" | tail -n 1
}

for _ in $(seq 1 90); do
	if sql -e "select 1" >/dev/null 2>&1; then
		break
	fi
	sleep 2
done
sql -e "select 1" >/dev/null

{
	cat <<'SQL'
SET GLOBAL tidb_enable_dist_task = 1;
DROP DATABASE IF EXISTS ntdml_system;
CREATE DATABASE ntdml_system;
USE ntdml_system;
SET tidb_nontransactional_dml_execution_mode = 'dxf';
SET tidb_nontransactional_dml_concurrency = 4;
CREATE TABLE t_int(id BIGINT PRIMARY KEY CLUSTERED, v INT NOT NULL);
SQL
	for start in $(seq 0 20 180); do
		values=()
		for i in $(seq "$start" $((start + 19))); do
			values+=("($i,$i)")
		done
		IFS=,
		echo "INSERT INTO t_int VALUES ${values[*]};"
		unset IFS
	done
	cat <<'SQL'
SPLIT TABLE t_int BETWEEN (0) AND (200) REGIONS 8;
BATCH ON id LIMIT 25 UPDATE t_int SET v = 100 WHERE id >= 0 AND id < 200;
SELECT IF(COUNT(*) = 200, 'ok', CONCAT('bad_int_count=', COUNT(*))) AS int_rows FROM t_int WHERE v = 100;
SQL
} > "$WORK_DIR/int.sql"
sql < "$WORK_DIR/int.sql"

docker restart "${PROJECT}-tidb1-1" >/dev/null
for _ in $(seq 1 60); do
	if docker run --rm --network "$NETWORK" "$CURL_IMAGE" -fsS http://tidb1:10080/status >/dev/null 2>&1; then
		break
	fi
	sleep 2
done

{
	cat <<'SQL'
USE ntdml_system;
SET tidb_nontransactional_dml_execution_mode = 'dxf';
SET tidb_nontransactional_dml_concurrency = 4;
CREATE TABLE t_varchar(
  id VARCHAR(64) COLLATE utf8mb4_bin PRIMARY KEY CLUSTERED,
  payload VARCHAR(64),
  marker JSON
);
SQL
	for start in $(seq 0 20 180); do
		values=()
		for i in $(seq "$start" $((start + 19))); do
			key=$(printf "v1:pacer_largepayload%04d" "$i")
			values+=("('$key','payload-$i',JSON_OBJECT('deleted_at','live'))")
		done
		IFS=,
		echo "INSERT INTO t_varchar VALUES ${values[*]};"
		unset IFS
	done
	cat <<'SQL'
SPLIT TABLE t_varchar BY
  ('v1:pacer_largepayload0050'),
  ('v1:pacer_largepayload0100'),
  ('v1:pacer_largepayload0150');
BATCH ON id LIMIT 30 UPDATE t_varchar
  SET marker = JSON_SET(marker, '$.deleted_at', CAST('null' AS JSON))
  WHERE id >= 'v1:pacer_largepayload0000' AND id < 'v1:pacer_largepayload0200';
SELECT IF(COUNT(*) = 200, 'ok', CONCAT('bad_varchar_count=', COUNT(*))) AS varchar_rows
  FROM t_varchar
  WHERE JSON_TYPE(JSON_EXTRACT(marker, '$.deleted_at')) = 'NULL';
BATCH ON id LIMIT 35 DELETE FROM t_varchar
  WHERE id >= 'v1:pacer_largepayload0040' AND id < 'v1:pacer_largepayload0160';
SELECT IF(COUNT(*) = 80, 'ok', CONCAT('bad_delete_remaining=', COUNT(*))) AS delete_rows FROM t_varchar;
SQL
} > "$WORK_DIR/varchar.sql"
sql < "$WORK_DIR/varchar.sql"

for _ in $(seq 1 30); do
	checkpoints="$(scalar "SELECT COUNT(*) FROM mysql.tidb_nontransactional_dml_checkpoint")"
	if [[ "$checkpoints" == "0" ]]; then
		break
	fi
	sleep 1
done
if [[ "$(scalar "SELECT COUNT(*) FROM mysql.tidb_nontransactional_dml_checkpoint")" != "0" ]]; then
	echo "checkpoint cleanup did not finish" >&2
	exit 1
fi

docker run --rm --network "$NETWORK" "$CURL_IMAGE" -fsS http://tidb0:10080/metrics \
	| grep -E 'tidb_session_non_transactional_dml_(task|chunk|rows)_total' >/dev/null

echo "parallel non-transactional DML DXF system test passed"
