#!/bin/sh
#
# Copyright 2019 PingCAP, Inc.
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

set -eu
DB="$TEST_NAME"
TABLE="usertable"
DDL_COUNT=5
LOG=/$TEST_DIR/backup.log
RESTORE_LOG=LOG=/$TEST_DIR/restore.log
BACKUP_STAT=/$TEST_DIR/backup_stat
RESOTRE_STAT=/$TEST_DIR/restore_stat
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

run_sql "CREATE DATABASE $DB;"
go-ycsb load mysql -P $CUR/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB

row_count_ori=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

for i in $(seq $DDL_COUNT); do
    run_sql "USE $DB; ALTER TABLE $TABLE ADD INDEX (FIELD$i);"
done

for i in $(seq $DDL_COUNT); do
    if (( RANDOM % 2 )); then
        run_sql "USE $DB; ALTER TABLE $TABLE DROP INDEX FIELD$i;"
    fi
done

# run analyze to generate stats
run_sql "analyze table $DB.$TABLE;"
# record the stats and remove last_update_version
# it's enough to compare with restore stats
# the stats looks like
#
#{
# "columns":
#  "ol_amount": {
#    "histogram": {
#      "ndv": 829568,
#      "buckets": [
#        {
#          "count": 7066,
#          "lower_bound": "MS40Mw==",
#          "upper_bound": "NDkuMjI=",
#          "repeats": 2,
#          "ndv": 0
#        },
#       ...(nearly 1000 rows)
#      ],
#      "cm_sketch": {
#          "top_n": [
#            {
#              "data": "BgYCgAAA",
#              "count": 4204452
#            }
#          ],
#          "default_value": 0
#        },
#        "null_count": 0,
#        "tot_col_size": 29988125,
#        "stats_ver": 2
#      },
#  }
#  "ol_d_id": { similar value },
#  "ol_delivery_d": { similar value },
#  "ol_dist_info": { similar value },
#  "ol_i_id": { similar value },
#  "ol_number": { similar value },
#  "ol_o_id": { similar value },
#  "ol_quantity": { similar value },
#  "ol_supply_w_id": { similar value },
#  "ol_w_id": { similar value },
#  },
# "indices": {
#   similar value
#  }
# }

run_curl https://$TIDB_STATUS_ADDR/stats/dump/$DB/$TABLE | jq '{columns,indices} | map_values(with_entries(del(.value.last_update_version, .value.correlation)))' > $BACKUP_STAT

# ensure buckets in stats
cat $BACKUP_STAT | grep buckets 
if [ $? -ne 0 ] ;then
    echo "TEST: [$TEST_NAME] fail due to grep not find buckets in stats file"
    echo $(cat $BACKUP_STAT)
    exit 1
else
    echo "grep find buckets in stats file"
fi

# backup full
echo "backup start with stats..."
# Do not log to terminal
unset BR_LOG_TO_TERM
cluster_index_before_backup=$(run_sql "show variables like '%cluster%';" | awk '{print $2}')

run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB" --log-file $LOG --ignore-stats=false || cat $LOG
checksum_count=$(cat $LOG | grep "checksum success" | wc -l | xargs)

if [ "${checksum_count}" -lt "1" ];then
    echo "TEST: [$TEST_NAME] fail on fast checksum"
    echo $(cat $LOG | grep checksum)
    exit 1
fi

# when we have backup stats during backup, we cannot close domain during one shot session.
# so we can check the log count of `one shot domain closed`.
# we will call UseOneShotSession once to get the value global variable.
one_shot_session_count=$(cat $LOG | grep "one shot session closed" | wc -l | xargs)
one_shot_domain_count=$(cat $LOG | grep "one shot domain closed" | wc -l | xargs)
if [ "${one_shot_session_count}" -ne "1" ] || [ "$one_shot_domain_count" -ne "0" ];then
    echo "TEST: [$TEST_NAME] fail on one shot session check, $one_shot_session_count, $one_shot_domain_count"
    exit 1
fi

echo "backup start without stats..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/${DB}_disable_stats" --concurrency 4

run_sql "DROP DATABASE $DB;"

cluster_index_before_restore=$(run_sql "show variables like '%cluster%';" | awk '{print $2}')
# keep cluster index enable or disable at same time.
if [[ "${cluster_index_before_backup}" != "${cluster_index_before_restore}" ]]; then
  echo "TEST: [$TEST_NAME] must enable or disable cluster_index at same time"
  echo "cluster index before backup is $cluster_index_before_backup"
  echo "cluster index before restore is $cluster_index_before_restore"
  exit 1
fi

echo "restore full without stats..."
run_br restore full -s "local://$TEST_DIR/${DB}_disable_stats" --pd $PD_ADDR
curl $TIDB_IP:10080/stats/dump/$DB/$TABLE | jq '{columns,indices} | map_values(with_entries(del(.value.last_update_version, .value.correlation)))' > $RESOTRE_STAT

# stats should not be equal because we disable stats by default.
if diff -q $BACKUP_STAT $RESOTRE_STAT > /dev/null
then
  echo "TEST: [$TEST_NAME] fail due to stats are equal"
  grep ERROR $LOG
  exit 1
fi

# clear restore environment
run_sql "DROP DATABASE $DB;"

# restore full
echo "restore start..."
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/restore/snap_client/restore-createtables-error=return(true)"
run_br restore full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --log-file $RESTORE_LOG --ddl-batch-size=128 || { cat $RESTORE_LOG; }
export GO_FAILPOINTS=""

panic_count=$(cat $RESTORE_LOG | grep "panic"| wc -l)
if [ "${panic_count}" != "0" ];then
    echo "TEST: [$TEST_NAME] fail on batch create tables"
    exit 1
fi

# clear restore environment
run_sql "DROP DATABASE $DB;"
run_sql "DROP DATABASE __tidb_br_temporary_mysql;"
# restore full
echo "restore start..."
export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/pdutil/PDEnabledPauseConfig=return(true)"
run_br restore full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --log-file $LOG || { cat $LOG; exit 1; }
export GO_FAILPOINTS=""

pause_count=$(cat $LOG | grep "pause configs successful"| wc -l | xargs)
if [ "${pause_count}" != "1" ];then
    echo "TEST: [$TEST_NAME] fail on pause config"
    exit 1
fi

BR_LOG_TO_TERM=1

skip_count=$(cat $LOG | grep "range is empty" | wc -l | xargs)

# ensure there are only less than two(write + default) range empty error,
# because backup range end key is large than reality.
# so the last region may download nothing.
# FIXME maybe we can treat endkey specially in the future.
if [ "${skip_count}" -gt "2" ];then
    echo "TEST: [$TEST_NAME] fail on download sst, too many skipped range"
    echo $(cat $LOG | grep "range is empty")
    exit 1
fi

run_curl https://$TIDB_STATUS_ADDR/stats/dump/$DB/$TABLE | jq '{columns,indices} | map_values(with_entries(del(.value.last_update_version, .value.correlation)))' > $RESOTRE_STAT

if diff -q $BACKUP_STAT $RESOTRE_STAT > /dev/null
then
  echo "stats are equal"
else
  echo "TEST: [$TEST_NAME] fail due to stats are not equal"
  grep ERROR $LOG
  cat $BACKUP_STAT | tail -n 1000
  cat $RESOTRE_STAT | tail -n 1000
  exit 1
fi

row_count_new=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

fail=false
if [ "${row_count_ori}" != "${row_count_new}" ];then
    fail=true
    echo "TEST: [$TEST_NAME] fail on database $DB${i}"
fi
echo "database $DB$ [original] row count: ${row_count_ori}, [after br] row count: ${row_count_new}"

if $fail; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_sql "DROP DATABASE $DB;"
