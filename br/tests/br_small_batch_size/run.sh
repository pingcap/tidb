#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
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

random_values() {
    length=$1
    count=$2
    python -c "
import random
import string
for ignored in range($count):
    print(''.join(random.choice(string.ascii_letters) for _ in range($length)))" | 
    awk '{print "(1" $1 "1)"}' | 
    tr "\n1" ",'" | 
    sed 's/,$//'
}

create_and_insert() {
    table_name=$1
    record_count=$2
    run_sql "CREATE TABLE $DB.$table_name(k varchar(256) primary key)"
    stmt="INSERT INTO $DB.$table_name VALUES `random_values 255 $record_count`"
    echo $stmt | mysql -uroot -h127.0.0.1 -P4000
}

check_size() {
    table_name=$1
    record_count=$2

    count=`run_sql 'select count(*) from $DB.$table_name' | awk '/count/{print $2}'`

    if [ $count -ne $record_count ]; then
        echo "check size failed: $count vs $record_count"
    fi
}

set -eu
DB="$TEST_NAME"
TABLE="usertable"

run_sql "CREATE DATABASE $DB;"

record_counts=(10000 10010 10086)
for i in $record_counts; do
    create_and_insert "t$i" $i
done
go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB


echo "backup start..."
backup_dir="$TEST_DIR/${TEST_NAME}_backup"
rm -rf $backup_dir
run_br backup full -s "local://$backup_dir" --pd $PD_ADDR

run_sql "drop database $DB"


echo "restore start..."
GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/task/small-batch-size=return(2)" \
run_br restore full -s "local://$backup_dir" --pd $PD_ADDR --ratelimit 1024

for i in $record_counts; do
    check_size "t$i" $i
done
check_size $TABLE 10000

run_sql "DROP DATABASE $DB"
