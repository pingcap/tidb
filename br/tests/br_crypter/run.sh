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
DB_COUNT=3

function create_db_with_table(){
    for i in $(seq $DB_COUNT); do
        run_sql "CREATE DATABASE $DB${i};"
        go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
    done
}

function drop_db(){
    for i in $(seq $DB_COUNT); do
        run_sql "DROP DATABASE $DB${i};"
    done
}

function check_db_row(){
    for i in $(seq $DB_COUNT); do
        row_count_new[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
    done

    fail=false
    for i in $(seq $DB_COUNT); do
        if [ "${row_count_ori[i]}" != "${row_count_new[i]}" ];then
            fail=true
            echo "TEST: [$TEST_NAME] fail on database $DB${i}"
        fi
        echo "database $DB${i} [original] row count: ${row_count_ori[i]}, [after br] row count: ${row_count_new[i]}"
    done

    if $fail; then
        echo "TEST: [$TEST_NAME] failed!"
        exit 1
    fi
}

function test_crypter_plaintext(){
    echo "backup with crypter method of plaintext"
    run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB/plaintext" --crypter.method "plaintext"

    drop_db

    echo "restore with crypter method of plaintext"
    run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB/plaintext" --crypter.method "PLAINTEXT"

    check_db_row
}

function test_crypter(){
    CRYPTER_METHOD=$1
    CRYPTER_KEY=$2
    CRYPTER_WRONG_KEY=$3
    CRYPTER_KEY_FILE=$TEST_DIR/$DB/$CRYPTER_METHOD-cipher-key
    CRYPTER_WRONG_KEY_FILE=$TEST_DIR/$DB/$CRYPTER_METHOD-wrong-cipher-key

    echo "backup crypter method of $CRYPTER_METHOD with the key of $CRYPTER_KEY"
    run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB/$CRYPTER_METHOD" \
        --crypter.method $CRYPTER_METHOD  --crypter.key $CRYPTER_KEY 

    drop_db

    echo "restore crypter method of $CRYPTER_METHOD with wrong key of $CRYPTER_WRONG_KEY"
    restore_fail=0
    run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB/$CRYPTER_METHOD" \
        --crypter.method $CRYPTER_METHOD  --crypter.key $CRYPTER_WRONG_KEY || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "TEST: [$TEST_NAME] test restore crypter with wrong key failed!"
        exit 1
    fi

    echo "restore crypter method of $CRYPTER_METHOD with the key of $CRYPTER_KEY"
    run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB/$CRYPTER_METHOD" \
        --crypter.method $CRYPTER_METHOD  --crypter.key $CRYPTER_KEY

    check_db_row

    echo $CRYPTER_KEY > $CRYPTER_KEY_FILE
    echo $CRYPTER_WRONG_KEY > $CRYPTER_WRONG_KEY_FILE

    echo "backup crypter method of $CRYPTER_METHOD with the key-file of $CRYPTER_KEY_FILE"
    run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB/${CRYPTER_METHOD}_file" \
        --use-backupmeta-v2=true --crypter.method $CRYPTER_METHOD  --crypter.key-file $CRYPTER_KEY_FILE

    drop_db

    echo "backup crypter method of $CRYPTER_METHOD with the wrong key-file of $CRYPTER_WRONG_KEY_FILE"
    restore_fail=0
    run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB/${CRYPTER_METHOD}_file" \
        --crypter.method $CRYPTER_METHOD  --crypter.key-file $CRYPTER_WRONG_KEY_FILE || restore_fail=1
    if [ $restore_fail -ne 1 ]; then
        echo "TEST: [$TEST_NAME] test restore with wrong key-file failed!"
        exit 1
    fi

    echo "restore crypter method of $CRYPTER_METHOD with the key-file of $CRYPTER_KEY_FILE"
    run_br --pd $PD_ADDR restore full -s "local://$TEST_DIR/$DB/${CRYPTER_METHOD}_file" \
        --crypter.method $CRYPTER_METHOD --crypter.key-file $CRYPTER_KEY_FILE

    check_db_row
}

# Create dbs with table
create_db_with_table

# Get the original row count from dbs
for i in $(seq $DB_COUNT); do
    row_count_ori[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

# Test crypter.method=plaintext for br
test_crypter_plaintext

# Test crypter.method=AESXXX for br
METHOD=aes128-ctr
KEY="0123456789abcdef0123456789abcdef"
WRONG_KEY="0123456789abcdef0123456789abcdee"
test_crypter $METHOD $KEY $WRONG_KEY

METHOD=AES192-CTR
KEY="0123456789abcdef0123456789abcdef0123456789abcdef"
WRONG_KEY="0123456789abcdef0123456789abcdef0123456789abcde"
test_crypter $METHOD $KEY $WRONG_KEY

METHOD=AES256-CTR
KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
WRONG_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeff"
test_crypter $METHOD $KEY $WRONG_KEY

# Drop dbs finally
drop_db
    
