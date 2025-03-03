#!/bin/sh
#
# Copyright 2024 PingCAP, Inc.
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

# disable global ENCRYPTION_ARGS and ENABLE_ENCRYPTION_CHECK for this script
ENCRYPTION_ARGS=""
ENABLE_ENCRYPTION_CHECK=false
export ENCRYPTION_ARGS
export ENABLE_ENCRYPTION_CHECK

set -eu
. run_services
CUR=$(cd "$(dirname "$0")" && pwd)

# const value
PREFIX="encryption_backup"
res_file="$TEST_DIR/sql_res.$TEST_NAME.txt"
DB="$TEST_NAME"
TABLE="usertable"
DB_COUNT=3
TASK_NAME="encryption_test"

create_db_with_table() {
    for i in $(seq $DB_COUNT); do
        run_sql "CREATE DATABASE $DB${i};"
        go-ycsb load mysql -P $CUR/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i} -p recordcount=1000
    done
}

start_log_backup() {
    _storage=$1
    _encryption_args=$2
    echo "start log backup task"
    run_br --pd "$PD_ADDR" log start --task-name $TASK_NAME -s "$_storage" $_encryption_args
}

drop_db() {
    for i in $(seq $DB_COUNT); do
        run_sql "DROP DATABASE IF EXISTS $DB${i};"
    done
}

insert_additional_data() {
    local prefix=$1
    for i in $(seq $DB_COUNT); do
        go-ycsb load mysql -P $CUR/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i} -p insertcount=1000 -p insertstart=1000000 -p recordcount=1001000 -p workload=core
    done
}

calculate_checksum() {
    local db=$1
    local checksum=$(run_sql "USE $db; ADMIN CHECKSUM TABLE $TABLE;" | awk '/CHECKSUM/{print $2}')
    echo $checksum
}

check_db_consistency() {
    fail=false
    for i in $(seq $DB_COUNT); do
        local original_checksum=${checksum_ori[i]}
        local new_checksum=$(calculate_checksum "$DB${i}")
        
        if [ "$original_checksum" != "$new_checksum" ]; then
            fail=true
            echo "TEST: [$TEST_NAME] checksum mismatch on database $DB${i}"
            echo "Original checksum: $original_checksum, New checksum: $new_checksum"
        else
            echo "Database $DB${i} checksum match: $new_checksum"
        fi
    done

    if $fail; then
        echo "TEST: [$TEST_NAME] data consistency check failed!"
        return 1
    fi
    echo "TEST: [$TEST_NAME] data consistency check passed."
    return 0
}

verify_dbs_empty() {
    echo "Verifying databases are empty"
    for i in $(seq $DB_COUNT); do
        db_name="$DB${i}"
        table_count=$(run_sql "USE $db_name; SHOW TABLES;" | wc -l)
        if [ "$table_count" -ne 0 ]; then
            echo "ERROR: Database $db_name is not empty"
            return 1
        fi
    done
    echo "All databases are empty"
    return 0
}

run_backup_restore_test() {
    local encryption_mode=$1
    local full_encryption_args=$2
    local log_encryption_args=$3

    echo "===== run_backup_restore_test $encryption_mode $full_encryption_args $log_encryption_args ====="

    restart_services || { echo "Failed to restart services"; exit 1; }

    # Drop existing databases before starting the test
    drop_db || { echo "Failed to drop databases"; exit 1; }

    # Start log backup
    start_log_backup "local://$TEST_DIR/$PREFIX/log" "$log_encryption_args" || { echo "Failed to start log backup"; exit 1; }

    # Create test databases and insert initial data
    create_db_with_table || { echo "Failed to create databases and tables"; exit 1; }

    # Calculate and store original checksums
    for i in $(seq $DB_COUNT); do
        checksum_ori[${i}]=$(calculate_checksum "$DB${i}") || { echo "Failed to calculate initial checksum"; exit 1; }
    done

    # Full backup
    echo "run full backup with $encryption_mode"
    run_br --pd "$PD_ADDR" backup full -s "local://$TEST_DIR/$PREFIX/full" $full_encryption_args || { echo "Full backup failed"; exit 1; }

    # Insert additional test data
    insert_additional_data "${encryption_mode}_after_full_backup" || { echo "Failed to insert additional data"; exit 1; }

    # Update checksums after inserting additional data
    for i in $(seq $DB_COUNT); do
        checksum_ori[${i}]=$(calculate_checksum "$DB${i}") || { echo "Failed to calculate checksum after insertion"; exit 1; }
    done

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance $TASK_NAME || { echo "Failed to wait for log checkpoint"; exit 1; }

    #sanity check pause still works
    run_br log pause --task-name $TASK_NAME --pd $PD_ADDR || { echo "Failed to pause log backup"; exit 1; }

    #sanity check resume still works
    run_br log resume --task-name $TASK_NAME --pd $PD_ADDR || { echo "Failed to resume log backup"; exit 1; }

    #sanity check stop still works
    run_br log stop --task-name $TASK_NAME --pd $PD_ADDR || { echo "Failed to stop log backup"; exit 1; }

    # restart service should clean up everything
    restart_services || { echo "Failed to restart services"; exit 1; }

    verify_dbs_empty || { echo "Failed to verify databases are empty"; exit 1; }
    
    # Run pitr restore and measure the performance
    echo "restore log backup with $full_encryption_args and $log_encryption_args"
    local start_time=$(date +%s.%N)
    timeout 300 run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full" $full_encryption_args $log_encryption_args || { 
        echo "Log backup restore failed or timed out after 5 minutes"
        exit 1
    }
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc | awk '{printf "%.3f", $0}')
    echo "${encryption_mode} took ${duration} seconds"
    echo "${encryption_mode},${duration}" >> "$TEST_DIR/performance_results.csv"

    # Check data consistency after restore
    echo "check data consistency after restore"
    check_db_consistency || { echo "TEST: [$TEST_NAME] $encryption_mode backup and restore (including log) failed"; exit 1; }

    # sanity check truncate still works
    # make sure some files exists in log dir
    log_dir="$TEST_DIR/$PREFIX/log"
    if [ -z "$(ls -A $log_dir)" ]; then
        echo "Error: No files found in the log directory $log_dir"
        exit 1
    else
        echo "Files exist in the log directory $log_dir"
    fi
    current_time=$(date -u +"%Y-%m-%d %H:%M:%S+0000")
    run_br log truncate -s "local://$TEST_DIR/$PREFIX/log" --until "$current_time" -y || { echo "Failed to truncate log backup"; exit 1; }
    # make sure no files exist in log dir
    if [ -z "$(ls -A $log_dir)" ]; then
        echo "Error: Files still exist in the log directory $log_dir"
        exit 1
    else
        echo "No files exist in the log directory $log_dir"
    fi

    # Clean up after the test
    drop_db || { echo "Failed to drop databases after test"; exit 1; }
    rm -rf "$TEST_DIR/$PREFIX"

    echo "TEST: [$TEST_NAME] $encryption_mode backup and restore (including log) passed"
}

start_and_wait_for_localstack() {
    # Start LocalStack in the background with only the required services
    SERVICES=s3,ec2,kms localstack start -d

    echo "Waiting for LocalStack services to be ready..."
    max_attempts=30
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        response=$(curl -s "http://localhost:4566/_localstack/health")
        if echo "$response" | jq -e '.services.s3 == "running" or .services.s3 == "available"' > /dev/null && \
           echo "$response" | jq -e '.services.ec2 == "running" or .services.ec2 == "available"' > /dev/null && \
           echo "$response" | jq -e '.services.kms == "running" or .services.kms == "available"' > /dev/null; then
            echo "LocalStack services are ready"
            return 0
        fi
        attempt=$((attempt+1))
        echo "Waiting for LocalStack services... Attempt $attempt of $max_attempts"
        sleep 2
    done

    echo "LocalStack services did not become ready in time"
    localstack stop
    return 1
}

test_backup_encrypted_restore_unencrypted() {
    echo "===== Testing backup with encryption, restore without encryption ====="
    
    restart_services || { echo "Failed to restart services"; exit 1; }

    # Start log backup
    start_log_backup "local://$TEST_DIR/$PREFIX/log" "--log.crypter.method AES256-CTR --log.crypter.key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" || { echo "Failed to start log backup"; exit 1; }

    # Create test databases and insert initial data
    create_db_with_table || { echo "Failed to create databases and tables"; exit 1; }

    # Backup with encryption
    run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$PREFIX/full --crypter.method AES256-CTR --crypter.key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

    # Insert additional test data
    insert_additional_data "insert_after_full_backup" || { echo "Failed to insert additional data"; exit 1; }

    . "$CUR/../br_test_utils.sh" && wait_log_checkpoint_advance $TASK_NAME || { echo "Failed to wait for log checkpoint"; exit 1; }


    # Stop and clean the cluster
    restart_services || { echo "Failed to restart services"; exit 1; }

    # Try to restore without encryption (this should fail)
    if run_br --pd "$PD_ADDR" restore point -s "local://$TEST_DIR/$PREFIX/log" --full-backup-storage "local://$TEST_DIR/$PREFIX/full --crypter.method AES256-CTR --crypter.key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"; then
        echo "Error: Restore without encryption should have failed, but it succeeded"
        exit 1
    else
        echo "Restore without encryption failed as expected"
    fi

    # Clean up after the test
    drop_db || { echo "Failed to drop databases after test"; exit 1; }
    rm -rf "$TEST_DIR/$PREFIX"

    echo "TEST: test_backup_encrypted_restore_unencrypted passed"
}


test_plaintext() {
    run_backup_restore_test "plaintext" "" ""
}

test_plaintext_data_key() {
    run_backup_restore_test "plaintext-data-key" "--crypter.method AES256-CTR --crypter.key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" "--log.crypter.method AES256-CTR --log.crypter.key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}

test_local_master_key() {
    _MASTER_KEY_DIR="$TEST_DIR/$PREFIX/master_key"
    mkdir -p "$_MASTER_KEY_DIR"
    openssl rand -hex 32 > "$_MASTER_KEY_DIR/master.key"

    _MASTER_KEY_PATH="local:///$_MASTER_KEY_DIR/master.key"

    run_backup_restore_test "local_master_key" "" "--master-key-crypter-method AES256-CTR --master-key $_MASTER_KEY_PATH"

    rm -rf "$_MASTER_KEY_DIR"
}

test_aws_kms() {
    # Start LocalStack and wait for services to be ready
    if ! start_and_wait_for_localstack; then
        echo "Failed to start LocalStack services"
        exit 1
    fi

    # localstack listening port
    ENDPOINT="http://localhost:4566"

    # Create KMS key using curl
    KMS_RESPONSE=$(curl -X POST "$ENDPOINT/kms/" \
     -H "Content-Type: application/x-amz-json-1.1" \
     -H "X-Amz-Target: TrentService.CreateKey" \
     -d '{
           "Description": "My test key",
           "KeyUsage": "ENCRYPT_DECRYPT",
           "Origin": "AWS_KMS"
         }')

    echo "KMS CreateKey response: $KMS_RESPONSE"

    AWS_KMS_KEY_ID=$(echo "$KMS_RESPONSE" | jq -r '.KeyMetadata.KeyId')
    AWS_ACCESS_KEY_ID="TEST"
    AWS_SECRET_ACCESS_KEY="TEST"
    REGION="us-east-1"

    AWS_KMS_URI="aws-kms:///${AWS_KMS_KEY_ID}?AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}&AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}&REGION=${REGION}&ENDPOINT=${ENDPOINT}"

    run_backup_restore_test "aws_kms" "--crypter.method AES256-CTR --crypter.key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" "--master-key-crypter-method AES256-CTR --master-key $AWS_KMS_URI"

    # Stop LocalStack
    localstack stop
}

test_aws_kms_with_iam() {
    # Start LocalStack and wait for services to be ready
    if ! start_and_wait_for_localstack; then
        echo "Failed to start LocalStack services"
        exit 1
    fi

    # localstack listening port
    ENDPOINT="http://localhost:4566"

    # Create KMS key using curl
    KMS_RESPONSE=$(curl -X POST "$ENDPOINT/kms/" \
     -H "Content-Type: application/x-amz-json-1.1" \
     -H "X-Amz-Target: TrentService.CreateKey" \
     -d '{
           "Description": "My test key",
           "KeyUsage": "ENCRYPT_DECRYPT",
           "Origin": "AWS_KMS"
         }')

    echo "KMS CreateKey response: $KMS_RESPONSE"

    AWS_KMS_KEY_ID=$(echo "$KMS_RESPONSE" | jq -r '.KeyMetadata.KeyId')
    # export these two as required by aws-kms backend
    export AWS_ACCESS_KEY_ID="TEST"
    export AWS_SECRET_ACCESS_KEY="TEST"
    REGION="us-east-1"

    AWS_KMS_URI="aws-kms:///${AWS_KMS_KEY_ID}?&REGION=${REGION}&ENDPOINT=${ENDPOINT}"

    run_backup_restore_test "aws_kms" "--crypter.method AES256-CTR --crypter.key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" "--master-key-crypter-method AES256-CTR --master-key $AWS_KMS_URI"

    # Stop LocalStack
    localstack stop
}

test_gcp_kms() {
    # Ensure GCP credentials are set
    if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
        echo "GCP credentials not set. Skipping GCP KMS test."
        return
    fi

    # Replace these with your actual GCP KMS details
    GCP_PROJECT_ID="carbide-network-435219-q3"
    GCP_LOCATION="us-west1"
    GCP_KEY_RING="local-kms-testing"
    GCP_KEY_NAME="kms-testing-key"
    GCP_CREDENTIALS="$GOOGLE_APPLICATION_CREDENTIALS"

    GCP_KMS_URI="gcp-kms:///projects/$GCP_PROJECT_ID/locations/$GCP_LOCATION/keyRings/$GCP_KEY_RING/cryptoKeys/$GCP_KEY_NAME?AUTH=specified&CREDENTIALS=$GCP_CREDENTIALS"

    run_backup_restore_test "gcp_kms" "--crypter.method AES256-CTR --crypter.key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" "--master-key-crypter-method AES256-CTR --master-key $GCP_KMS_URI"
}

test_mixed_full_encrypted_log_plain() {
    local full_encryption_args="--crypter.method AES256-CTR --crypter.key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    local log_encryption_args=""

    run_backup_restore_test "mixed_full_encrypted_log_plain" "$full_encryption_args" "$log_encryption_args"
}

test_mixed_full_plain_log_encrypted() {
    local full_encryption_args=""
    local log_encryption_args="--log.crypter.method AES256-CTR --log.crypter.key 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

    run_backup_restore_test "mixed_full_plain_log_encrypted" "$full_encryption_args" "$log_encryption_args"
}

# Initialize performance results file
echo "Operation,Encryption Mode,Duration (seconds)" > "$TEST_DIR/performance_results.csv"

# Run tests
test_backup_encrypted_restore_unencrypted
test_plaintext
test_plaintext_data_key
test_local_master_key
# localstack not working with older glibc version in our centos7 base image...
#test_aws_kms
#test_aws_kms_with_iam
test_mixed_full_encrypted_log_plain
test_mixed_full_plain_log_encrypted

# uncomment for manual GCP KMS testing
#test_gcp_kms

echo "All encryption tests passed successfully"

# Display performance results
echo "Performance Results:"
cat "$TEST_DIR/performance_results.csv"

