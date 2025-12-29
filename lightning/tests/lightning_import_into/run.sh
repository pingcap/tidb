#!/bin/bash
set -eux

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CP_FILE="/tmp/lightning_import_into_checkpoint.pb"
DB_NAME="import_into"
TABLE_NAME="t"

# Helper to run lightning with common arguments
run_import_into() {
    run_lightning --backend "import-into" --config "$CUR/config.toml" "$@"
}

cleanup() {
    echo "Cleaning up..."
    rm -f "$CP_FILE"
    run_sql "DROP DATABASE IF EXISTS $DB_NAME"
}

test_basic_import() {
    echo "******** Case 1: Basic import ********"
    cleanup
    run_sql "CREATE DATABASE $DB_NAME"
    run_import_into
    
    run_sql "SELECT count(*) FROM $DB_NAME.$TABLE_NAME"
    check_contains "count(*): 3"
}

test_resume() {
    echo "******** Case 2: Test resume with failpoint ********"
    cleanup
    run_sql "CREATE DATABASE $DB_NAME"
    
    export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importinto/FailAfterSubmission=return"
    set +e
    run_import_into --enable-checkpoint=1
    RET=$?
    set -e
    [ $RET -ne 0 ] || (echo "Should fail with failpoint"; exit 1)

    export GO_FAILPOINTS=""
    run_import_into --enable-checkpoint=1
    
    run_sql "SELECT count(*) FROM $DB_NAME.$TABLE_NAME"
    check_contains "count(*): 3"
}

test_invalid_data() {
    echo "******** Case 3: Test with invalid data ********"
    cleanup
    run_sql "CREATE DATABASE $DB_NAME"
    
    set +e
    run_import_into -d "$CUR/data_invalid" --enable-checkpoint=1
    RET=$?
    set -e
    [ $RET -ne 0 ] || (echo "Should fail with invalid data"; exit 1)
    grep -qi "job failed" "$TEST_DIR/lightning.log"
}

test_precheck() {
    echo "******** Case 4: Test precheck failure ********"
    # We don't cleanup here because we want to see the failed checkpoint from Case 3
    set +e
    run_import_into -d "$CUR/data_invalid" --enable-checkpoint=1 --check-requirements=1
    RET=$?
    set -e
    [ $RET -ne 0 ] || (echo "Should fail precheck"; exit 1)
    grep -qi "precheck.*failed" "$TEST_DIR/lightning.log"
}

test_cancel() {
    echo "******** Case 5: Test cancel ********"
    cleanup
    run_sql "CREATE DATABASE $DB_NAME"
    
    export GO_FAILPOINTS="github.com/pingcap/tidb/lightning/pkg/importinto/SlowDownPolling=sleep(5000)"
    run_import_into --enable-checkpoint=1 &
    LPID=$!
    
    sleep 2
    pkill -INT -f tidb-lightning.test || kill -INT $LPID
    wait $LPID || true
    
    run_sql "SHOW IMPORT JOBS"
    if ! grep -Eiq "cancelled|failed" "$TEST_DIR/sql_res.$TEST_NAME.txt"; then
        echo "Cancel failed: status not cancelled or failed"
        exit 1
    fi
    export GO_FAILPOINTS=""
}

# Main execution
test_basic_import
test_resume
test_invalid_data
test_precheck
test_cancel

echo "All tests passed!"
