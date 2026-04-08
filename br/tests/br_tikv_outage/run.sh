#! /bin/bash

set -eux

. run_services

. br_tikv_outage_util

load

hint_backup_start=$TEST_DIR/hint_backup_start
hint_get_backup_client=$TEST_DIR/hint_get_backup_client

cases=${cases:-'shutdown'}

for failure in $cases; do
    rm -f "$hint_backup_start" "$hint_get_backup_client"
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/backup/hint-backup-start=1*return(\"$hint_backup_start\");\
github.com/pingcap/tidb/br/pkg/utils/hint-get-backup-client=1*return(\"$hint_get_backup_client\")"

    backup_dir=${TEST_DIR:?}/"backup{test:${TEST_NAME}|with:${failure}}"
    rm -rf "${backup_dir:?}"
    # Add ratelimit for backup task, otherwise the backup task will finishes too quickly.
    run_br backup full -s local://"$backup_dir" --concurrency 1 --ratelimit 3 &
    backup_pid=$!
    single_point_fault $failure
    wait $backup_pid

    # case 'shutdown' need to restart services
    stop_services
    start_services


    check
done
