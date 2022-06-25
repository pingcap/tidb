#! /bin/bash

set -eux

. run_services

. br_tikv_outage_util

load

hint_finegrained=$TEST_DIR/hint_finegrained
hint_backup_start=$TEST_DIR/hint_backup_start
hint_get_backup_client=$TEST_DIR/hint_get_backup_client

cases=${cases:-'outage-at-finegrained outage outage-after-request'}

for failure in $cases; do
    rm -f "$hint_finegrained" "$hint_backup_start" "$hint_get_backup_client"
    export GO_FAILPOINTS="github.com/pingcap/tidb/br/pkg/backup/hint-backup-start=1*return(\"$hint_backup_start\");\
github.com/pingcap/tidb/br/pkg/backup/hint-fine-grained-backup=1*return(\"$hint_finegrained\");\
github.com/pingcap/tidb/br/pkg/conn/hint-get-backup-client=1*return(\"$hint_get_backup_client\")"
    if [ "$failure" = outage-at-finegrained ]; then
        export GO_FAILPOINTS="$GO_FAILPOINTS;github.com/pingcap/tidb/br/pkg/backup/noop-backup=return(true)"
    fi

    backup_dir=${TEST_DIR:?}/"backup{test:${TEST_NAME}|with:${failure}}"
    rm -rf "${backup_dir:?}"
    run_br backup full -s local://"$backup_dir" &
    backup_pid=$!
    single_point_fault $failure
    wait $backup_pid
    case $failure in
    scale-out | shutdown | outage-at-finegrained ) stop_services
        start_services ;;
    *) ;;
    esac


    check
done
