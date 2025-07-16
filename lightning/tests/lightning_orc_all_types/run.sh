#!/bin/sh

set -eu
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

function run_with() {
	backend=$1
	config_file=$2
    if [ "$backend" = 'local' ]; then
        check_cluster_version 4 0 0 'local backend' || continue
    fi

    run_sql 'DROP DATABASE IF EXISTS orc_supported_db'

    bash run_lightning --backend $backend --config $config_file

    run_sql 'SELECT count(*) FROM orc_supported_db.orc_supported_table'
    check_contains 'count(*): 2'
}

# test backend=local and tidb.
for BACKEND in local tidb; do 
    run_with "tidb" "$CUR/config.toml"
done
