#!/bin/sh

set -eu
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

function run_with() {
	backend=$1
	config_file=$2
    if [ "$backend" = 'local' ]; then
        check_cluster_version 4 0 0 'local backend' || continue
    fi

    run_sql 'DROP DATABASE IF EXISTS test'

    bash run_lightning --backend $backend --config $config_file

    run_sql 'SELECT count(*) FROM test.binary_table'
    check_contains 'count(*): 3'

    run_sql 'SELECT hex(data), flag FROM test.binary_table WHERE id = 1'
    check_contains 'hex(data): 546869732069732061206D6573736167652E'
    check_contains 'flag: 97'
    run_sql 'SELECT hex(data), flag FROM test.binary_table WHERE id = 2'
    check_contains 'hex(data): 5468697320697320616E6F74686572206D6573736167652E'
    check_contains 'flag: 98'
    run_sql 'SELECT hex(data), flag FROM test.binary_table WHERE id = 3'
    check_contains 'hex(data): 546869732069732061207468697264206D6573736167652E'
    check_contains 'flag: 99'
}

# test backend=local and tidb.
for BACKEND in local tidb; do 
    run_with "tidb" "$CUR/config.toml"
done
