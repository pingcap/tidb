#!/bin/sh

set -eux

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

run_lightning --config "$CUR/config1.toml" 2>&1 | grep -q "checksum mismatched remote vs local"

run_sql 'DROP DATABASE IF EXISTS cm'
run_lightning --config "$CUR/config2.toml" 2>&1 | grep -q "checksum mismatched remote vs local"

run_sql 'DROP DATABASE IF EXISTS cm'
run_lightning --config "$CUR/config3.toml" 2>&1 | grep -q "checksum mismatched remote vs local"
