#!/usr/bin/env bash
#
# Copyright 2022 PingCAP, Inc.
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

# This script split the integration tests into 9 groups to support parallel group tests execution.
# all the integration tests are located in br/tests directory. only the directories
# containing run.sh will be considered as valid lightning integration tests. the script will print the total case number

set -eo pipefail

# Step 1
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
group=$1
export COV_DIR="/tmp/group_cover"
rm -rf $COV_DIR
mkdir -p $COV_DIR

# Define groups
# Note: If new group is added, the group name must also be added to CI
# * https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tidb/latest/pull_lightning_integration_test.groovy
# Each group of tests consumes as much time as possible, thus reducing CI waiting time.
# Putting multiple light tests together and heavy tests in a separate group.
declare -A groups
groups=(
	["G00"]='lightning_auto_random_default lightning_bom_file lightning_character_sets lightning_check_partial_imported lightning_checkpoint lightning_checkpoint_chunks lightning_checkpoint_columns lightning_checkpoint_dirty_tableid'
	["G01"]='lightning_checkpoint_engines lightning_checkpoint_engines_order lightning_checkpoint_error_destroy lightning_checkpoint_parquet lightning_checkpoint_timestamp lightning_checksum_mismatch lightning_cmdline_override lightning_column_permutation lightning_common_handle lightning_compress lightning_concurrent-restore'
	["G02"]='lightning_config_max_error lightning_config_skip_csv_header lightning_csv lightning_default-columns lightning_disable_scheduler_by_key_range lightning_disk_quota lightning_distributed_import lightning_drop_other_tables_halfway lightning_duplicate_detection lightning_duplicate_detection_new lightning_duplicate_resolution_error lightning_duplicate_resolution_error_pk_multiple_files lightning_duplicate_resolution_error_uk_multiple_files lightning_duplicate_resolution_error_uk_multiple_files_multicol_index lightning_duplicate_resolution_incremental'
	["G03"]='lightning_duplicate_resolution_merge lightning_duplicate_resolution_replace_multiple_keys_clustered_pk lightning_duplicate_resolution_replace_multiple_keys_nonclustered_pk lightning_duplicate_resolution_replace_multiple_unique_keys_clustered_pk lightning_duplicate_resolution_replace_multiple_unique_keys_nonclustered_pk lightning_duplicate_resolution_replace_one_key lightning_duplicate_resolution_replace_one_key_multiple_conflicts_clustered_pk lightning_duplicate_resolution_replace_one_key_multiple_conflicts_nonclustered_pk'
	["G04"]='lightning_duplicate_resolution_replace_one_unique_key_clustered_pk lightning_duplicate_resolution_replace_one_unique_key_multiple_conflicts_clustered_pk lightning_duplicate_resolution_replace_one_unique_key_multiple_conflicts_nonclustered_pk lightning_duplicate_resolution_replace_one_unique_key_nonclustered_varchar_pk lightning_error_summary lightning_examples lightning_exotic_filenames lightning_extend_routes'
	["G05"]='lightning_fail_fast lightning_fail_fast_on_nonretry_err lightning_file_routing lightning_foreign_key lightning_gcs lightning_generated_columns lightning_ignore_columns lightning_import_compress lightning_incremental lightning_issue_282 lightning_issue_40657 lightning_issue_410 lightning_issue_519 lightning_local_backend lightning_max_incr'
	["G06"]='lightning_max_random lightning_multi_valued_index lightning_new_collation lightning_no_schema lightning_parquet lightning_partition_incremental lightning_partitioned-table lightning_record_network lightning_reload_cert lightning_restore lightning_routes lightning_routes_panic lightning_row-format-v2 lightning_s3'
	["G07"]='lightning_shard_rowid lightning_source_linkfile lightning_sqlmode lightning_tidb_duplicate_data lightning_tidb_rowid lightning_tiflash lightning_tikv_multi_rocksdb lightning_too_many_columns lightning_tool_135'
	["G08"]='lightning_tool_1420 lightning_tool_1472 lightning_tool_241 lightning_ttl lightning_unused_config_keys lightning_various_types lightning_view lightning_write_batch lightning_write_limit lightning_pd_leader_switch lightning_add_index lightning_alter_random lightning_auto_columns'
)

# Get other lightning cases not in groups, to avoid missing any case
others=()
for script in "$CUR"/*/run.sh; do
	test_name="$(basename "$(dirname "$script")")"
	if [[ $test_name != lightning_* ]]; then
		continue
	fi
	# shellcheck disable=SC2076
	if [[ ! " ${groups[*]} " =~ " ${test_name} " ]]; then
		others=("${others[@]} ${test_name}")
	fi
done

if [[ "$group" == "others" ]]; then
	if [[ -z $others ]]; then
		echo "All lightning test cases have been added to groups"
		exit 0
	fi
	echo "Error: "$others" is not added to any group in br/tests/run_group_lightning_tests.sh"
	exit 1
elif [[ " ${!groups[*]} " =~ " ${group} " ]]; then
	test_names="${groups[${group}]}"
	# Run test cases
	if [[ -n $test_names ]]; then
		echo ""
		echo "Run cases: ${test_names}"
        for case_name in $test_names; do
            echo "Run cases: ${case_name}"
            rm -rf /tmp/lightning_test
            mkdir -p /tmp/lightning_test
            TEST_NAME=${case_name} ${CUR}/run.sh
        done
	fi
else
	echo "Error: invalid group name: ${group}"
	exit 1
fi
