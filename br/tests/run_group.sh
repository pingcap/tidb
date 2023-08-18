#!/usr/bin/env bash

# This script split the integration tests into 16 groups to support parallel group tests execution.
# all the integration tests are located in br/tests directory. only the directories
# containing run.sh will be considered as integration tests. the script will print the total # # # number

set -eo pipefail

# Step 1
CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
group=$1
export COV_DIR="/tmp/group_cover"
rm -rf COV_DIR
mkdir $COV_DIR

# Define groups
# Note: If new group is added, the group name must also be added to CI
# * https://github.com/PingCAP-QE/ci/blob/main/pipelines/pingcap/tidb/latest/pull_br_integration_test.groovy
# Each group of tests consumes as much time as possible, thus reducing CI waiting time.
# Putting multiple light tests together and heavy tests in a separate group.
declare -A groups
groups=(
	["G00"]="br_300_small_tables br_backup_empty br_backup_version br_cache_table br_case_sensitive br_charset_gbk br_check_new_collocation_enable"
	["G01"]="br_autoid br_crypter2 br_db br_db_online br_db_online_newkv br_db_skip br_debug_meta br_ebs br_foreign_key br_full"
	["G02"]="br_full_cluster_restore br_full_ddl br_full_index br_gcs br_history"
    ["G03"]='br_incompatible_tidb_config br_incremental br_incremental_ddl br_incremental_index'
	["G04"]='br_incremental_only_ddl br_incremental_same_table br_insert_after_restore br_key_locked br_log_test br_move_backup br_mv_index br_other br_partition_add_index'
	["G05"]='br_range br_rawkv br_replica_read br_restore_TDE_enable br_restore_log_task_enable br_s3 br_shuffle_leader br_shuffle_region br_single_table'
	["G06"]='br_skip_checksum br_small_batch_size br_split_region_fail br_systables br_table_filter br_txn'
	["G07"]='br_clustered_index br_crypter br_table_partition br_tidb_placement_policy br_tiflash br_tikv_outage'
	["G08"]='br_tikv_outage2 br_ttl br_views_and_sequences br_z_gc_safepoint lightning_add_index lightning_alter_random lightning_auto_columns'
	["G09"]='lightning_auto_random_default lightning_bom_file lightning_character_sets lightning_check_partial_imported lightning_checkpoint lightning_checkpoint_chunks lightning_checkpoint_columns lightning_checkpoint_dirty_tableid'
	["G10"]='lightning_checkpoint_engines lightning_checkpoint_engines_order lightning_checkpoint_error_destroy lightning_checkpoint_parquet lightning_checkpoint_timestamp lightning_checksum_mismatch lightning_cmdline_override lightning_column_permutation lightning_common_handle'
	["G11"]='lightning_compress lightning_concurrent-restore lightning_config_max_error lightning_config_skip_csv_header lightning_csv lightning_default-columns lightning_disable_scheduler_by_key_range lightning_disk_quota lightning_distributed_import'
	["G12"]='lightning_drop_other_tables_halfway lightning_duplicate_detection lightning_duplicate_detection_new lightning_duplicate_resolution lightning_duplicate_resolution_incremental lightning_error_summary lightning_examples lightning_exotic_filenames lightning_extend_routes lightning_fail_fast'
	["G13"]='lightning_fail_fast_on_nonretry_err lightning_file_routing lightning_foreign_key lightning_gcs lightning_generated_columns lightning_ignore_columns lightning_import_compress lightning_incremental lightning_issue_282'
	["G14"]='lightning_issue_40657 lightning_issue_410 lightning_issue_519 lightning_local_backend lightning_max_incr lightning_max_random lightning_multi_valued_index lightning_new_collation lightning_no_schema'
	["G15"]='lightning_parquet lightning_partition_incremental lightning_partitioned-table lightning_record_network lightning_reload_cert lightning_restore lightning_routes lightning_routes_panic lightning_row-format-v2 lightning_s3'
	["G16"]='lightning_shard_rowid lightning_source_linkfile lightning_sqlmode lightning_tidb_duplicate_data lightning_tidb_rowid lightning_tiflash lightning_tikv_multi_rocksdb lightning_too_many_columns lightning_tool_135'
	["G17"]='lightning_tool_1420 lightning_tool_1472 lightning_tool_241 lightning_ttl lightning_unused_config_keys lightning_various_types lightning_view lightning_write_batch lightning_write_limit'
)

# Get other cases not in groups, to avoid missing any case
others=()
for script in "$CUR"/*/run.sh; do
	test_name="$(basename "$(dirname "$script")")"
	# shellcheck disable=SC2076
	if [[ ! " ${groups[*]} " =~ " ${test_name} " ]]; then
		others=("${others[@]} ${test_name}")
	fi
done

if [[ "$group" == "others" ]]; then
	if [[ -z $others ]]; then
		echo "All br&lightning integration test cases have been added to groups"
		exit 0
	fi
	echo "Error: "$others" is not added to any group in br/tests/run_group.sh"
	exit 1
elif [[ " ${!groups[*]} " =~ " ${group} " ]]; then
	test_names="${groups[${group}]}"
	# Run test cases
	if [[ -n $test_names ]]; then
		echo ""
		echo "Run cases: ${test_names}"
        for case_name in $test_names; do
            echo "Run cases: ${case_name}"
            rm -rf /tmp/backup_restore_test
            mkdir -p /tmp/backup_restore_test
            TEST_NAME=${case_name} ${CUR}/run.sh
        done
	fi
else
	echo "Error: invalid group name: ${group}"
	exit 1
fi
