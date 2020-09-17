// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/versioninfo"
)

// ScopeFlag is for system variable whether can be changed in global/session dynamically or not.
type ScopeFlag uint8

const (
	// ScopeNone means the system variable can not be changed dynamically.
	ScopeNone ScopeFlag = 0
	// ScopeGlobal means the system variable can be changed globally.
	ScopeGlobal ScopeFlag = 1 << 0
	// ScopeSession means the system variable can only be changed in current session.
	ScopeSession ScopeFlag = 1 << 1
)

// SysVar is for system variable.
type SysVar struct {
	// Scope is for whether can be changed or not
	Scope ScopeFlag

	// Name is the variable name.
	Name string

	// Value is the variable value.
	Value string

	// SetVarHintApply indicate whether variable is hintable
	SetVarHintApply bool
}

// SysVars is global sys vars map.
var SysVars map[string]*SysVar

// GetSysVar returns sys var info for name as key.
func GetSysVar(name string) *SysVar {
	name = strings.ToLower(name)
	return SysVars[name]
}

// PluginVarNames is global plugin var names set.
var PluginVarNames []string

func init() {
	SysVars = make(map[string]*SysVar)
	for _, v := range defaultSysVars {
		SysVars[v.Name] = v
	}
	initSynonymsSysVariables()
}

// BoolToIntStr converts bool to int string, for example "0" or "1".
func BoolToIntStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func boolToOnOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// BoolToInt32 converts bool to int32
func BoolToInt32(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

// we only support MySQL now
var defaultSysVars = []*SysVar{
	{ScopeGlobal, "gtid_mode", "OFF", false},
	{ScopeGlobal, FlushTime, "0", false},
	{ScopeNone, "performance_schema_max_mutex_classes", "200", false},
	{ScopeGlobal | ScopeSession, LowPriorityUpdates, "0", false},
	{ScopeGlobal | ScopeSession, SessionTrackGtids, "OFF", false},
	{ScopeGlobal | ScopeSession, "ndbinfo_max_rows", "", false},
	{ScopeGlobal | ScopeSession, "ndb_index_stat_option", "", false},
	{ScopeGlobal | ScopeSession, OldPasswords, "0", false},
	{ScopeNone, "innodb_version", "5.6.25", false},
	{ScopeGlobal, MaxConnections, "151", false},
	{ScopeGlobal | ScopeSession, BigTables, "0", false},
	{ScopeNone, "skip_external_locking", "1", false},
	{ScopeNone, "innodb_sync_array_size", "1", false},
	{ScopeSession, "rand_seed2", "", false},
	{ScopeGlobal, ValidatePasswordCheckUserName, "0", false},
	{ScopeGlobal, "validate_password_number_count", "1", false},
	{ScopeSession, "gtid_next", "", false},
	{ScopeGlobal | ScopeSession, SQLSelectLimit, "18446744073709551615", true},
	{ScopeGlobal, "ndb_show_foreign_key_mock_tables", "", false},
	{ScopeNone, "multi_range_count", "256", false},
	{ScopeGlobal | ScopeSession, DefaultWeekFormat, "0", false},
	{ScopeGlobal | ScopeSession, "binlog_error_action", "IGNORE_ERROR", false},
	{ScopeGlobal | ScopeSession, "default_storage_engine", "InnoDB", false},
	{ScopeNone, "ft_query_expansion_limit", "20", false},
	{ScopeGlobal, MaxConnectErrors, "100", false},
	{ScopeGlobal, SyncBinlog, "0", false},
	{ScopeNone, "max_digest_length", "1024", false},
	{ScopeNone, "innodb_force_load_corrupted", "0", false},
	{ScopeNone, "performance_schema_max_table_handles", "4000", false},
	{ScopeGlobal, InnodbFastShutdown, "1", false},
	{ScopeNone, "ft_max_word_len", "84", false},
	{ScopeGlobal, "log_backward_compatible_user_definitions", "", false},
	{ScopeNone, "lc_messages_dir", "/usr/local/mysql-5.6.25-osx10.8-x86_64/share/", false},
	{ScopeGlobal, "ft_boolean_syntax", "+ -><()~*:\"\"&|", false},
	{ScopeGlobal, TableDefinitionCache, "-1", false},
	{ScopeNone, SkipNameResolve, "0", false},
	{ScopeNone, "performance_schema_max_file_handles", "32768", false},
	{ScopeSession, "transaction_allow_batching", "", false},
	{ScopeGlobal | ScopeSession, SQLModeVar, mysql.DefaultSQLMode, true},
	{ScopeNone, "performance_schema_max_statement_classes", "168", false},
	{ScopeGlobal, "server_id", "0", false},
	{ScopeGlobal, "innodb_flushing_avg_loops", "30", false},
	{ScopeGlobal | ScopeSession, TmpTableSize, "16777216", true},
	{ScopeGlobal, "innodb_max_purge_lag", "0", false},
	{ScopeGlobal | ScopeSession, "preload_buffer_size", "32768", false},
	{ScopeGlobal, CheckProxyUsers, "0", false},
	{ScopeNone, "have_query_cache", "YES", false},
	{ScopeGlobal, "innodb_flush_log_at_timeout", "1", false},
	{ScopeGlobal, "innodb_max_undo_log_size", "", false},
	{ScopeGlobal | ScopeSession, "range_alloc_block_size", "4096", true},
	{ScopeGlobal, ConnectTimeout, "10", false},
	{ScopeGlobal | ScopeSession, MaxExecutionTime, "0", true},
	{ScopeGlobal | ScopeSession, CollationServer, mysql.DefaultCollationName, false},
	{ScopeNone, "have_rtree_keys", "YES", false},
	{ScopeGlobal, "innodb_old_blocks_pct", "37", false},
	{ScopeGlobal, "innodb_file_format", "Antelope", false},
	{ScopeGlobal, "innodb_compression_failure_threshold_pct", "5", false},
	{ScopeNone, "performance_schema_events_waits_history_long_size", "10000", false},
	{ScopeGlobal, "innodb_checksum_algorithm", "innodb", false},
	{ScopeNone, "innodb_ft_sort_pll_degree", "2", false},
	{ScopeNone, "thread_stack", "262144", false},
	{ScopeGlobal, "relay_log_info_repository", "FILE", false},
	{ScopeGlobal | ScopeSession, SQLLogBin, "1", false},
	{ScopeGlobal, SuperReadOnly, "0", false},
	{ScopeGlobal | ScopeSession, "max_delayed_threads", "20", false},
	{ScopeNone, "protocol_version", "10", false},
	{ScopeGlobal | ScopeSession, "new", "OFF", false},
	{ScopeGlobal | ScopeSession, "myisam_sort_buffer_size", "8388608", false},
	{ScopeGlobal | ScopeSession, "optimizer_trace_offset", "-1", false},
	{ScopeGlobal, InnodbBufferPoolDumpAtShutdown, "0", false},
	{ScopeGlobal | ScopeSession, SQLNotes, "1", false},
	{ScopeGlobal, InnodbCmpPerIndexEnabled, "0", false},
	{ScopeGlobal, "innodb_ft_server_stopword_table", "", false},
	{ScopeNone, "performance_schema_max_file_instances", "7693", false},
	{ScopeNone, "log_output", "FILE", false},
	{ScopeGlobal, "binlog_group_commit_sync_delay", "", false},
	{ScopeGlobal, "binlog_group_commit_sync_no_delay_count", "", false},
	{ScopeNone, "have_crypt", "YES", false},
	{ScopeGlobal, "innodb_log_write_ahead_size", "", false},
	{ScopeNone, "innodb_log_group_home_dir", "./", false},
	{ScopeNone, "performance_schema_events_statements_history_size", "10", false},
	{ScopeGlobal, GeneralLog, "0", false},
	{ScopeGlobal, "validate_password_dictionary_file", "", false},
	{ScopeGlobal, BinlogOrderCommits, "1", false},
	{ScopeGlobal, "key_cache_division_limit", "100", false},
	{ScopeGlobal | ScopeSession, "max_insert_delayed_threads", "20", false},
	{ScopeNone, "performance_schema_session_connect_attrs_size", "512", false},
	{ScopeGlobal | ScopeSession, "time_zone", "SYSTEM", true},
	{ScopeGlobal, "innodb_max_dirty_pages_pct", "75", false},
	{ScopeGlobal, InnodbFilePerTable, "1", false},
	{ScopeGlobal, InnodbLogCompressedPages, "1", false},
	{ScopeNone, "skip_networking", "0", false},
	{ScopeGlobal, "innodb_monitor_reset", "", false},
	{ScopeNone, "have_ssl", "DISABLED", false},
	{ScopeNone, "have_openssl", "DISABLED", false},
	{ScopeNone, "ssl_ca", "", false},
	{ScopeNone, "ssl_cert", "", false},
	{ScopeNone, "ssl_key", "", false},
	{ScopeNone, "ssl_cipher", "", false},
	{ScopeNone, "tls_version", "TLSv1,TLSv1.1,TLSv1.2", false},
	{ScopeNone, "system_time_zone", "CST", false},
	{ScopeGlobal, InnodbPrintAllDeadlocks, "0", false},
	{ScopeNone, "innodb_autoinc_lock_mode", "1", false},
	{ScopeGlobal, "key_buffer_size", "8388608", false},
	{ScopeGlobal | ScopeSession, ForeignKeyChecks, "OFF", true},
	{ScopeGlobal, "host_cache_size", "279", false},
	{ScopeGlobal, DelayKeyWrite, "ON", false},
	{ScopeNone, "metadata_locks_cache_size", "1024", false},
	{ScopeNone, "innodb_force_recovery", "0", false},
	{ScopeGlobal, "innodb_file_format_max", "Antelope", false},
	{ScopeGlobal | ScopeSession, "debug", "", false},
	{ScopeGlobal, "log_warnings", "1", false},
	{ScopeGlobal, OfflineMode, "0", false},
	{ScopeGlobal | ScopeSession, InnodbStrictMode, "1", false},
	{ScopeGlobal, "innodb_rollback_segments", "128", false},
	{ScopeGlobal | ScopeSession, "join_buffer_size", "262144", true},
	{ScopeNone, "innodb_mirrored_log_groups", "1", false},
	{ScopeGlobal, "max_binlog_size", "1073741824", false},
	{ScopeGlobal, "concurrent_insert", "AUTO", false},
	{ScopeGlobal, InnodbAdaptiveHashIndex, "1", false},
	{ScopeGlobal, InnodbFtEnableStopword, "1", false},
	{ScopeGlobal, "general_log_file", "/usr/local/mysql/data/localhost.log", false},
	{ScopeGlobal | ScopeSession, InnodbSupportXA, "1", false},
	{ScopeGlobal, "innodb_compression_level", "6", false},
	{ScopeNone, "innodb_file_format_check", "1", false},
	{ScopeNone, "myisam_mmap_size", "18446744073709551615", false},
	{ScopeNone, "innodb_buffer_pool_instances", "8", false},
	{ScopeGlobal | ScopeSession, BlockEncryptionMode, "aes-128-ecb", false},
	{ScopeGlobal | ScopeSession, "max_length_for_sort_data", "1024", true},
	{ScopeNone, "character_set_system", "utf8", false},
	{ScopeGlobal | ScopeSession, InteractiveTimeout, "28800", false},
	{ScopeGlobal, InnodbOptimizeFullTextOnly, "0", false},
	{ScopeNone, "character_sets_dir", "/usr/local/mysql-5.6.25-osx10.8-x86_64/share/charsets/", false},
	{ScopeGlobal | ScopeSession, QueryCacheType, "OFF", false},
	{ScopeNone, "innodb_rollback_on_timeout", "0", false},
	{ScopeGlobal | ScopeSession, "query_alloc_block_size", "8192", false},
	{ScopeGlobal | ScopeSession, InitConnect, "", false},
	{ScopeNone, "have_compress", "YES", false},
	{ScopeNone, "thread_concurrency", "10", false},
	{ScopeGlobal | ScopeSession, "query_prealloc_size", "8192", false},
	{ScopeNone, "relay_log_space_limit", "0", false},
	{ScopeGlobal | ScopeSession, MaxUserConnections, "0", false},
	{ScopeNone, "performance_schema_max_thread_classes", "50", false},
	{ScopeGlobal, "innodb_api_trx_level", "0", false},
	{ScopeNone, "disconnect_on_expired_password", "1", false},
	{ScopeNone, "performance_schema_max_file_classes", "50", false},
	{ScopeGlobal, "expire_logs_days", "0", false},
	{ScopeGlobal | ScopeSession, BinlogRowQueryLogEvents, "0", false},
	{ScopeGlobal, "default_password_lifetime", "", false},
	{ScopeNone, "pid_file", "/usr/local/mysql/data/localhost.pid", false},
	{ScopeNone, "innodb_undo_tablespaces", "0", false},
	{ScopeGlobal, InnodbStatusOutputLocks, "0", false},
	{ScopeNone, "performance_schema_accounts_size", "100", false},
	{ScopeGlobal | ScopeSession, "max_error_count", "64", true},
	{ScopeGlobal, "max_write_lock_count", "18446744073709551615", false},
	{ScopeNone, "performance_schema_max_socket_instances", "322", false},
	{ScopeNone, "performance_schema_max_table_instances", "12500", false},
	{ScopeGlobal, "innodb_stats_persistent_sample_pages", "20", false},
	{ScopeGlobal, "show_compatibility_56", "", false},
	{ScopeNone, "innodb_open_files", "2000", false},
	{ScopeGlobal, "innodb_spin_wait_delay", "6", false},
	{ScopeGlobal, "thread_cache_size", "9", false},
	{ScopeGlobal, LogSlowAdminStatements, "0", false},
	{ScopeNone, "innodb_checksums", "ON", false},
	{ScopeNone, "hostname", ServerHostname, false},
	{ScopeGlobal | ScopeSession, "auto_increment_offset", "1", false},
	{ScopeNone, "ft_stopword_file", "(built-in)", false},
	{ScopeGlobal, "innodb_max_dirty_pages_pct_lwm", "0", false},
	{ScopeGlobal, LogQueriesNotUsingIndexes, "0", false},
	{ScopeSession, "timestamp", "", true},
	{ScopeGlobal | ScopeSession, QueryCacheWlockInvalidate, "0", false},
	{ScopeGlobal | ScopeSession, "sql_buffer_result", "OFF", true},
	{ScopeGlobal | ScopeSession, "character_set_filesystem", "binary", false},
	{ScopeGlobal | ScopeSession, "collation_database", mysql.DefaultCollationName, false},
	{ScopeGlobal | ScopeSession, AutoIncrementIncrement, strconv.FormatInt(DefAutoIncrementIncrement, 10), false},
	{ScopeGlobal | ScopeSession, AutoIncrementOffset, strconv.FormatInt(DefAutoIncrementOffset, 10), false},
	{ScopeGlobal | ScopeSession, "max_heap_table_size", "16777216", true},
	{ScopeGlobal | ScopeSession, "div_precision_increment", "4", true},
	{ScopeGlobal, "innodb_lru_scan_depth", "1024", false},
	{ScopeGlobal, "innodb_purge_rseg_truncate_frequency", "", false},
	{ScopeGlobal | ScopeSession, SQLAutoIsNull, "0", true},
	{ScopeNone, "innodb_api_enable_binlog", "0", false},
	{ScopeGlobal | ScopeSession, "innodb_ft_user_stopword_table", "", false},
	{ScopeNone, "server_id_bits", "32", false},
	{ScopeGlobal, "innodb_log_checksum_algorithm", "", false},
	{ScopeNone, "innodb_buffer_pool_load_at_startup", "1", false},
	{ScopeGlobal | ScopeSession, "sort_buffer_size", "262144", true},
	{ScopeGlobal, "innodb_flush_neighbors", "1", false},
	{ScopeNone, "innodb_use_sys_malloc", "1", false},
	{ScopeSession, PluginLoad, "", false},
	{ScopeSession, PluginDir, "/data/deploy/plugin", false},
	{ScopeNone, "performance_schema_max_socket_classes", "10", false},
	{ScopeNone, "performance_schema_max_stage_classes", "150", false},
	{ScopeGlobal, "innodb_purge_batch_size", "300", false},
	{ScopeNone, "have_profiling", "NO", false},
	{ScopeGlobal | ScopeSession, "character_set_client", mysql.DefaultCharset, false},
	{ScopeGlobal, InnodbBufferPoolDumpNow, "0", false},
	{ScopeGlobal, RelayLogPurge, "1", false},
	{ScopeGlobal, "ndb_distribution", "", false},
	{ScopeGlobal, "myisam_data_pointer_size", "6", false},
	{ScopeGlobal, "ndb_optimization_delay", "", false},
	{ScopeGlobal, "innodb_ft_num_word_optimize", "2000", false},
	{ScopeGlobal | ScopeSession, "max_join_size", "18446744073709551615", true},
	{ScopeNone, CoreFile, "0", false},
	{ScopeGlobal | ScopeSession, "max_seeks_for_key", "18446744073709551615", true},
	{ScopeNone, "innodb_log_buffer_size", "8388608", false},
	{ScopeGlobal, "delayed_insert_timeout", "300", false},
	{ScopeGlobal, "max_relay_log_size", "0", false},
	{ScopeGlobal | ScopeSession, MaxSortLength, "1024", true},
	{ScopeNone, "metadata_locks_hash_instances", "8", false},
	{ScopeGlobal, "ndb_eventbuffer_free_percent", "", false},
	{ScopeNone, "large_files_support", "1", false},
	{ScopeGlobal, "binlog_max_flush_queue_time", "0", false},
	{ScopeGlobal, "innodb_fill_factor", "", false},
	{ScopeGlobal, "log_syslog_facility", "", false},
	{ScopeNone, "innodb_ft_min_token_size", "3", false},
	{ScopeGlobal | ScopeSession, "transaction_write_set_extraction", "", false},
	{ScopeGlobal | ScopeSession, "ndb_blob_write_batch_bytes", "", false},
	{ScopeGlobal, "automatic_sp_privileges", "1", false},
	{ScopeGlobal, "innodb_flush_sync", "", false},
	{ScopeNone, "performance_schema_events_statements_history_long_size", "10000", false},
	{ScopeGlobal, "innodb_monitor_disable", "", false},
	{ScopeNone, "innodb_doublewrite", "1", false},
	{ScopeNone, "log_bin_use_v1_row_events", "0", false},
	{ScopeSession, "innodb_optimize_point_storage", "", false},
	{ScopeNone, "innodb_api_disable_rowlock", "0", false},
	{ScopeGlobal, "innodb_adaptive_flushing_lwm", "10", false},
	{ScopeNone, "innodb_log_files_in_group", "2", false},
	{ScopeGlobal, InnodbBufferPoolLoadNow, "0", false},
	{ScopeNone, "performance_schema_max_rwlock_classes", "40", false},
	{ScopeNone, "binlog_gtid_simple_recovery", "1", false},
	{ScopeNone, Port, "4000", false},
	{ScopeNone, "performance_schema_digests_size", "10000", false},
	{ScopeGlobal | ScopeSession, Profiling, "0", false},
	{ScopeNone, "lower_case_table_names", "2", false},
	{ScopeSession, "rand_seed1", "", false},
	{ScopeGlobal, "sha256_password_proxy_users", "", false},
	{ScopeGlobal | ScopeSession, SQLQuoteShowCreate, "1", false},
	{ScopeGlobal | ScopeSession, "binlogging_impossible_mode", "IGNORE_ERROR", false},
	{ScopeGlobal | ScopeSession, QueryCacheSize, "1048576", false},
	{ScopeGlobal, "innodb_stats_transient_sample_pages", "8", false},
	{ScopeGlobal, InnodbStatsOnMetadata, "0", false},
	{ScopeNone, "server_uuid", "00000000-0000-0000-0000-000000000000", false},
	{ScopeNone, "open_files_limit", "5000", false},
	{ScopeGlobal | ScopeSession, "ndb_force_send", "", false},
	{ScopeNone, "skip_show_database", "0", false},
	{ScopeGlobal, "log_timestamps", "", false},
	{ScopeNone, "version_compile_machine", "x86_64", false},
	{ScopeGlobal, "event_scheduler", "OFF", false},
	{ScopeGlobal | ScopeSession, "ndb_deferred_constraints", "", false},
	{ScopeGlobal, "log_syslog_include_pid", "", false},
	{ScopeSession, "last_insert_id", "", false},
	{ScopeNone, "innodb_ft_cache_size", "8000000", false},
	{ScopeNone, LogBin, "0", false},
	{ScopeGlobal, InnodbDisableSortFileCache, "0", false},
	{ScopeGlobal, "log_error_verbosity", "", false},
	{ScopeNone, "performance_schema_hosts_size", "100", false},
	{ScopeGlobal, "innodb_replication_delay", "0", false},
	{ScopeGlobal, SlowQueryLog, "0", false},
	{ScopeSession, "debug_sync", "", false},
	{ScopeGlobal, InnodbStatsAutoRecalc, "1", false},
	{ScopeGlobal | ScopeSession, "lc_messages", "en_US", false},
	{ScopeGlobal | ScopeSession, "bulk_insert_buffer_size", "8388608", true},
	{ScopeGlobal | ScopeSession, BinlogDirectNonTransactionalUpdates, "0", false},
	{ScopeGlobal, "innodb_change_buffering", "all", false},
	{ScopeGlobal | ScopeSession, SQLBigSelects, "1", true},
	{ScopeGlobal | ScopeSession, CharacterSetResults, mysql.DefaultCharset, false},
	{ScopeGlobal, "innodb_max_purge_lag_delay", "0", false},
	{ScopeGlobal | ScopeSession, "session_track_schema", "", false},
	{ScopeGlobal, "innodb_io_capacity_max", "2000", false},
	{ScopeGlobal, "innodb_autoextend_increment", "64", false},
	{ScopeGlobal | ScopeSession, "binlog_format", "STATEMENT", false},
	{ScopeGlobal | ScopeSession, "optimizer_trace", "enabled=off,one_line=off", false},
	{ScopeGlobal | ScopeSession, "read_rnd_buffer_size", "262144", true},
	{ScopeNone, "version_comment", "TiDB Server (Apache License 2.0) " + versioninfo.TiDBEdition + " Edition, MySQL 5.7 compatible", false},
	{ScopeGlobal | ScopeSession, NetWriteTimeout, "60", false},
	{ScopeGlobal, InnodbBufferPoolLoadAbort, "0", false},
	{ScopeGlobal | ScopeSession, TxnIsolation, "REPEATABLE-READ", false},
	{ScopeGlobal | ScopeSession, TransactionIsolation, "REPEATABLE-READ", false},
	{ScopeGlobal | ScopeSession, "collation_connection", mysql.DefaultCollationName, false},
	{ScopeGlobal | ScopeSession, "transaction_prealloc_size", "4096", false},
	{ScopeNone, "performance_schema_setup_objects_size", "100", false},
	{ScopeGlobal, "sync_relay_log", "10000", false},
	{ScopeGlobal, "innodb_ft_result_cache_limit", "2000000000", false},
	{ScopeNone, "innodb_sort_buffer_size", "1048576", false},
	{ScopeGlobal, "innodb_ft_enable_diag_print", "OFF", false},
	{ScopeNone, "thread_handling", "one-thread-per-connection", false},
	{ScopeGlobal, "stored_program_cache", "256", false},
	{ScopeNone, "performance_schema_max_mutex_instances", "15906", false},
	{ScopeGlobal, "innodb_adaptive_max_sleep_delay", "150000", false},
	{ScopeNone, "large_pages", "OFF", false},
	{ScopeGlobal | ScopeSession, "session_track_system_variables", "", false},
	{ScopeGlobal, "innodb_change_buffer_max_size", "25", false},
	{ScopeGlobal, LogBinTrustFunctionCreators, "0", false},
	{ScopeNone, "innodb_write_io_threads", "4", false},
	{ScopeGlobal, "mysql_native_password_proxy_users", "", false},
	{ScopeGlobal, serverReadOnly, "0", false},
	{ScopeNone, "large_page_size", "0", false},
	{ScopeNone, "table_open_cache_instances", "1", false},
	{ScopeGlobal, InnodbStatsPersistent, "1", false},
	{ScopeGlobal | ScopeSession, "session_track_state_change", "", false},
	{ScopeNone, "optimizer_switch", "index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,subquery_materialization_cost_based=on,use_index_extensions=on", true},
	{ScopeGlobal, "delayed_queue_size", "1000", false},
	{ScopeNone, "innodb_read_only", "0", false},
	{ScopeNone, "datetime_format", "%Y-%m-%d %H:%i:%s", false},
	{ScopeGlobal, "log_syslog", "", false},
	{ScopeNone, "version", mysql.ServerVersion, false},
	{ScopeGlobal | ScopeSession, "transaction_alloc_block_size", "8192", false},
	{ScopeGlobal, "innodb_large_prefix", "OFF", false},
	{ScopeNone, "performance_schema_max_cond_classes", "80", false},
	{ScopeGlobal, "innodb_io_capacity", "200", false},
	{ScopeGlobal, "max_binlog_cache_size", "18446744073709547520", false},
	{ScopeGlobal | ScopeSession, "ndb_index_stat_enable", "", false},
	{ScopeGlobal, "executed_gtids_compression_period", "", false},
	{ScopeNone, "time_format", "%H:%i:%s", false},
	{ScopeGlobal | ScopeSession, OldAlterTable, "0", false},
	{ScopeGlobal | ScopeSession, "long_query_time", "10.000000", false},
	{ScopeNone, "innodb_use_native_aio", "0", false},
	{ScopeGlobal, "log_throttle_queries_not_using_indexes", "0", false},
	{ScopeNone, "locked_in_memory", "0", false},
	{ScopeNone, "innodb_api_enable_mdl", "0", false},
	{ScopeGlobal, "binlog_cache_size", "32768", false},
	{ScopeGlobal, "innodb_compression_pad_pct_max", "50", false},
	{ScopeGlobal, InnodbCommitConcurrency, "0", false},
	{ScopeNone, "ft_min_word_len", "4", false},
	{ScopeGlobal, EnforceGtidConsistency, "OFF", false},
	{ScopeGlobal, SecureAuth, "1", false},
	{ScopeNone, "max_tmp_tables", "32", false},
	{ScopeGlobal, InnodbRandomReadAhead, "0", false},
	{ScopeGlobal | ScopeSession, UniqueChecks, "1", true},
	{ScopeGlobal, "internal_tmp_disk_storage_engine", "", false},
	{ScopeGlobal | ScopeSession, "myisam_repair_threads", "1", false},
	{ScopeGlobal, "ndb_eventbuffer_max_alloc", "", false},
	{ScopeGlobal, "innodb_read_ahead_threshold", "56", false},
	{ScopeGlobal, "key_cache_block_size", "1024", false},
	{ScopeNone, "ndb_recv_thread_cpu_mask", "", false},
	{ScopeGlobal, "gtid_purged", "", false},
	{ScopeGlobal, "max_binlog_stmt_cache_size", "18446744073709547520", false},
	{ScopeGlobal | ScopeSession, "lock_wait_timeout", "31536000", true},
	{ScopeGlobal | ScopeSession, "read_buffer_size", "131072", true},
	{ScopeNone, "innodb_read_io_threads", "4", false},
	{ScopeGlobal | ScopeSession, MaxSpRecursionDepth, "0", false},
	{ScopeNone, "ignore_builtin_innodb", "0", false},
	{ScopeGlobal, "slow_query_log_file", "/usr/local/mysql/data/localhost-slow.log", false},
	{ScopeGlobal, "innodb_thread_sleep_delay", "10000", false},
	{ScopeNone, "license", "Apache License 2.0", false},
	{ScopeGlobal, "innodb_ft_aux_table", "", false},
	{ScopeGlobal | ScopeSession, SQLWarnings, "0", false},
	{ScopeGlobal | ScopeSession, KeepFilesOnCreate, "0", false},
	{ScopeNone, "innodb_data_file_path", "ibdata1:12M:autoextend", false},
	{ScopeNone, "performance_schema_setup_actors_size", "100", false},
	{ScopeNone, "innodb_additional_mem_pool_size", "8388608", false},
	{ScopeNone, "log_error", "/usr/local/mysql/data/localhost.err", false},
	{ScopeGlobal, "binlog_stmt_cache_size", "32768", false},
	{ScopeNone, "relay_log_info_file", "relay-log.info", false},
	{ScopeNone, "innodb_ft_total_cache_size", "640000000", false},
	{ScopeNone, "performance_schema_max_rwlock_instances", "9102", false},
	{ScopeGlobal, "table_open_cache", "2000", false},
	{ScopeNone, "performance_schema_events_stages_history_long_size", "10000", false},
	{ScopeGlobal | ScopeSession, AutoCommit, "1", false},
	{ScopeSession, "insert_id", "", false},
	{ScopeGlobal | ScopeSession, "default_tmp_storage_engine", "InnoDB", true},
	{ScopeGlobal | ScopeSession, "optimizer_search_depth", "62", true},
	{ScopeGlobal | ScopeSession, "max_points_in_geometry", "", true},
	{ScopeGlobal, "innodb_stats_sample_pages", "8", false},
	{ScopeGlobal | ScopeSession, "profiling_history_size", "15", false},
	{ScopeGlobal | ScopeSession, "character_set_database", mysql.DefaultCharset, false},
	{ScopeNone, "have_symlink", "YES", false},
	{ScopeGlobal | ScopeSession, "storage_engine", "InnoDB", false},
	{ScopeGlobal | ScopeSession, "sql_log_off", "0", false},
	// In MySQL, the default value of `explicit_defaults_for_timestamp` is `0`.
	// But In TiDB, it's set to `1` to be consistent with TiDB timestamp behavior.
	// See: https://github.com/pingcap/tidb/pull/6068 for details
	{ScopeNone, "explicit_defaults_for_timestamp", "1", false},
	{ScopeNone, "performance_schema_events_waits_history_size", "10", false},
	{ScopeGlobal, "log_syslog_tag", "", false},
	{ScopeGlobal | ScopeSession, TxReadOnly, "0", false},
	{ScopeGlobal | ScopeSession, TransactionReadOnly, "0", false},
	{ScopeGlobal, "innodb_undo_log_truncate", "", false},
	{ScopeSession, "innodb_create_intrinsic", "", false},
	{ScopeGlobal, "gtid_executed_compression_period", "", false},
	{ScopeGlobal, "ndb_log_empty_epochs", "", false},
	{ScopeGlobal, MaxPreparedStmtCount, strconv.FormatInt(DefMaxPreparedStmtCount, 10), false},
	{ScopeNone, "have_geometry", "YES", false},
	{ScopeGlobal | ScopeSession, "optimizer_trace_max_mem_size", "16384", true},
	{ScopeGlobal | ScopeSession, "net_retry_count", "10", false},
	{ScopeSession, "ndb_table_no_logging", "", false},
	{ScopeGlobal | ScopeSession, "optimizer_trace_features", "greedy_search=on,range_optimizer=on,dynamic_range=on,repeated_subselect=on", false},
	{ScopeGlobal, "innodb_flush_log_at_trx_commit", "1", false},
	{ScopeGlobal, "rewriter_enabled", "", false},
	{ScopeGlobal, "query_cache_min_res_unit", "4096", false},
	{ScopeGlobal | ScopeSession, "updatable_views_with_limit", "YES", true},
	{ScopeGlobal | ScopeSession, "optimizer_prune_level", "1", true},
	{ScopeGlobal | ScopeSession, "completion_type", "NO_CHAIN", false},
	{ScopeGlobal, "binlog_checksum", "CRC32", false},
	{ScopeNone, "report_port", "3306", false},
	{ScopeGlobal | ScopeSession, ShowOldTemporals, "0", false},
	{ScopeGlobal, "query_cache_limit", "1048576", false},
	{ScopeGlobal, "innodb_buffer_pool_size", "134217728", false},
	{ScopeGlobal, InnodbAdaptiveFlushing, "1", false},
	{ScopeNone, "datadir", "/usr/local/mysql/data/", false},
	{ScopeGlobal | ScopeSession, WaitTimeout, strconv.FormatInt(DefWaitTimeout, 10), false},
	{ScopeGlobal, "innodb_monitor_enable", "", false},
	{ScopeNone, "date_format", "%Y-%m-%d", false},
	{ScopeGlobal, "innodb_buffer_pool_filename", "ib_buffer_pool", false},
	{ScopeGlobal, "slow_launch_time", "2", false},
	{ScopeGlobal | ScopeSession, "ndb_use_transactions", "", false},
	{ScopeNone, "innodb_purge_threads", "1", false},
	{ScopeGlobal, "innodb_concurrency_tickets", "5000", false},
	{ScopeGlobal, "innodb_monitor_reset_all", "", false},
	{ScopeNone, "performance_schema_users_size", "100", false},
	{ScopeGlobal, "ndb_log_updated_only", "", false},
	{ScopeNone, "basedir", "/usr/local/mysql", false},
	{ScopeGlobal, "innodb_old_blocks_time", "1000", false},
	{ScopeGlobal, "innodb_stats_method", "nulls_equal", false},
	{ScopeGlobal | ScopeSession, InnodbLockWaitTimeout, strconv.FormatInt(DefInnodbLockWaitTimeout, 10), false},
	{ScopeGlobal, LocalInFile, "1", false},
	{ScopeGlobal | ScopeSession, "myisam_stats_method", "nulls_unequal", false},
	{ScopeNone, "version_compile_os", "osx10.8", false},
	{ScopeNone, "relay_log_recovery", "0", false},
	{ScopeNone, "old", "0", false},
	{ScopeGlobal | ScopeSession, InnodbTableLocks, "1", false},
	{ScopeNone, PerformanceSchema, "0", false},
	{ScopeNone, "myisam_recover_options", "OFF", false},
	{ScopeGlobal | ScopeSession, NetBufferLength, "16384", false},
	{ScopeGlobal | ScopeSession, "binlog_row_image", "FULL", false},
	{ScopeNone, "innodb_locks_unsafe_for_binlog", "0", false},
	{ScopeSession, "rbr_exec_mode", "", false},
	{ScopeGlobal, "myisam_max_sort_file_size", "9223372036853727232", false},
	{ScopeNone, "back_log", "80", false},
	{ScopeNone, "lower_case_file_system", "1", false},
	{ScopeGlobal | ScopeSession, GroupConcatMaxLen, "1024", true},
	{ScopeSession, "pseudo_thread_id", "", false},
	{ScopeNone, "socket", "/tmp/myssock", false},
	{ScopeNone, "have_dynamic_loading", "YES", false},
	{ScopeGlobal, "rewriter_verbose", "", false},
	{ScopeGlobal, "innodb_undo_logs", "128", false},
	{ScopeNone, "performance_schema_max_cond_instances", "3504", false},
	{ScopeGlobal, "delayed_insert_limit", "100", false},
	{ScopeGlobal, Flush, "0", false},
	{ScopeGlobal | ScopeSession, "eq_range_index_dive_limit", "10", true},
	{ScopeNone, "performance_schema_events_stages_history_size", "10", false},
	{ScopeGlobal | ScopeSession, "character_set_connection", mysql.DefaultCharset, false},
	{ScopeGlobal, MyISAMUseMmap, "0", false},
	{ScopeGlobal | ScopeSession, "ndb_join_pushdown", "", false},
	{ScopeGlobal | ScopeSession, CharacterSetServer, mysql.DefaultCharset, false},
	{ScopeGlobal, "validate_password_special_char_count", "1", false},
	{ScopeNone, "performance_schema_max_thread_instances", "402", false},
	{ScopeGlobal | ScopeSession, "ndbinfo_show_hidden", "", false},
	{ScopeGlobal | ScopeSession, "net_read_timeout", "30", false},
	{ScopeNone, "innodb_page_size", "16384", false},
	{ScopeGlobal | ScopeSession, MaxAllowedPacket, "67108864", false},
	{ScopeNone, "innodb_log_file_size", "50331648", false},
	{ScopeGlobal, "sync_relay_log_info", "10000", false},
	{ScopeGlobal | ScopeSession, "optimizer_trace_limit", "1", false},
	{ScopeNone, "innodb_ft_max_token_size", "84", false},
	{ScopeGlobal, "validate_password_length", "8", false},
	{ScopeGlobal, "ndb_log_binlog_index", "", false},
	{ScopeGlobal, "innodb_api_bk_commit_interval", "5", false},
	{ScopeNone, "innodb_undo_directory", ".", false},
	{ScopeNone, "bind_address", "*", false},
	{ScopeGlobal, "innodb_sync_spin_loops", "30", false},
	{ScopeGlobal | ScopeSession, SQLSafeUpdates, "0", true},
	{ScopeNone, "tmpdir", "/var/tmp/", false},
	{ScopeGlobal, "innodb_thread_concurrency", "0", false},
	{ScopeGlobal, "innodb_buffer_pool_dump_pct", "", false},
	{ScopeGlobal | ScopeSession, "lc_time_names", "en_US", false},
	{ScopeGlobal | ScopeSession, "max_statement_time", "", false},
	{ScopeGlobal | ScopeSession, EndMakersInJSON, "0", true},
	{ScopeGlobal, AvoidTemporalUpgrade, "0", false},
	{ScopeGlobal, "key_cache_age_threshold", "300", false},
	{ScopeGlobal, InnodbStatusOutput, "0", false},
	{ScopeSession, "identity", "", false},
	{ScopeGlobal | ScopeSession, "min_examined_row_limit", "0", false},
	{ScopeGlobal, "sync_frm", "ON", false},
	{ScopeGlobal, "innodb_online_alter_log_max_size", "134217728", false},
	{ScopeSession, WarningCount, "0", false},
	{ScopeSession, ErrorCount, "0", false},
	{ScopeGlobal | ScopeSession, "information_schema_stats_expiry", "86400", false},
	{ScopeGlobal, "thread_pool_size", "16", false},
	{ScopeGlobal | ScopeSession, WindowingUseHighPrecision, "ON", true},
	/* TiDB specific variables */
	{ScopeSession, TiDBSnapshot, "", false},
	{ScopeSession, TiDBOptAggPushDown, BoolToIntStr(DefOptAggPushDown), false},
	{ScopeGlobal | ScopeSession, TiDBOptBCJ, BoolToIntStr(DefOptBCJ), false},
	{ScopeSession, TiDBOptDistinctAggPushDown, BoolToIntStr(config.GetGlobalConfig().Performance.DistinctAggPushDown), false},
	{ScopeSession, TiDBOptWriteRowID, BoolToIntStr(DefOptWriteRowID), false},
	{ScopeGlobal | ScopeSession, TiDBBuildStatsConcurrency, strconv.Itoa(DefBuildStatsConcurrency), false},
	{ScopeGlobal, TiDBAutoAnalyzeRatio, strconv.FormatFloat(DefAutoAnalyzeRatio, 'f', -1, 64), false},
	{ScopeGlobal, TiDBAutoAnalyzeStartTime, DefAutoAnalyzeStartTime, false},
	{ScopeGlobal, TiDBAutoAnalyzeEndTime, DefAutoAnalyzeEndTime, false},
	{ScopeSession, TiDBChecksumTableConcurrency, strconv.Itoa(DefChecksumTableConcurrency), false},
	{ScopeGlobal | ScopeSession, TiDBExecutorConcurrency, strconv.Itoa(DefExecutorConcurrency), false},
	{ScopeGlobal | ScopeSession, TiDBDistSQLScanConcurrency, strconv.Itoa(DefDistSQLScanConcurrency), false},
	{ScopeGlobal | ScopeSession, TiDBOptInSubqToJoinAndAgg, BoolToIntStr(DefOptInSubqToJoinAndAgg), false},
	{ScopeGlobal | ScopeSession, TiDBOptCorrelationThreshold, strconv.FormatFloat(DefOptCorrelationThreshold, 'f', -1, 64), false},
	{ScopeGlobal | ScopeSession, TiDBOptCorrelationExpFactor, strconv.Itoa(DefOptCorrelationExpFactor), false},
	{ScopeGlobal | ScopeSession, TiDBOptCPUFactor, strconv.FormatFloat(DefOptCPUFactor, 'f', -1, 64), false},
	{ScopeGlobal | ScopeSession, TiDBOptTiFlashConcurrencyFactor, strconv.FormatFloat(DefOptTiFlashConcurrencyFactor, 'f', -1, 64), false},
	{ScopeGlobal | ScopeSession, TiDBOptCopCPUFactor, strconv.FormatFloat(DefOptCopCPUFactor, 'f', -1, 64), false},
	{ScopeGlobal | ScopeSession, TiDBOptNetworkFactor, strconv.FormatFloat(DefOptNetworkFactor, 'f', -1, 64), false},
	{ScopeGlobal | ScopeSession, TiDBOptScanFactor, strconv.FormatFloat(DefOptScanFactor, 'f', -1, 64), false},
	{ScopeGlobal | ScopeSession, TiDBOptDescScanFactor, strconv.FormatFloat(DefOptDescScanFactor, 'f', -1, 64), false},
	{ScopeGlobal | ScopeSession, TiDBOptSeekFactor, strconv.FormatFloat(DefOptSeekFactor, 'f', -1, 64), false},
	{ScopeGlobal | ScopeSession, TiDBOptMemoryFactor, strconv.FormatFloat(DefOptMemoryFactor, 'f', -1, 64), false},
	{ScopeGlobal | ScopeSession, TiDBOptDiskFactor, strconv.FormatFloat(DefOptDiskFactor, 'f', -1, 64), false},
	{ScopeGlobal | ScopeSession, TiDBOptConcurrencyFactor, strconv.FormatFloat(DefOptConcurrencyFactor, 'f', -1, 64), false},
	{ScopeGlobal | ScopeSession, TiDBIndexJoinBatchSize, strconv.Itoa(DefIndexJoinBatchSize), false},
	{ScopeGlobal | ScopeSession, TiDBIndexLookupSize, strconv.Itoa(DefIndexLookupSize), false},
	{ScopeGlobal | ScopeSession, TiDBIndexLookupConcurrency, strconv.Itoa(DefIndexLookupConcurrency), false},
	{ScopeGlobal | ScopeSession, TiDBIndexLookupJoinConcurrency, strconv.Itoa(DefIndexLookupJoinConcurrency), false},
	{ScopeGlobal | ScopeSession, TiDBIndexSerialScanConcurrency, strconv.Itoa(DefIndexSerialScanConcurrency), false},
	{ScopeGlobal | ScopeSession, TiDBSkipUTF8Check, BoolToIntStr(DefSkipUTF8Check), false},
	{ScopeGlobal | ScopeSession, TiDBSkipASCIICheck, BoolToIntStr(DefSkipASCIICheck), false},
	{ScopeSession, TiDBBatchInsert, BoolToIntStr(DefBatchInsert), false},
	{ScopeSession, TiDBBatchDelete, BoolToIntStr(DefBatchDelete), false},
	{ScopeSession, TiDBBatchCommit, BoolToIntStr(DefBatchCommit), false},
	{ScopeGlobal | ScopeSession, TiDBDMLBatchSize, strconv.Itoa(DefDMLBatchSize), false},
	{ScopeSession, TiDBCurrentTS, strconv.Itoa(DefCurretTS), false},
	{ScopeSession, TiDBLastTxnInfo, strconv.Itoa(DefCurretTS), false},
	{ScopeGlobal | ScopeSession, TiDBMaxChunkSize, strconv.Itoa(DefMaxChunkSize), false},
	{ScopeGlobal | ScopeSession, TiDBAllowBatchCop, strconv.Itoa(DefTiDBAllowBatchCop), false},
	{ScopeGlobal | ScopeSession, TiDBInitChunkSize, strconv.Itoa(DefInitChunkSize), false},
	{ScopeGlobal | ScopeSession, TiDBEnableCascadesPlanner, "0", false},
	{ScopeGlobal | ScopeSession, TiDBEnableIndexMerge, "0", false},
	{ScopeSession, TIDBMemQuotaQuery, strconv.FormatInt(config.GetGlobalConfig().MemQuotaQuery, 10), false},
	{ScopeSession, TIDBMemQuotaHashJoin, strconv.FormatInt(DefTiDBMemQuotaHashJoin, 10), false},
	{ScopeSession, TIDBMemQuotaMergeJoin, strconv.FormatInt(DefTiDBMemQuotaMergeJoin, 10), false},
	{ScopeSession, TIDBMemQuotaSort, strconv.FormatInt(DefTiDBMemQuotaSort, 10), false},
	{ScopeSession, TIDBMemQuotaTopn, strconv.FormatInt(DefTiDBMemQuotaTopn, 10), false},
	{ScopeSession, TIDBMemQuotaIndexLookupReader, strconv.FormatInt(DefTiDBMemQuotaIndexLookupReader, 10), false},
	{ScopeSession, TIDBMemQuotaIndexLookupJoin, strconv.FormatInt(DefTiDBMemQuotaIndexLookupJoin, 10), false},
	{ScopeSession, TIDBMemQuotaNestedLoopApply, strconv.FormatInt(DefTiDBMemQuotaNestedLoopApply, 10), false},
	{ScopeSession, TiDBEnableStreaming, "0", false},
	{ScopeSession, TiDBEnableChunkRPC, "1", false},
	{ScopeSession, TxnIsolationOneShot, "", false},
	{ScopeGlobal | ScopeSession, TiDBEnableTablePartition, "on", false},
	{ScopeGlobal | ScopeSession, TiDBHashJoinConcurrency, strconv.Itoa(DefTiDBHashJoinConcurrency), false},
	{ScopeGlobal | ScopeSession, TiDBProjectionConcurrency, strconv.Itoa(DefTiDBProjectionConcurrency), false},
	{ScopeGlobal | ScopeSession, TiDBHashAggPartialConcurrency, strconv.Itoa(DefTiDBHashAggPartialConcurrency), false},
	{ScopeGlobal | ScopeSession, TiDBHashAggFinalConcurrency, strconv.Itoa(DefTiDBHashAggFinalConcurrency), false},
	{ScopeGlobal | ScopeSession, TiDBWindowConcurrency, strconv.Itoa(DefTiDBWindowConcurrency), false},
	{ScopeGlobal | ScopeSession, TiDBEnableParallelApply, BoolToIntStr(DefTiDBEnableParallelApply), false},
	{ScopeGlobal | ScopeSession, TiDBBackoffLockFast, strconv.Itoa(kv.DefBackoffLockFast), false},
	{ScopeGlobal | ScopeSession, TiDBBackOffWeight, strconv.Itoa(kv.DefBackOffWeight), false},
	{ScopeGlobal | ScopeSession, TiDBRetryLimit, strconv.Itoa(DefTiDBRetryLimit), false},
	{ScopeGlobal | ScopeSession, TiDBDisableTxnAutoRetry, BoolToIntStr(DefTiDBDisableTxnAutoRetry), false},
	{ScopeGlobal | ScopeSession, TiDBConstraintCheckInPlace, BoolToIntStr(DefTiDBConstraintCheckInPlace), false},
	{ScopeGlobal | ScopeSession, TiDBTxnMode, DefTiDBTxnMode, false},
	{ScopeGlobal, TiDBRowFormatVersion, strconv.Itoa(DefTiDBRowFormatV1), false},
	{ScopeSession, TiDBOptimizerSelectivityLevel, strconv.Itoa(DefTiDBOptimizerSelectivityLevel), false},
	{ScopeGlobal | ScopeSession, TiDBEnableWindowFunction, BoolToIntStr(DefEnableWindowFunction), false},
	{ScopeGlobal | ScopeSession, TiDBEnableVectorizedExpression, BoolToIntStr(DefEnableVectorizedExpression), false},
	{ScopeGlobal | ScopeSession, TiDBEnableFastAnalyze, BoolToIntStr(DefTiDBUseFastAnalyze), false},
	{ScopeGlobal | ScopeSession, TiDBSkipIsolationLevelCheck, BoolToIntStr(DefTiDBSkipIsolationLevelCheck), false},
	/* The following variable is defined as session scope but is actually server scope. */
	{ScopeSession, TiDBGeneralLog, strconv.Itoa(DefTiDBGeneralLog), false},
	{ScopeSession, TiDBPProfSQLCPU, strconv.Itoa(DefTiDBPProfSQLCPU), false},
	{ScopeSession, TiDBDDLSlowOprThreshold, strconv.Itoa(DefTiDBDDLSlowOprThreshold), false},
	{ScopeSession, TiDBConfig, "", false},
	{ScopeGlobal, TiDBDDLReorgWorkerCount, strconv.Itoa(DefTiDBDDLReorgWorkerCount), false},
	{ScopeGlobal, TiDBDDLReorgBatchSize, strconv.Itoa(DefTiDBDDLReorgBatchSize), false},
	{ScopeGlobal, TiDBDDLErrorCountLimit, strconv.Itoa(DefTiDBDDLErrorCountLimit), false},
	{ScopeSession, TiDBDDLReorgPriority, "PRIORITY_LOW", false},
	{ScopeGlobal, TiDBMaxDeltaSchemaCount, strconv.Itoa(DefTiDBMaxDeltaSchemaCount), false},
	{ScopeGlobal, TiDBEnableChangeColumnType, BoolToIntStr(DefTiDBChangeColumnType), false},
	{ScopeSession, TiDBForcePriority, mysql.Priority2Str[DefTiDBForcePriority], false},
	{ScopeSession, TiDBEnableRadixJoin, BoolToIntStr(DefTiDBUseRadixJoin), false},
	{ScopeGlobal | ScopeSession, TiDBOptJoinReorderThreshold, strconv.Itoa(DefTiDBOptJoinReorderThreshold), false},
	{ScopeSession, TiDBSlowQueryFile, "", false},
	{ScopeGlobal, TiDBScatterRegion, BoolToIntStr(DefTiDBScatterRegion), false},
	{ScopeSession, TiDBWaitSplitRegionFinish, BoolToIntStr(DefTiDBWaitSplitRegionFinish), false},
	{ScopeSession, TiDBWaitSplitRegionTimeout, strconv.Itoa(DefWaitSplitRegionTimeout), false},
	{ScopeSession, TiDBLowResolutionTSO, "0", false},
	{ScopeSession, TiDBExpensiveQueryTimeThreshold, strconv.Itoa(DefTiDBExpensiveQueryTimeThreshold), false},
	{ScopeGlobal | ScopeSession, TiDBEnableNoopFuncs, BoolToIntStr(DefTiDBEnableNoopFuncs), false},
	{ScopeSession, TiDBReplicaRead, "leader", false},
	{ScopeSession, TiDBAllowRemoveAutoInc, BoolToIntStr(DefTiDBAllowRemoveAutoInc), false},
	{ScopeGlobal | ScopeSession, TiDBEnableStmtSummary, BoolToIntStr(config.GetGlobalConfig().StmtSummary.Enable), false},
	{ScopeGlobal | ScopeSession, TiDBStmtSummaryInternalQuery, BoolToIntStr(config.GetGlobalConfig().StmtSummary.EnableInternalQuery), false},
	{ScopeGlobal | ScopeSession, TiDBStmtSummaryRefreshInterval, strconv.Itoa(config.GetGlobalConfig().StmtSummary.RefreshInterval), false},
	{ScopeGlobal | ScopeSession, TiDBStmtSummaryHistorySize, strconv.Itoa(config.GetGlobalConfig().StmtSummary.HistorySize), false},
	{ScopeGlobal | ScopeSession, TiDBStmtSummaryMaxStmtCount, strconv.FormatUint(uint64(config.GetGlobalConfig().StmtSummary.MaxStmtCount), 10), false},
	{ScopeGlobal | ScopeSession, TiDBStmtSummaryMaxSQLLength, strconv.FormatUint(uint64(config.GetGlobalConfig().StmtSummary.MaxSQLLength), 10), false},
	{ScopeGlobal | ScopeSession, TiDBCapturePlanBaseline, "off", false},
	{ScopeGlobal | ScopeSession, TiDBUsePlanBaselines, boolToOnOff(DefTiDBUsePlanBaselines), false},
	{ScopeGlobal | ScopeSession, TiDBEvolvePlanBaselines, boolToOnOff(DefTiDBEvolvePlanBaselines), false},
	{ScopeGlobal, TiDBEvolvePlanTaskMaxTime, strconv.Itoa(DefTiDBEvolvePlanTaskMaxTime), false},
	{ScopeGlobal, TiDBEvolvePlanTaskStartTime, DefTiDBEvolvePlanTaskStartTime, false},
	{ScopeGlobal, TiDBEvolvePlanTaskEndTime, DefTiDBEvolvePlanTaskEndTime, false},
	{ScopeSession, TiDBIsolationReadEngines, strings.Join(config.GetGlobalConfig().IsolationRead.Engines, ", "), false},
	{ScopeGlobal | ScopeSession, TiDBStoreLimit, strconv.FormatInt(atomic.LoadInt64(&config.GetGlobalConfig().TiKVClient.StoreLimit), 10), false},
	{ScopeSession, TiDBMetricSchemaStep, strconv.Itoa(DefTiDBMetricSchemaStep), false},
	{ScopeSession, TiDBMetricSchemaRangeDuration, strconv.Itoa(DefTiDBMetricSchemaRangeDuration), false},
	{ScopeSession, TiDBSlowLogThreshold, strconv.Itoa(logutil.DefaultSlowThreshold), false},
	{ScopeSession, TiDBRecordPlanInSlowLog, strconv.Itoa(logutil.DefaultRecordPlanInSlowLog), false},
	{ScopeSession, TiDBEnableSlowLog, BoolToIntStr(logutil.DefaultTiDBEnableSlowLog), false},
	{ScopeSession, TiDBQueryLogMaxLen, strconv.Itoa(logutil.DefaultQueryLogMaxLen), false},
	{ScopeSession, TiDBCheckMb4ValueInUTF8, BoolToIntStr(config.GetGlobalConfig().CheckMb4ValueInUTF8), false},
	{ScopeSession, TiDBFoundInPlanCache, BoolToIntStr(DefTiDBFoundInPlanCache), false},
	{ScopeSession, TiDBEnableCollectExecutionInfo, BoolToIntStr(DefTiDBEnableCollectExecutionInfo), false},
	{ScopeGlobal | ScopeSession, TiDBAllowAutoRandExplicitInsert, boolToOnOff(DefTiDBAllowAutoRandExplicitInsert), false},
	{ScopeGlobal | ScopeSession, TiDBEnableClusteredIndex, BoolToIntStr(DefTiDBEnableClusteredIndex), false},
	{ScopeGlobal | ScopeSession, TiDBPartitionPruneMode, string(StaticOnly), false},
	{ScopeGlobal, TiDBSlowLogMasking, BoolToIntStr(DefTiDBSlowLogMasking), false},
	{ScopeGlobal, TiDBRedactLog, strconv.Itoa(config.DefTiDBRedactLog), false},
	{ScopeGlobal | ScopeSession, TiDBShardAllocateStep, strconv.Itoa(DefTiDBShardAllocateStep), false},
	{ScopeGlobal, TiDBEnableTelemetry, BoolToIntStr(DefTiDBEnableTelemetry), false},
	{ScopeGlobal | ScopeSession, TiDBEnableAmendPessimisticTxn, boolToOnOff(DefTiDBEnableAmendPessimisticTxn), false},

	// for compatibility purpose, we should leave them alone.
	// TODO: Follow the Terminology Updates of MySQL after their changes arrived.
	// https://mysqlhighavailability.com/mysql-terminology-updates/
	{ScopeSession, PseudoSlaveMode, "", false},
	{ScopeGlobal, "slave_pending_jobs_size_max", "16777216", false},
	{ScopeGlobal, "slave_transaction_retries", "10", false},
	{ScopeGlobal, "slave_checkpoint_period", "300", false},
	{ScopeGlobal, MasterVerifyChecksum, "0", false},
	{ScopeGlobal, "rpl_semi_sync_master_trace_level", "", false},
	{ScopeGlobal, "master_info_repository", "FILE", false},
	{ScopeGlobal, "rpl_stop_slave_timeout", "31536000", false},
	{ScopeGlobal, "slave_net_timeout", "3600", false},
	{ScopeGlobal, "sync_master_info", "10000", false},
	{ScopeGlobal, "init_slave", "", false},
	{ScopeGlobal, SlaveCompressedProtocol, "0", false},
	{ScopeGlobal, "rpl_semi_sync_slave_trace_level", "", false},
	{ScopeGlobal, LogSlowSlaveStatements, "0", false},
	{ScopeGlobal, "slave_checkpoint_group", "512", false},
	{ScopeNone, "slave_load_tmpdir", "/var/tmp/", false},
	{ScopeGlobal, "slave_parallel_type", "", false},
	{ScopeGlobal, "slave_parallel_workers", "0", false},
	{ScopeGlobal, "rpl_semi_sync_master_timeout", "", false},
	{ScopeNone, "slave_skip_errors", "OFF", false},
	{ScopeGlobal, "sql_slave_skip_counter", "0", false},
	{ScopeGlobal, "rpl_semi_sync_slave_enabled", "", false},
	{ScopeGlobal, "rpl_semi_sync_master_enabled", "", false},
	{ScopeGlobal, "slave_preserve_commit_order", "", false},
	{ScopeGlobal, "slave_exec_mode", "STRICT", false},
	{ScopeNone, "log_slave_updates", "0", false},
	{ScopeGlobal, "rpl_semi_sync_master_wait_point", "", false},
	{ScopeGlobal, "slave_sql_verify_checksum", "1", false},
	{ScopeGlobal, "slave_max_allowed_packet", "1073741824", false},
	{ScopeGlobal, "rpl_semi_sync_master_wait_for_slave_count", "", false},
	{ScopeGlobal, "rpl_semi_sync_master_wait_no_slave", "", false},
	{ScopeGlobal, "slave_rows_search_algorithms", "TABLE_SCAN,INDEX_SCAN", false},
	{ScopeGlobal, SlaveAllowBatching, "0", false},
}

// SynonymsSysVariables is synonyms of system variables.
var SynonymsSysVariables = map[string][]string{}

func addSynonymsSysVariables(synonyms ...string) {
	for _, s := range synonyms {
		SynonymsSysVariables[s] = synonyms
	}
}

func initSynonymsSysVariables() {
	addSynonymsSysVariables(TxnIsolation, TransactionIsolation)
	addSynonymsSysVariables(TxReadOnly, TransactionReadOnly)
}

// SetNamesVariables is the system variable names related to set names statements.
var SetNamesVariables = []string{
	"character_set_client",
	"character_set_connection",
	"character_set_results",
}

// SetCharsetVariables is the system variable names related to set charset statements.
var SetCharsetVariables = []string{
	"character_set_client",
	"character_set_results",
}

const (
	// CharacterSetConnection is the name for character_set_connection system variable.
	CharacterSetConnection = "character_set_connection"
	// CollationConnection is the name for collation_connection system variable.
	CollationConnection = "collation_connection"
	// CharsetDatabase is the name for character_set_database system variable.
	CharsetDatabase = "character_set_database"
	// CollationDatabase is the name for collation_database system variable.
	CollationDatabase = "collation_database"
	// GeneralLog is the name for 'general_log' system variable.
	GeneralLog = "general_log"
	// AvoidTemporalUpgrade is the name for 'avoid_temporal_upgrade' system variable.
	AvoidTemporalUpgrade = "avoid_temporal_upgrade"
	// MaxPreparedStmtCount is the name for 'max_prepared_stmt_count' system variable.
	MaxPreparedStmtCount = "max_prepared_stmt_count"
	// BigTables is the name for 'big_tables' system variable.
	BigTables = "big_tables"
	// CheckProxyUsers is the name for 'check_proxy_users' system variable.
	CheckProxyUsers = "check_proxy_users"
	// CoreFile is the name for 'core_file' system variable.
	CoreFile = "core_file"
	// DefaultWeekFormat is the name for 'default_week_format' system variable.
	DefaultWeekFormat = "default_week_format"
	// GroupConcatMaxLen is the name for 'group_concat_max_len' system variable.
	GroupConcatMaxLen = "group_concat_max_len"
	// DelayKeyWrite is the name for 'delay_key_write' system variable.
	DelayKeyWrite = "delay_key_write"
	// EndMakersInJSON is the name for 'end_markers_in_json' system variable.
	EndMakersInJSON = "end_markers_in_json"
	// InnodbCommitConcurrency is the name for 'innodb_commit_concurrency' system variable.
	InnodbCommitConcurrency = "innodb_commit_concurrency"
	// InnodbFastShutdown is the name for 'innodb_fast_shutdown' system variable.
	InnodbFastShutdown = "innodb_fast_shutdown"
	// InnodbLockWaitTimeout is the name for 'innodb_lock_wait_timeout' system variable.
	InnodbLockWaitTimeout = "innodb_lock_wait_timeout"
	// SQLLogBin is the name for 'sql_log_bin' system variable.
	SQLLogBin = "sql_log_bin"
	// LogBin is the name for 'log_bin' system variable.
	LogBin = "log_bin"
	// MaxSortLength is the name for 'max_sort_length' system variable.
	MaxSortLength = "max_sort_length"
	// MaxSpRecursionDepth is the name for 'max_sp_recursion_depth' system variable.
	MaxSpRecursionDepth = "max_sp_recursion_depth"
	// MaxUserConnections is the name for 'max_user_connections' system variable.
	MaxUserConnections = "max_user_connections"
	// OfflineMode is the name for 'offline_mode' system variable.
	OfflineMode = "offline_mode"
	// InteractiveTimeout is the name for 'interactive_timeout' system variable.
	InteractiveTimeout = "interactive_timeout"
	// FlushTime is the name for 'flush_time' system variable.
	FlushTime = "flush_time"
	// PseudoSlaveMode is the name for 'pseudo_slave_mode' system variable.
	PseudoSlaveMode = "pseudo_slave_mode"
	// LowPriorityUpdates is the name for 'low_priority_updates' system variable.
	LowPriorityUpdates = "low_priority_updates"
	// SessionTrackGtids is the name for 'session_track_gtids' system variable.
	SessionTrackGtids = "session_track_gtids"
	// OldPasswords is the name for 'old_passwords' system variable.
	OldPasswords = "old_passwords"
	// MaxConnections is the name for 'max_connections' system variable.
	MaxConnections = "max_connections"
	// SkipNameResolve is the name for 'skip_name_resolve' system variable.
	SkipNameResolve = "skip_name_resolve"
	// ForeignKeyChecks is the name for 'foreign_key_checks' system variable.
	ForeignKeyChecks = "foreign_key_checks"
	// SQLSafeUpdates is the name for 'sql_safe_updates' system variable.
	SQLSafeUpdates = "sql_safe_updates"
	// WarningCount is the name for 'warning_count' system variable.
	WarningCount = "warning_count"
	// ErrorCount is the name for 'error_count' system variable.
	ErrorCount = "error_count"
	// SQLSelectLimit is the name for 'sql_select_limit' system variable.
	SQLSelectLimit = "sql_select_limit"
	// MaxConnectErrors is the name for 'max_connect_errors' system variable.
	MaxConnectErrors = "max_connect_errors"
	// TableDefinitionCache is the name for 'table_definition_cache' system variable.
	TableDefinitionCache = "table_definition_cache"
	// TmpTableSize is the name for 'tmp_table_size' system variable.
	TmpTableSize = "tmp_table_size"
	// ConnectTimeout is the name for 'connect_timeout' system variable.
	ConnectTimeout = "connect_timeout"
	// SyncBinlog is the name for 'sync_binlog' system variable.
	SyncBinlog = "sync_binlog"
	// BlockEncryptionMode is the name for 'block_encryption_mode' system variable.
	BlockEncryptionMode = "block_encryption_mode"
	// WaitTimeout is the name for 'wait_timeout' system variable.
	WaitTimeout = "wait_timeout"
	// ValidatePasswordNumberCount is the name of 'validate_password_number_count' system variable.
	ValidatePasswordNumberCount = "validate_password_number_count"
	// ValidatePasswordLength is the name of 'validate_password_length' system variable.
	ValidatePasswordLength = "validate_password_length"
	// PluginDir is the name of 'plugin_dir' system variable.
	PluginDir = "plugin_dir"
	// PluginLoad is the name of 'plugin_load' system variable.
	PluginLoad = "plugin_load"
	// Port is the name for 'port' system variable.
	Port = "port"
	// DataDir is the name for 'datadir' system variable.
	DataDir = "datadir"
	// Profiling is the name for 'Profiling' system variable.
	Profiling = "profiling"
	// Socket is the name for 'socket' system variable.
	Socket = "socket"
	// BinlogOrderCommits is the name for 'binlog_order_commits' system variable.
	BinlogOrderCommits = "binlog_order_commits"
	// MasterVerifyChecksum is the name for 'master_verify_checksum' system variable.
	MasterVerifyChecksum = "master_verify_checksum"
	// ValidatePasswordCheckUserName is the name for 'validate_password_check_user_name' system variable.
	ValidatePasswordCheckUserName = "validate_password_check_user_name"
	// SuperReadOnly is the name for 'super_read_only' system variable.
	SuperReadOnly = "super_read_only"
	// SQLNotes is the name for 'sql_notes' system variable.
	SQLNotes = "sql_notes"
	// QueryCacheType is the name for 'query_cache_type' system variable.
	QueryCacheType = "query_cache_type"
	// SlaveCompressedProtocol is the name for 'slave_compressed_protocol' system variable.
	SlaveCompressedProtocol = "slave_compressed_protocol"
	// BinlogRowQueryLogEvents is the name for 'binlog_rows_query_log_events' system variable.
	BinlogRowQueryLogEvents = "binlog_rows_query_log_events"
	// LogSlowSlaveStatements is the name for 'log_slow_slave_statements' system variable.
	LogSlowSlaveStatements = "log_slow_slave_statements"
	// LogSlowAdminStatements is the name for 'log_slow_admin_statements' system variable.
	LogSlowAdminStatements = "log_slow_admin_statements"
	// LogQueriesNotUsingIndexes is the name for 'log_queries_not_using_indexes' system variable.
	LogQueriesNotUsingIndexes = "log_queries_not_using_indexes"
	// QueryCacheWlockInvalidate is the name for 'query_cache_wlock_invalidate' system variable.
	QueryCacheWlockInvalidate = "query_cache_wlock_invalidate"
	// SQLAutoIsNull is the name for 'sql_auto_is_null' system variable.
	SQLAutoIsNull = "sql_auto_is_null"
	// RelayLogPurge is the name for 'relay_log_purge' system variable.
	RelayLogPurge = "relay_log_purge"
	// AutomaticSpPrivileges is the name for 'automatic_sp_privileges' system variable.
	AutomaticSpPrivileges = "automatic_sp_privileges"
	// SQLQuoteShowCreate is the name for 'sql_quote_show_create' system variable.
	SQLQuoteShowCreate = "sql_quote_show_create"
	// SlowQueryLog is the name for 'slow_query_log' system variable.
	SlowQueryLog = "slow_query_log"
	// BinlogDirectNonTransactionalUpdates is the name for 'binlog_direct_non_transactional_updates' system variable.
	BinlogDirectNonTransactionalUpdates = "binlog_direct_non_transactional_updates"
	// SQLBigSelects is the name for 'sql_big_selects' system variable.
	SQLBigSelects = "sql_big_selects"
	// LogBinTrustFunctionCreators is the name for 'log_bin_trust_function_creators' system variable.
	LogBinTrustFunctionCreators = "log_bin_trust_function_creators"
	// OldAlterTable is the name for 'old_alter_table' system variable.
	OldAlterTable = "old_alter_table"
	// EnforceGtidConsistency is the name for 'enforce_gtid_consistency' system variable.
	EnforceGtidConsistency = "enforce_gtid_consistency"
	// SecureAuth is the name for 'secure_auth' system variable.
	SecureAuth = "secure_auth"
	// UniqueChecks is the name for 'unique_checks' system variable.
	UniqueChecks = "unique_checks"
	// SQLWarnings is the name for 'sql_warnings' system variable.
	SQLWarnings = "sql_warnings"
	// AutoCommit is the name for 'autocommit' system variable.
	AutoCommit = "autocommit"
	// KeepFilesOnCreate is the name for 'keep_files_on_create' system variable.
	KeepFilesOnCreate = "keep_files_on_create"
	// ShowOldTemporals is the name for 'show_old_temporals' system variable.
	ShowOldTemporals = "show_old_temporals"
	// LocalInFile is the name for 'local_infile' system variable.
	LocalInFile = "local_infile"
	// PerformanceSchema is the name for 'performance_schema' system variable.
	PerformanceSchema = "performance_schema"
	// Flush is the name for 'flush' system variable.
	Flush = "flush"
	// SlaveAllowBatching is the name for 'slave_allow_batching' system variable.
	SlaveAllowBatching = "slave_allow_batching"
	// MyISAMUseMmap is the name for 'myisam_use_mmap' system variable.
	MyISAMUseMmap = "myisam_use_mmap"
	// InnodbFilePerTable is the name for 'innodb_file_per_table' system variable.
	InnodbFilePerTable = "innodb_file_per_table"
	// InnodbLogCompressedPages is the name for 'innodb_log_compressed_pages' system variable.
	InnodbLogCompressedPages = "innodb_log_compressed_pages"
	// InnodbPrintAllDeadlocks is the name for 'innodb_print_all_deadlocks' system variable.
	InnodbPrintAllDeadlocks = "innodb_print_all_deadlocks"
	// InnodbStrictMode is the name for 'innodb_strict_mode' system variable.
	InnodbStrictMode = "innodb_strict_mode"
	// InnodbCmpPerIndexEnabled is the name for 'innodb_cmp_per_index_enabled' system variable.
	InnodbCmpPerIndexEnabled = "innodb_cmp_per_index_enabled"
	// InnodbBufferPoolDumpAtShutdown is the name for 'innodb_buffer_pool_dump_at_shutdown' system variable.
	InnodbBufferPoolDumpAtShutdown = "innodb_buffer_pool_dump_at_shutdown"
	// InnodbAdaptiveHashIndex is the name for 'innodb_adaptive_hash_index' system variable.
	InnodbAdaptiveHashIndex = "innodb_adaptive_hash_index"
	// InnodbFtEnableStopword is the name for 'innodb_ft_enable_stopword' system variable.
	InnodbFtEnableStopword = "innodb_ft_enable_stopword"
	// InnodbSupportXA is the name for 'innodb_support_xa' system variable.
	InnodbSupportXA = "innodb_support_xa"
	// InnodbOptimizeFullTextOnly is the name for 'innodb_optimize_fulltext_only' system variable.
	InnodbOptimizeFullTextOnly = "innodb_optimize_fulltext_only"
	// InnodbStatusOutputLocks is the name for 'innodb_status_output_locks' system variable.
	InnodbStatusOutputLocks = "innodb_status_output_locks"
	// InnodbBufferPoolDumpNow is the name for 'innodb_buffer_pool_dump_now' system variable.
	InnodbBufferPoolDumpNow = "innodb_buffer_pool_dump_now"
	// InnodbBufferPoolLoadNow is the name for 'innodb_buffer_pool_load_now' system variable.
	InnodbBufferPoolLoadNow = "innodb_buffer_pool_load_now"
	// InnodbStatsOnMetadata is the name for 'innodb_stats_on_metadata' system variable.
	InnodbStatsOnMetadata = "innodb_stats_on_metadata"
	// InnodbDisableSortFileCache is the name for 'innodb_disable_sort_file_cache' system variable.
	InnodbDisableSortFileCache = "innodb_disable_sort_file_cache"
	// InnodbStatsAutoRecalc is the name for 'innodb_stats_auto_recalc' system variable.
	InnodbStatsAutoRecalc = "innodb_stats_auto_recalc"
	// InnodbBufferPoolLoadAbort is the name for 'innodb_buffer_pool_load_abort' system variable.
	InnodbBufferPoolLoadAbort = "innodb_buffer_pool_load_abort"
	// InnodbStatsPersistent is the name for 'innodb_stats_persistent' system variable.
	InnodbStatsPersistent = "innodb_stats_persistent"
	// InnodbRandomReadAhead is the name for 'innodb_random_read_ahead' system variable.
	InnodbRandomReadAhead = "innodb_random_read_ahead"
	// InnodbAdaptiveFlushing is the name for 'innodb_adaptive_flushing' system variable.
	InnodbAdaptiveFlushing = "innodb_adaptive_flushing"
	// InnodbTableLocks is the name for 'innodb_table_locks' system variable.
	InnodbTableLocks = "innodb_table_locks"
	// InnodbStatusOutput is the name for 'innodb_status_output' system variable.
	InnodbStatusOutput = "innodb_status_output"

	// NetBufferLength is the name for 'net_buffer_length' system variable.
	NetBufferLength = "net_buffer_length"
	// QueryCacheSize is the name of 'query_cache_size' system variable.
	QueryCacheSize = "query_cache_size"
	// TxReadOnly is the name of 'tx_read_only' system variable.
	TxReadOnly = "tx_read_only"
	// TransactionReadOnly is the name of 'transaction_read_only' system variable.
	TransactionReadOnly = "transaction_read_only"
	// CharacterSetServer is the name of 'character_set_server' system variable.
	CharacterSetServer = "character_set_server"
	// AutoIncrementIncrement is the name of 'auto_increment_increment' system variable.
	AutoIncrementIncrement = "auto_increment_increment"
	// AutoIncrementOffset is the name of 'auto_increment_offset' system variable.
	AutoIncrementOffset = "auto_increment_offset"
	// InitConnect is the name of 'init_connect' system variable.
	InitConnect = "init_connect"
	// CollationServer is the name of 'collation_server' variable.
	CollationServer = "collation_server"
	// NetWriteTimeout is the name of 'net_write_timeout' variable.
	NetWriteTimeout = "net_write_timeout"
	// ThreadPoolSize is the name of 'thread_pool_size' variable.
	ThreadPoolSize = "thread_pool_size"
	// WindowingUseHighPrecision is the name of 'windowing_use_high_precision' system variable.
	WindowingUseHighPrecision = "windowing_use_high_precision"
)

// GlobalVarAccessor is the interface for accessing global scope system and status variables.
type GlobalVarAccessor interface {
	// GetAllSysVars gets all the global system variable values.
	GetAllSysVars() (map[string]string, error)
	// GetGlobalSysVar gets the global system variable value for name.
	GetGlobalSysVar(name string) (string, error)
	// SetGlobalSysVar sets the global system variable name to value.
	SetGlobalSysVar(name string, value string) error
}
