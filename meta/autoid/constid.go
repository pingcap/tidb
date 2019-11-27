package autoid

import "github.com/pingcap/errors"

// GetSystemSchemaID gets the system schema ID.
func GetSystemSchemaID(dbName string) (int64, error) {
	id, ok := SystemSchemaIDMap[dbName]
	if !ok {
		return 0, errors.Errorf("get system schema id failed, unknown system schema `%v`", dbName)
	}
	return SystemIDFlag | id, nil
}

// GetInformationSchemaTableID gets the information_schema table ID.
func GetInformationSchemaTableID(tableName string) (int64, error) {
	id, ok := InformationSchemaTableIDMap[tableName]
	if !ok {
		return 0, errors.Errorf("get system table id failed, unknown system table `%v`", tableName)
	}
	return SystemIDFlag | id, nil
}

// GetPerformanceSchemaTableID gets the performance_schema table ID.
func GetPerformanceSchemaTableID(tableName string) (int64, error) {
	id, ok := PerformanceSchemaTableIDMap[tableName]
	if !ok {
		return 0, errors.Errorf("get system table id failed, unknown system table `%v`", tableName)
	}
	return SystemIDFlag | id, nil
}

// SystemIDFlag exports for test.
const SystemIDFlag = 1 << 62

// Attention:
// For reading cluster TiDB memory tables, the system schema/table should be same.
// Once the system schema/table id been allocated, it can't be changed any more.
// Change the system schema/table id may have the compatibility problem.

// SystemSchemaIDMap exports for test.
var SystemSchemaIDMap = map[string]int64{
	"INFORMATION_SCHEMA": 1,
	"PERFORMANCE_SCHEMA": 2,

	// Reserve to math.MaxInt64 - 10000,
}

// InformationSchemaTableIDMap exports for test.
var InformationSchemaTableIDMap = map[string]int64{
	"SCHEMATA":                              10001,
	"TABLES":                                10002,
	"COLUMNS":                               10003,
	"COLUMN_STATISTICS":                     10004,
	"STATISTICS":                            10005,
	"CHARACTER_SETS":                        10006,
	"COLLATIONS":                            10007,
	"FILES":                                 10008,
	"def":                                   10009,
	"PROFILING":                             10010,
	"PARTITIONS":                            10011,
	"KEY_COLUMN_USAGE":                      10012,
	"REFERENTIAL_CONSTRAINTS":               10013,
	"SESSION_VARIABLES":                     10014,
	"PLUGINS":                               10015,
	"TABLE_CONSTRAINTS":                     10016,
	"TRIGGERS":                              10017,
	"USER_PRIVILEGES":                       10018,
	"SCHEMA_PRIVILEGES":                     10019,
	"TABLE_PRIVILEGES":                      10020,
	"COLUMN_PRIVILEGES":                     10021,
	"ENGINES":                               10022,
	"VIEWS":                                 10023,
	"ROUTINES":                              10024,
	"PARAMETERS":                            10025,
	"EVENTS":                                10026,
	"GLOBAL_STATUS":                         10027,
	"GLOBAL_VARIABLES":                      10028,
	"SESSION_STATUS":                        10029,
	"OPTIMIZER_TRACE":                       10030,
	"TABLESPACES":                           10031,
	"COLLATION_CHARACTER_SET_APPLICABILITY": 10032,
	"PROCESSLIST":                           10033,
	"TIDB_INDEXES":                          10034,
	"SLOW_QUERY":                            10035,
	"TIDB_HOT_REGIONS":                      10036,
	"TIKV_STORE_STATUS":                     10037,
	"ANALYZE_STATUS":                        10038,
	"TIKV_REGION_STATUS":                    10039,
	"TIKV_REGION_PEERS":                     10040,
	"TIDB_SERVERS_INFO":                     10041,
	"TIDB_CLUSTER_INFO":                     10042,
	"TIDB_CLUSTER_CONFIG":                   10043,
	"TIDB_CLUSTER_LOAD":                     10044,
	"TIFLASH_REPLICA":                       10045,
	"TIDB_CLUSTER_SLOW_QUERY":               10046,
	"TIDB_CLUSTER_PROCESSLIST":              10047,

	// Reserve to math.MaxInt64 - 20000.
}

// PerformanceSchemaTableIDMap exports for test.
var PerformanceSchemaTableIDMap = map[string]int64{
	"global_status":                       20001,
	"session_status":                      20002,
	"setup_actors":                        20003,
	"setup_objects":                       20004,
	"setup_instruments":                   20005,
	"setup_consumers":                     20006,
	"events_statements_current":           20007,
	"events_statements_history":           20008,
	"events_statements_history_long":      20009,
	"prepared_statements_instances":       20010,
	"events_transactions_current":         20011,
	"events_transactions_history":         20012,
	"events_transactions_history_long":    20013,
	"events_stages_current":               20014,
	"events_stages_history":               20015,
	"events_stages_history_long":          20016,
	"events_statements_summary_by_digest": 20017,
	"tidb_profile_cpu":                    20018,
	"tidb_profile_memory":                 20019,
	"tidb_profile_mutex":                  20020,
	"tidb_profile_allocs":                 20021,
	"tidb_profile_block":                  20022,
	"tidb_profile_goroutines":             20023,

	// Reserve to math.MaxInt64 - 30000.
}
