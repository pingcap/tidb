package infoschema

import (
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

func init() {
	tableIDMap[TableUserLoginHistory] = autoid.ReservedTablesBaseID
	tableIDMap[ClusterTableAuditLog] = autoid.ReservedTablesBaseID + 1
	tableIDMap[TableAuditLog] = autoid.ReservedTablesBaseID + 2
	// there's a hole of autoid.ReservedTablesBaseID + 3. Can be reused later
	tableIDMap[TableRegions] = autoid.ReservedTablesBaseID + 4
	tableIDMap[TableLogReplStatusGlobal] = autoid.ReservedTablesBaseID + 5
	tableIDMap[TableLogReplClusterStatusGlobal] = autoid.ReservedTablesBaseID + 6
	tableIDMap[TableLogReplWorkflowHistoryGlobal] = autoid.ReservedTablesBaseID + 7
	tableIDMap[TableLogReplStatusLocal] = autoid.ReservedTablesBaseID + 8

	init2()
}

// ClusterRoleStrs contains the possible roles of a cluster in log replication.
var ClusterRoleStrs = []string{
	"PRIMARY",
	"STANDBY",
}

// ReadyStatusStrs contains the possible ready statuses for switchover/failover.
var ReadyStatusStrs = []string{
	"YES",
	"NO",
	"UNKNOWN",
}

// ProtectionModeStrs contains the possible protection modes for log replication.
var ProtectionModeStrs = []string{
	"MAXIMUM_PERFORMANCE",
	"MAXIMUM_PROTECTION",
	"MAXIMUM_AVAILABILITY",
}

// TableLogReplStatusGlobalCols contains column definitions for TIDB_LOG_REPLICATION_STATUS table.
var TableLogReplStatusGlobalCols = []columnInfo{
	{name: "NAME", tp: mysql.TypeVarchar, flag: mysql.PriKeyFlag | mysql.NotNullFlag, size: 64},
	{name: "REPLICA_CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.NotNullFlag | mysql.UnsignedFlag, size: 21},
	{name: "SOURCE_CLUSTER_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "PROTECTION_MODE", tp: mysql.TypeEnum, enumElems: ProtectionModeStrs, flag: mysql.NotNullFlag, size: 64},
	{name: "DEGRADE_TIMEOUT", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "STATE", tp: mysql.TypeVarchar, flag: mysql.NotNullFlag, size: 64},
	{name: "CHECKPOINT_TS", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "CHECKPOINT_TIME", tp: mysql.TypeTimestamp, size: 26},
	{name: "CHECKPOINT_LAG", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "INITIALIZING_PROGRESS", tp: mysql.TypeFloat, size: 12},
	{name: "LAST_HEARTBEAT_TIME", tp: mysql.TypeTimestamp, size: 26},
}

// TableLogReplClusterStatusGlobalCols contains column definitions for TIDB_LOG_REPLICATION_CLUSTER_STATUS table.
var TableLogReplClusterStatusGlobalCols = []columnInfo{
	{name: "CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.PriKeyFlag | mysql.NotNullFlag | mysql.UnsignedFlag, size: 21},
	{name: "SOURCE_CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "ROLE", tp: mysql.TypeEnum, enumElems: ClusterRoleStrs, flag: mysql.NotNullFlag, size: 32},
	{name: "SWITCHOVER_READY", tp: mysql.TypeEnum, enumElems: ReadyStatusStrs, flag: mysql.NotNullFlag, size: 16},
	{name: "FAILOVER_READY", tp: mysql.TypeEnum, enumElems: ReadyStatusStrs, flag: mysql.NotNullFlag, size: 16},
	{name: "LOG_REPLICATION_STATE", tp: mysql.TypeVarchar, flag: mysql.NotNullFlag, size: 64},
	{name: "LAST_HEARTBEAT_TIME", tp: mysql.TypeTimestamp, size: 26},
}

// TableLogReplWorkflowHistoryGlobalCols contains column definitions for TIDB_LOG_REPLICATION_WORKFLOW_HISTORY table.
var TableLogReplWorkflowHistoryGlobalCols = []columnInfo{
	{name: "WORKFLOW_ID", tp: mysql.TypeLonglong, flag: mysql.PriKeyFlag | mysql.NotNullFlag | mysql.UnsignedFlag, size: 21},
	{name: "LOG_REPLICATION_NAME", tp: mysql.TypeVarchar, flag: mysql.NotNullFlag, size: 64},
	{name: "REPLICA_CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.NotNullFlag | mysql.UnsignedFlag, size: 21},
	{name: "SOURCE_CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.NotNullFlag | mysql.UnsignedFlag, size: 21},
	{name: "WORKFLOW_TYPE", tp: mysql.TypeVarchar, flag: mysql.NotNullFlag, size: 32},
	{name: "WORKFLOW_INFO", tp: mysql.TypeVarchar, size: 65535},
	{name: "START_TIME", tp: mysql.TypeTimestamp, flag: mysql.NotNullFlag, size: 26},
	{name: "END_TIME", tp: mysql.TypeTimestamp, size: 26},
	{name: "WORKFLOW_STATE", tp: mysql.TypeVarchar, flag: mysql.NotNullFlag, size: 32},
	{name: "WORKFLOW_STATE_INFO", tp: mysql.TypeVarchar, size: 65535},
	{name: "INITIATOR_CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag | mysql.NotNullFlag, size: 21},
}

// TableLogReplStatusLocalCols contains column definitions for TIDB_LOG_REPLICATION_STATUS_LOCAL table.
var TableLogReplStatusLocalCols = []columnInfo{
	{name: "CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.PriKeyFlag | mysql.NotNullFlag | mysql.UnsignedFlag, size: 21},
	{name: "ROLE", tp: mysql.TypeEnum, enumElems: ClusterRoleStrs, flag: mysql.NotNullFlag, size: 32},
	{name: "HAS_REPLICA", tp: mysql.TypeTiny, flag: mysql.NotNullFlag, size: 1},
	{name: "LAST_GLOBAL_UPDATE", tp: mysql.TypeTimestamp, size: 26},
	{name: "LOG_REPLICATION_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "SOURCE_CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "SOURCE_PD_ADDRS", tp: mysql.TypeVarchar, size: 65535},
	{name: "PROTECTION_MODE", tp: mysql.TypeEnum, enumElems: ProtectionModeStrs, size: 64},
	{name: "DEGRADE_TIMEOUT", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "LOG_REPLICATION_STATE", tp: mysql.TypeVarchar, size: 64},
	{name: "CHECKPOINT_TS", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "CHECKPOINT_TIME", tp: mysql.TypeTimestamp, size: 26},
	{name: "CHECKPOINT_LAG", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "SWITCHOVER_READY", tp: mysql.TypeEnum, enumElems: ReadyStatusStrs, size: 16},
	{name: "FAILOVER_READY", tp: mysql.TypeEnum, enumElems: ReadyStatusStrs, size: 16},
	{name: "INITIALIZING_PROGRESS", tp: mysql.TypeFloat, size: 12},
}
