package infoschema

import (
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/tikv/pd/client/clients/pkdb"
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

// ClusterRoleStrs and the enum variables contains the possible roles of a
// cluster in log replication.
var (
	ClusterRoleStrs = []string{
		"PRIMARY",
		"STANDBY",
		"STANDARD",
	}
	ClusterRolePrimaryEnum, _  = types.ParseEnumName(ClusterRoleStrs, "PRIMARY", mysql.DefaultCollationName)
	ClusterRoleStandbyEnum, _  = types.ParseEnumName(ClusterRoleStrs, "STANDBY", mysql.DefaultCollationName)
	ClusterRoleStandardEnum, _ = types.ParseEnumName(ClusterRoleStrs, "STANDARD", mysql.DefaultCollationName)
)

// ReadyStatusStrs and the enum variables contains the possible ready statuses
// for switchover/failover.
var (
	ReadyStatusStrs = []string{
		pkdb.LogReplicationValueYes,
		pkdb.LogReplicationValueNo,
		pkdb.LogReplicationUnknown,
	}
	ReadyStatusUnknownEnum, _ = types.ParseEnumName(ReadyStatusStrs, pkdb.LogReplicationUnknown, mysql.DefaultCollationName)
)

// ProtectionModeStrs and the enum variables contains the possible protection
// modes for log replication.
var (
	ProtectionModeStrs = []string{
		"MAXIMUM_PERFORMANCE",
		"MAXIMUM_PROTECTION",
		"MAXIMUM_AVAILABILITY",
		"UNKNOWN",
	}
	ProtectionModeMaximumPerformanceEnum, _  = types.ParseEnumName(ProtectionModeStrs, "MAXIMUM_PERFORMANCE", mysql.DefaultCollationName)
	ProtectionModeMaximumProtectionEnum, _   = types.ParseEnumName(ProtectionModeStrs, "MAXIMUM_PROTECTION", mysql.DefaultCollationName)
	ProtectionModeMaximumAvailabilityEnum, _ = types.ParseEnumName(ProtectionModeStrs, "MAXIMUM_AVAILABILITY", mysql.DefaultCollationName)
	ProtectionModeMaximumUnknownEnum, _      = types.ParseEnumName(ProtectionModeStrs, "UNKNOWN", mysql.DefaultCollationName)
)

// ReplicationModeStrs and the enum variables contains the possible replication
// modes for log replication.
var (
	ReplicationModeStrs = []string{
		pkdb.LogReplicationModeSync,
		pkdb.LogReplicationModeAsync,
		pkdb.LogReplicationUnknown,
	}
	ReplicationModeUnknownEnum, _ = types.ParseEnumName(ReplicationModeStrs, pkdb.LogReplicationUnknown, mysql.DefaultCollationName)
)

// TableLogReplStatusGlobalCols contains column definitions for LR_STATUS_GLOBAL table.
var TableLogReplStatusGlobalCols = []columnInfo{
	{name: "REPLICATION_NAME", tp: mysql.TypeVarchar, flag: mysql.PriKeyFlag | mysql.NotNullFlag, size: 64},
	{name: "REPLICA_CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.NotNullFlag | mysql.UnsignedFlag, size: 21},
	{name: "SOURCE_CLUSTER_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "PROTECTION_MODE", tp: mysql.TypeEnum, enumElems: ProtectionModeStrs, flag: mysql.NotNullFlag, size: 64},
	{name: "DEGRADE_TIMEOUT", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "REPLICATION_STATE", tp: mysql.TypeVarchar, flag: mysql.NotNullFlag, size: 64},
	{name: "REPLICATION_MODE", tp: mysql.TypeEnum, enumElems: ReplicationModeStrs, flag: mysql.NotNullFlag, size: 64},
	{name: "CHECKPOINT_TS", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "CHECKPOINT_TIME", tp: mysql.TypeTimestamp, size: 26},
	{name: "CHECKPOINT_LAG", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "INITIALIZING_PROGRESS", tp: mysql.TypeFloat, size: 12},
	{name: "LAST_HEARTBEAT_TIME", tp: mysql.TypeTimestamp, size: 26},
}

// TableLogReplClusterStatusGlobalCols contains column definitions for LR_CLUSTER_STATUS_GLOBAL table.
var TableLogReplClusterStatusGlobalCols = []columnInfo{
	{name: "CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.PriKeyFlag | mysql.NotNullFlag | mysql.UnsignedFlag, size: 21},
	{name: "SOURCE_CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "ROLE", tp: mysql.TypeEnum, enumElems: ClusterRoleStrs, flag: mysql.NotNullFlag, size: 32},
	{name: "SWITCHOVER_READY", tp: mysql.TypeEnum, enumElems: ReadyStatusStrs, size: 16},
	{name: "FAILOVER_READY", tp: mysql.TypeEnum, enumElems: ReadyStatusStrs, size: 16},
	{name: "REPLICATION_STATE", tp: mysql.TypeVarchar, size: 64},
	{name: "LAST_HEARTBEAT_TIME", tp: mysql.TypeTimestamp, size: 26},
}

// TableLogReplWorkflowHistoryGlobalCols contains column definitions for LR_WORKFLOW_HISTORY_GLOBAL table.
var TableLogReplWorkflowHistoryGlobalCols = []columnInfo{
	{name: "WORKFLOW_ID", tp: mysql.TypeLonglong, flag: mysql.PriKeyFlag | mysql.NotNullFlag | mysql.UnsignedFlag, size: 21},
	{name: "REPLICATION_NAME", tp: mysql.TypeVarchar, flag: mysql.NotNullFlag, size: 64},
	{name: "REPLICA_CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.NotNullFlag | mysql.UnsignedFlag, size: 21},
	{name: "SOURCE_CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.NotNullFlag | mysql.UnsignedFlag, size: 21},
	{name: "WORKFLOW_TYPE", tp: mysql.TypeVarchar, flag: mysql.NotNullFlag, size: 32},
	{name: "WORKFLOW_INFO", tp: mysql.TypeVarchar, size: 65535},
	{name: "START_TIME", tp: mysql.TypeTimestamp, size: 26},
	{name: "END_TIME", tp: mysql.TypeTimestamp, size: 26},
	{name: "WORKFLOW_STATE", tp: mysql.TypeVarchar, flag: mysql.NotNullFlag, size: 32},
	{name: "WORKFLOW_STATE_INFO", tp: mysql.TypeVarchar, size: 65535},
	{name: "INITIATOR_CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag | mysql.NotNullFlag, size: 21},
}

// TableLogReplStatusLocalCols contains column definitions for LR_STATUS_LOCAL table.
var TableLogReplStatusLocalCols = []columnInfo{
	{name: "CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.PriKeyFlag | mysql.NotNullFlag | mysql.UnsignedFlag, size: 21},
	{name: "ROLE", tp: mysql.TypeEnum, enumElems: ClusterRoleStrs, flag: mysql.NotNullFlag, size: 32},
	{name: "HAS_REPLICA", tp: mysql.TypeTiny, flag: mysql.NotNullFlag, size: 1},
	{name: "LAST_GLOBAL_UPDATE", tp: mysql.TypeTimestamp, size: 26},
	{name: "REPLICATION_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "SOURCE_CLUSTER_ID", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "SOURCE_PD_ADDRS", tp: mysql.TypeVarchar, size: 65535},
	{name: "PROTECTION_MODE", tp: mysql.TypeEnum, enumElems: ProtectionModeStrs, size: 64},
	{name: "DEGRADE_TIMEOUT", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "REPLICATION_STATE", tp: mysql.TypeVarchar, size: 64},
	{name: "REPLICATION_MODE", tp: mysql.TypeEnum, enumElems: ReplicationModeStrs, size: 64},
	{name: "CHECKPOINT_TS", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "CHECKPOINT_TIME", tp: mysql.TypeTimestamp, size: 26},
	{name: "CHECKPOINT_LAG", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag, size: 21},
	{name: "SWITCHOVER_READY", tp: mysql.TypeEnum, enumElems: ReadyStatusStrs, size: 16},
	{name: "FAILOVER_READY", tp: mysql.TypeEnum, enumElems: ReadyStatusStrs, size: 16},
	{name: "INITIALIZING_PROGRESS", tp: mysql.TypeFloat, size: 12},
}
