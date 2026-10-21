package executor

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

func (e *memtableRetriever) setDataForClusterMonitoring(sctx sessionctx.Context) error {
	prometheusAddr, err := infosync.GetPrometheusAddr()
	if err != nil {
		if infosync.ErrPrometheusAddrIsNotSet.Equal(err) {
			sctx.GetSessionVars().StmtCtx.AppendWarning(err)
			return nil
		}
		return err
	}
	row := types.MakeDatums("prometheus", prometheusAddr)
	e.recordMemoryConsume(row)
	e.rows = [][]types.Datum{row}
	return nil
}

func (e *memtableRetriever) setDataForLogReplStatusGlobal(ctx context.Context, sctx sessionctx.Context) error {
	do := domain.GetDomain(sctx)
	pdCli := do.GetPDClient()

	statuses, err := pdCli.ListLogReplStatuses(ctx)
	if err != nil {
		return err
	}

	rows := make([][]types.Datum, 0, len(statuses))
	for _, status := range statuses {
		rows = append(rows, buildLogReplStatusGlobalRow(status))
	}

	e.rows = rows
	return nil
}

func buildLogReplStatusGlobalRow(status *pdpb.LogReplicationStatus) []types.Datum {
	row := make([]types.Datum, len(infoschema.TableLogReplStatusGlobalCols))
	row[0].SetString(status.Name, mysql.DefaultCollationName) // REPLICATION_NAME
	row[1].SetUint64(status.ReplicaClusterId)                 // REPLICA_CLUSTER_ID
	if status.GetReplicaClusterName() != "" {
		row[2].SetString(status.GetReplicaClusterName(), mysql.DefaultCollationName) // REPLICA_CLUSTER_NAME
	}
	row[3].SetUint64(status.SourceClusterId) // SOURCE_CLUSTER_ID
	if status.GetSourceClusterName() != "" {
		row[4].SetString(status.GetSourceClusterName(), mysql.DefaultCollationName) // SOURCE_CLUSTER_NAME
	}

	var protectionModeEnum types.Enum
	switch status.GetProtectionMode() {
	case pdpb.ProtectionMode_MAXIMUM_PERFORMANCE:
		protectionModeEnum = infoschema.ProtectionModeMaximumPerformanceEnum
	case pdpb.ProtectionMode_MAXIMUM_PROTECTION:
		protectionModeEnum = infoschema.ProtectionModeMaximumProtectionEnum
	case pdpb.ProtectionMode_MAXIMUM_AVAILABILITY:
		protectionModeEnum = infoschema.ProtectionModeMaximumAvailabilityEnum
	default:
		protectionModeEnum = infoschema.ProtectionModeMaximumUnknownEnum
	}
	row[5].SetMysqlEnum(protectionModeEnum, mysql.DefaultCollationName) // PROTECTION_MODE

	if status.GetProtectionMode() == pdpb.ProtectionMode_MAXIMUM_AVAILABILITY {
		row[6].SetUint64(status.GetDegradeTimeoutSec()) // DEGRADE_TIMEOUT
	}

	row[7].SetString(status.State, mysql.DefaultCollationName) // REPLICATION_STATE

	replicationModeEnum := infoschema.ReplicationModeUnknownEnum
	if newEnum, err := types.ParseEnumName(infoschema.ReplicationModeStrs, status.ReplicationMode, mysql.DefaultCollationName); err == nil {
		replicationModeEnum = newEnum
	}
	row[8].SetMysqlEnum(replicationModeEnum, mysql.DefaultCollationName) // REPLICATION_MODE

	if status.CheckpointTs > 0 {
		row[9].SetUint64(status.CheckpointTs) // CHECKPOINT_TS
		goTime := oracle.GetTimeFromTS(status.CheckpointTs)
		checkpointTime := types.NewTime(types.FromGoTime(goTime), mysql.TypeTimestamp, types.DefaultFsp)
		row[10].SetMysqlTime(checkpointTime) // CHECKPOINT_TIME
		if status.CheckpointLagSec >= 0 {
			row[11].SetUint64(uint64(status.CheckpointLagSec)) // CHECKPOINT_LAG
		}
	}

	row[12].SetFloat32(status.InitializingProgress) // INITIALIZING_PROGRESS
	if status.LastHeartbeatTime > 0 {
		goTime := time.Unix(int64(status.LastHeartbeatTime), 0)
		lastHeartbeatTime := types.NewTime(types.FromGoTime(goTime), mysql.TypeTimestamp, types.DefaultFsp)
		row[13].SetMysqlTime(lastHeartbeatTime) // LAST_HEARTBEAT_TIME
	}
	return row
}

func (e *memtableRetriever) setDataForLogReplClusterStatusGlobal(ctx context.Context, sctx sessionctx.Context) error {
	do := domain.GetDomain(sctx)
	pdCli := do.GetPDClient()

	statuses, err := pdCli.ListLogReplStatuses(ctx)
	if err != nil {
		return err
	}

	if len(statuses) == 0 {
		e.rows = [][]types.Datum{}
		return nil
	}

	rows := make([][]types.Datum, 0, len(statuses)+1)

	primaryRow := make([]types.Datum, len(infoschema.TableLogReplClusterStatusGlobalCols))
	primaryRow[0].SetUint64(statuses[0].GetPrimaryClusterId()) // CLUSTER_ID
	if statuses[0].GetPrimaryClusterName() != "" {
		primaryRow[1].SetString(statuses[0].GetPrimaryClusterName(), mysql.DefaultCollationName) // CLUSTER_NAME
	}
	primaryRow[2].SetNull() // SOURCE_CLUSTER_ID
	primaryRow[3].SetNull() // SOURCE_CLUSTER_NAME
	roleEnum := infoschema.ClusterRolePrimaryEnum
	primaryRow[4].SetMysqlEnum(roleEnum, mysql.DefaultCollationName) // ROLE
	primaryRow[5].SetNull()                                          // SWITCHOVER_READY
	primaryRow[6].SetNull()                                          // FAILOVER_READY
	primaryRow[7].SetNull()                                          // LOG_REPLICATION_STATE
	primaryRow[8].SetNull()                                          // LAST_HEARTBEAT_TIME
	rows = append(rows, primaryRow)

	for _, status := range statuses {
		row := make([]types.Datum, len(infoschema.TableLogReplClusterStatusGlobalCols))
		row[0].SetUint64(status.ReplicaClusterId) // CLUSTER_ID
		if status.GetReplicaClusterName() != "" {
			row[1].SetString(status.GetReplicaClusterName(), mysql.DefaultCollationName) // CLUSTER_NAME
		}
		row[2].SetUint64(status.SourceClusterId) // SOURCE_CLUSTER_ID
		if status.GetSourceClusterName() != "" {
			row[3].SetString(status.GetSourceClusterName(), mysql.DefaultCollationName) // SOURCE_CLUSTER_NAME
		}

		row[4].SetMysqlEnum(infoschema.ClusterRoleStandbyEnum, mysql.DefaultCollationName) // ROLE

		switchoverEnum := infoschema.ReadyStatusUnknownEnum
		if newEnum, err := types.ParseEnumName(infoschema.ReadyStatusStrs, status.SwitchoverReady, mysql.DefaultCollationName); err == nil {
			switchoverEnum = newEnum
		}
		row[5].SetMysqlEnum(switchoverEnum, mysql.DefaultCollationName) // SWITCHOVER_READY

		failoverEnum := infoschema.ReadyStatusUnknownEnum
		if newEnum, err := types.ParseEnumName(infoschema.ReadyStatusStrs, status.FailoverReady, mysql.DefaultCollationName); err == nil {
			failoverEnum = newEnum
		}
		row[6].SetMysqlEnum(failoverEnum, mysql.DefaultCollationName) // FAILOVER_READY

		row[7].SetString(status.State, mysql.DefaultCollationName) // REPLICATION_STATE

		if status.LastHeartbeatTime > 0 {
			goTime := time.Unix(int64(status.LastHeartbeatTime), 0)
			lastHeartbeatTime := types.NewTime(types.FromGoTime(goTime), mysql.TypeTimestamp, types.DefaultFsp)
			row[8].SetMysqlTime(lastHeartbeatTime) // LAST_HEARTBEAT_TIME
		}
		rows = append(rows, row)
	}

	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataForLogReplWorkflowHistoryGlobal(ctx context.Context, sctx sessionctx.Context) error {
	do := domain.GetDomain(sctx)
	pdCli := do.GetPDClient()

	opts := pd.ListLogReplWorkflowsOptions{}
	if extractor, ok := e.extractor.(*plannercore.LogReplWorkflowHistoryGlobalExtractor); ok {
		if extractor.SkipRequest {
			e.rows = nil
			return nil
		}
		opts.StartWorkflowID = extractor.StartWorkflowID
		opts.EndWorkflowID = extractor.EndWorkflowID
		opts.Limit = extractor.Limit
	}

	workflows, err := pdCli.ListLogReplWorkflows(ctx, opts)
	if err != nil {
		return err
	}

	rows := make([][]types.Datum, 0, len(workflows))
	for _, wf := range workflows {
		row := make([]types.Datum, len(infoschema.TableLogReplWorkflowHistoryGlobalCols))
		row[0].SetUint64(wf.Id)                                             // WORKFLOW_ID
		row[1].SetString(wf.LogReplicationName, mysql.DefaultCollationName) // REPLICATION_NAME
		row[2].SetUint64(wf.ReplicaClusterId)                               // REPLICA_CLUSTER_ID
		if wf.GetReplicaClusterName() != "" {
			row[3].SetString(wf.GetReplicaClusterName(), mysql.DefaultCollationName) // REPLICA_CLUSTER_NAME
		}
		row[4].SetUint64(wf.SourceClusterId) // SOURCE_CLUSTER_ID
		if wf.GetSourceClusterName() != "" {
			row[5].SetString(wf.GetSourceClusterName(), mysql.DefaultCollationName) // SOURCE_CLUSTER_NAME
		}
		row[6].SetString(wf.Type, mysql.DefaultCollationName) // WORKFLOW_TYPE
		row[7].SetString(wf.Info, mysql.DefaultCollationName) // WORKFLOW_INFO
		if wf.StartTime > 0 {
			goTime := time.Unix(int64(wf.StartTime), 0)
			startTime := types.NewTime(types.FromGoTime(goTime), mysql.TypeTimestamp, types.DefaultFsp)
			row[8].SetMysqlTime(startTime) // START_TIME
		}
		if wf.EndTime > 0 {
			goTime := time.Unix(int64(wf.EndTime), 0)
			endTime := types.NewTime(types.FromGoTime(goTime), mysql.TypeTimestamp, types.DefaultFsp)
			row[9].SetMysqlTime(endTime) // END_TIME
		}

		state := wf.State
		row[10].SetString(state, mysql.DefaultCollationName) // WORKFLOW_STATE
		if wf.StateInfo != "" {
			row[11].SetString(wf.StateInfo, mysql.DefaultCollationName) // WORKFLOW_STATE_INFO
		}
		row[12].SetUint64(wf.InitiatorClusterId) // INITIATOR_CLUSTER_ID
		if wf.GetInitiatorClusterName() != "" {
			row[13].SetString(wf.GetInitiatorClusterName(), mysql.DefaultCollationName) // INITIATOR_CLUSTER_NAME
		}

		rows = append(rows, row)
	}

	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataForLogReplStatusLocal(ctx context.Context, sctx sessionctx.Context) error {
	do := domain.GetDomain(sctx)
	pdCli := do.GetPDClient()

	localStatus, err := pdCli.GetLogReplLocalStatus(ctx)
	if err != nil {
		return err
	}

	row, err := buildLogReplStatusLocalRow(localStatus)
	if err != nil {
		return err
	}
	e.rows = [][]types.Datum{row}
	return nil
}

func buildLogReplStatusLocalRow(localStatus *pdpb.LogReplicationLocalStatus) ([]types.Datum, error) {
	status := localStatus.GetStatus()
	row := make([]types.Datum, len(infoschema.TableLogReplStatusLocalCols))

	row[0].SetUint64(status.GetReplicaClusterId()) // CLUSTER_ID
	if status.GetReplicaClusterName() != "" {
		row[1].SetString(status.GetReplicaClusterName(), mysql.DefaultCollationName) // CLUSTER_NAME
	}

	isPrimary := status.GetSourceClusterId() == 0
	hasReplica := localStatus.GetHasReplica()
	var roleEnum types.Enum
	if !isPrimary {
		roleEnum = infoschema.ClusterRoleStandbyEnum
	} else if hasReplica {
		roleEnum = infoschema.ClusterRolePrimaryEnum
	} else {
		roleEnum = infoschema.ClusterRoleStandardEnum
	}
	row[2].SetMysqlEnum(roleEnum, mysql.DefaultCollationName) // ROLE

	if localStatus.GetHasReplica() {
		row[3].SetInt64(1) // HAS_REPLICA
	} else {
		row[3].SetInt64(0) // HAS_REPLICA
	}

	lastGlobalUpdateTime := time.Unix(int64(localStatus.GetLastGlobalUpdateTs()), 0)
	row[4].SetMysqlTime(types.NewTime(types.FromGoTime(lastGlobalUpdateTime), mysql.TypeTimestamp, types.DefaultFsp)) // LAST_GLOBAL_UPDATE

	if isPrimary {
		return row, nil
	}

	row[5].SetString(status.GetName(), mysql.DefaultCollationName) // REPLICATION_NAME
	row[6].SetUint64(status.GetSourceClusterId())                  // SOURCE_CLUSTER_ID
	if status.GetSourceClusterName() != "" {
		row[7].SetString(status.GetSourceClusterName(), mysql.DefaultCollationName) // SOURCE_CLUSTER_NAME
	}
	row[8].SetString(strings.Join(status.GetSourcePdAddrs(), ","), mysql.DefaultCollationName) // SOURCE_PD_ADDRS

	var protectionModeEnum types.Enum
	switch status.GetProtectionMode() {
	case pdpb.ProtectionMode_MAXIMUM_PERFORMANCE:
		protectionModeEnum = infoschema.ProtectionModeMaximumPerformanceEnum
	case pdpb.ProtectionMode_MAXIMUM_PROTECTION:
		protectionModeEnum = infoschema.ProtectionModeMaximumProtectionEnum
	case pdpb.ProtectionMode_MAXIMUM_AVAILABILITY:
		protectionModeEnum = infoschema.ProtectionModeMaximumAvailabilityEnum
		row[10].SetUint64(status.GetDegradeTimeoutSec()) // DEGRADE_TIMEOUT
	default:
		protectionModeEnum = infoschema.ProtectionModeMaximumUnknownEnum
	}
	row[9].SetMysqlEnum(protectionModeEnum, mysql.DefaultCollationName) // PROTECTION_MODE

	row[11].SetString(status.GetState(), mysql.DefaultCollationName) // REPLICATION_STATE

	// REPLICATION_MODE
	replicationModeEnum := infoschema.ReplicationModeUnknownEnum
	if newEnum, err := types.ParseEnumName(infoschema.ReplicationModeStrs, status.GetReplicationMode(), mysql.DefaultCollationName); err == nil {
		replicationModeEnum = newEnum
	}
	row[12].SetMysqlEnum(replicationModeEnum, mysql.DefaultCollationName)

	if status.GetCheckpointTs() > 0 {
		row[13].SetUint64(status.GetCheckpointTs()) // CHECKPOINT_TS
		checkpointTime := types.NewTime(types.FromGoTime(oracle.GetTimeFromTS(status.GetCheckpointTs())), mysql.TypeTimestamp, types.DefaultFsp)
		row[14].SetMysqlTime(checkpointTime) // CHECKPOINT_TIME
		if status.GetCheckpointLagSec() >= 0 {
			row[15].SetUint64(uint64(status.GetCheckpointLagSec())) // CHECKPOINT_LAG
		}
	}

	switchoverEnum := infoschema.ReadyStatusUnknownEnum
	if newEnum, err := types.ParseEnumName(infoschema.ReadyStatusStrs, status.GetSwitchoverReady(), mysql.DefaultCollationName); err == nil {
		switchoverEnum = newEnum
	}
	row[16].SetMysqlEnum(switchoverEnum, mysql.DefaultCollationName) // SWITCHOVER_READY

	failoverEnum := infoschema.ReadyStatusUnknownEnum
	if newEnum, err := types.ParseEnumName(infoschema.ReadyStatusStrs, status.GetFailoverReady(), mysql.DefaultCollationName); err == nil {
		failoverEnum = newEnum
	}
	row[17].SetMysqlEnum(failoverEnum, mysql.DefaultCollationName) // FAILOVER_READY
	row[18].SetFloat32(status.GetInitializingProgress())           // INITIALIZING_PROGRESS

	return row, nil
}
