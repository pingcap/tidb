package executor

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/tikv/client-go/v2/oracle"
)

func (e *memtableRetriever) setDataForLogReplStatusGlobal(ctx context.Context, sctx sessionctx.Context) error {
	do := domain.GetDomain(sctx)
	pdCli := do.GetPDClient()

	statuses, err := pdCli.ListLogReplStatuses(ctx)
	if err != nil {
		return err
	}

	rows := make([][]types.Datum, 0, len(statuses))
	for _, status := range statuses {
		row := make([]types.Datum, len(infoschema.TableLogReplStatusGlobalCols))
		row[0].SetString(status.Name, mysql.DefaultCollationName) // NAME
		row[1].SetUint64(status.ReplicaClusterId)                 // REPLICA_CLUSTER_ID
		row[2].SetUint64(status.SourceClusterId)                  // SOURCE_CLUSTER_ID

		var protectionModeStr string
		switch status.GetProtectionMode() {
		case pdpb.ProtectionMode_MaximumPerformance:
			protectionModeStr = "MAXIMUM_PERFORMANCE"
		case pdpb.ProtectionMode_MaximumProtection:
			protectionModeStr = "MAXIMUM_PROTECTION"
		case pdpb.ProtectionMode_MaximumAvailability:
			protectionModeStr = "MAXIMUM_AVAILABILITY"
		default:
			protectionModeStr = "UNKNOWN"
		}
		protectionModeEnum, err := types.ParseEnumName(infoschema.ProtectionModeStrs, protectionModeStr, mysql.DefaultCollationName)
		if err != nil {
			return err
		}
		row[3].SetMysqlEnum(protectionModeEnum, mysql.DefaultCollationName) // PROTECTION_MODE

		if status.GetProtectionMode() == pdpb.ProtectionMode_MaximumAvailability {
			row[4].SetUint64(status.GetDegradeTimeoutSec()) // DEGRADE_TIMEOUT
		}

		row[5].SetString(status.State, mysql.DefaultCollationName) // STATE

		if status.CheckpointTs > 0 {
			row[6].SetUint64(status.CheckpointTs) // CHECKPOINT_TS
			goTime := oracle.GetTimeFromTS(status.CheckpointTs)
			checkpointTime := types.NewTime(types.FromGoTime(goTime), mysql.TypeTimestamp, types.DefaultFsp)
			row[7].SetMysqlTime(checkpointTime)               // CHECKPOINT_TIME
			row[8].SetUint64(uint64(status.CheckpointLagSec)) // CHECKPOINT_LAG
		}

		row[9].SetFloat32(status.InitializingProgress) // INITIALIZING_PROGRESS
		if status.LastHeartbeatTime > 0 {
			goTime := time.Unix(int64(status.LastHeartbeatTime), 0)
			lastHeartbeatTime := types.NewTime(types.FromGoTime(goTime), mysql.TypeTimestamp, types.DefaultFsp)
			row[10].SetMysqlTime(lastHeartbeatTime) // LAST_HEARTBEAT_TIME
		}
		rows = append(rows, row)
	}

	e.rows = rows
	return nil
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
	primaryRow[1].SetNull()                                    // SOURCE_CLUSTER_ID
	roleEnum, _ := types.ParseEnumName(infoschema.ClusterRoleStrs, "PRIMARY", mysql.DefaultCollationName)
	primaryRow[2].SetMysqlEnum(roleEnum, mysql.DefaultCollationName) // ROLE
	primaryRow[3].SetNull()                                          // SWITCHOVER_READY
	primaryRow[4].SetNull()                                          // FAILOVER_READY
	primaryRow[5].SetNull()                                          // LOG_REPLICATION_STATE
	primaryRow[6].SetNull()                                          // LAST_HEARTBEAT_TIME

	rows = append(rows, primaryRow)

	for _, status := range statuses {
		row := make([]types.Datum, len(infoschema.TableLogReplClusterStatusGlobalCols))
		row[0].SetUint64(status.ReplicaClusterId) // CLUSTER_ID
		row[1].SetUint64(status.SourceClusterId)  // SOURCE_CLUSTER_ID

		roleEnum, _ := types.ParseEnumName(infoschema.ClusterRoleStrs, "STANDBY", mysql.DefaultCollationName)
		row[2].SetMysqlEnum(roleEnum, mysql.DefaultCollationName) // ROLE

		switchoverReady := "UNKNOWN"
		if status.SwitchoverReady != "" {
			switchoverReady = status.SwitchoverReady
		}
		switchoverEnum, _ := types.ParseEnumName(infoschema.ReadyStatusStrs, switchoverReady, mysql.DefaultCollationName)
		row[3].SetMysqlEnum(switchoverEnum, mysql.DefaultCollationName) // SWITCHOVER_READY

		failoverReady := "UNKNOWN"
		if status.FailoverReady != "" {
			failoverReady = status.FailoverReady
		}
		failoverEnum, _ := types.ParseEnumName(infoschema.ReadyStatusStrs, failoverReady, mysql.DefaultCollationName)
		row[4].SetMysqlEnum(failoverEnum, mysql.DefaultCollationName) // FAILOVER_READY

		row[5].SetString(status.State, mysql.DefaultCollationName) // LOG_REPLICATION_STATE

		if status.LastHeartbeatTime > 0 {
			goTime := time.Unix(int64(status.LastHeartbeatTime), 0)
			lastHeartbeatTime := types.NewTime(types.FromGoTime(goTime), mysql.TypeTimestamp, types.DefaultFsp)
			row[6].SetMysqlTime(lastHeartbeatTime) // LAST_HEARTBEAT_TIME
		}
		rows = append(rows, row)
	}

	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataForLogReplWorkflowHistoryGlobal(ctx context.Context, sctx sessionctx.Context) error {
	do := domain.GetDomain(sctx)
	pdCli := do.GetPDClient()

	workflows, err := pdCli.ListLogReplWorkflows(ctx)
	if err != nil {
		return err
	}

	rows := make([][]types.Datum, 0, len(workflows))
	for _, wf := range workflows {
		row := make([]types.Datum, len(infoschema.TableLogReplWorkflowHistoryGlobalCols))
		row[0].SetUint64(wf.Id)                                             // WORKFLOW_ID
		row[1].SetString(wf.LogReplicationName, mysql.DefaultCollationName) // LOG_REPLICATION_NAME
		row[2].SetUint64(wf.ReplicaClusterId)                               // REPLICA_CLUSTER_ID
		row[3].SetUint64(wf.SourceClusterId)                                // SOURCE_CLUSTER_ID
		row[4].SetString(wf.Type, mysql.DefaultCollationName)               // WORKFLOW_TYPE
		row[5].SetString(wf.Info, mysql.DefaultCollationName)               // WORKFLOW_INFO
		if wf.StartTime > 0 {
			goTime := time.Unix(int64(wf.StartTime), 0)
			startTime := types.NewTime(types.FromGoTime(goTime), mysql.TypeTimestamp, types.DefaultFsp)
			row[6].SetMysqlTime(startTime) // START_TIME
		}
		if wf.EndTime > 0 {
			goTime := time.Unix(int64(wf.EndTime), 0)
			endTime := types.NewTime(types.FromGoTime(goTime), mysql.TypeTimestamp, types.DefaultFsp)
			row[7].SetMysqlTime(endTime) // END_TIME
		}

		state := wf.State
		row[8].SetString(state, mysql.DefaultCollationName) // WORKFLOW_STATE
		if wf.StateInfo != "" {
			row[9].SetString(wf.StateInfo, mysql.DefaultCollationName) // WORKFLOW_STATE_INFO
		}
		row[10].SetUint64(wf.InitiatorClusterId) // INITIATOR_CLUSTER_ID
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

	isPrimary := status.GetSourceClusterId() == 0
	role := "PRIMARY"
	if !isPrimary {
		role = "STANDBY"
	}
	roleEnum, err := types.ParseEnumName(infoschema.ClusterRoleStrs, role, mysql.DefaultCollationName)
	if err != nil {
		return nil, err
	}
	row[1].SetMysqlEnum(roleEnum, mysql.DefaultCollationName) // ROLE

	if localStatus.GetHasReplica() {
		row[2].SetInt64(1) // HAS_REPLICA
	} else {
		row[2].SetInt64(0) // HAS_REPLICA
	}

	lastGlobalUpdateTime := time.Unix(int64(localStatus.GetLastGlobalUpdateTs()), 0)
	row[3].SetMysqlTime(types.NewTime(types.FromGoTime(lastGlobalUpdateTime), mysql.TypeTimestamp, types.DefaultFsp)) // LAST_GLOBAL_UPDATE

	if isPrimary {
		return row, nil
	}

	row[4].SetString(status.GetName(), mysql.DefaultCollationName)                             // LOG_REPLICATION_NAME
	row[5].SetUint64(status.GetSourceClusterId())                                              // SOURCE_CLUSTER_ID
	row[6].SetString(strings.Join(status.GetSourcePdAddrs(), ","), mysql.DefaultCollationName) // SOURCE_PD_ADDRS

	var protectionModeStr string
	switch status.GetProtectionMode() {
	case pdpb.ProtectionMode_MaximumPerformance:
		protectionModeStr = "MAXIMUM_PERFORMANCE"
	case pdpb.ProtectionMode_MaximumProtection:
		protectionModeStr = "MAXIMUM_PROTECTION"
	case pdpb.ProtectionMode_MaximumAvailability:
		protectionModeStr = "MAXIMUM_AVAILABILITY"
		row[8].SetUint64(status.GetDegradeTimeoutSec()) // DEGRADE_TIMEOUT
	default:
		protectionModeStr = "UNKNOWN"
	}
	protectionModeEnum, err := types.ParseEnumName(infoschema.ProtectionModeStrs, protectionModeStr, mysql.DefaultCollationName)
	if err != nil {
		return nil, err
	}
	row[7].SetMysqlEnum(protectionModeEnum, mysql.DefaultCollationName) // PROTECTION_MODE

	row[9].SetString(status.GetState(), mysql.DefaultCollationName) // LOG_REPLICATION_STATE

	if status.GetCheckpointTs() > 0 {
		row[10].SetUint64(status.GetCheckpointTs()) // CHECKPOINT_TS
		checkpointTime := types.NewTime(types.FromGoTime(oracle.GetTimeFromTS(status.GetCheckpointTs())), mysql.TypeTimestamp, types.DefaultFsp)
		row[11].SetMysqlTime(checkpointTime)                 // CHECKPOINT_TIME
		row[12].SetUint64(uint64(status.GetCheckpointLagSec())) // CHECKPOINT_LAG
	}

	switchoverReady := "UNKNOWN"
	if status.GetSwitchoverReady() != "" {
		switchoverReady = status.GetSwitchoverReady()
	}
	switchoverEnum, err := types.ParseEnumName(infoschema.ReadyStatusStrs, switchoverReady, mysql.DefaultCollationName)
	if err != nil {
		return nil, err
	}
	row[13].SetMysqlEnum(switchoverEnum, mysql.DefaultCollationName) // SWITCHOVER_READY

	failoverReady := "UNKNOWN"
	if status.GetFailoverReady() != "" {
		failoverReady = status.GetFailoverReady()
	}
	failoverEnum, err := types.ParseEnumName(infoschema.ReadyStatusStrs, failoverReady, mysql.DefaultCollationName)
	if err != nil {
		return nil, err
	}
	row[14].SetMysqlEnum(failoverEnum, mysql.DefaultCollationName) // FAILOVER_READY
	row[15].SetFloat32(status.GetInitializingProgress())           // INITIALIZING_PROGRESS

	return row, nil
}
