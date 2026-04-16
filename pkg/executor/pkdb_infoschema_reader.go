package executor

import (
	"context"
	"sort"
	"strconv"
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
			row[7].SetMysqlTime(checkpointTime)            // CHECKPOINT_TIME
			row[8].SetUint64(uint64(status.CheckpointLag)) // CHECKPOINT_LAG
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
		if state == "" {
			// Currently, PD does not maintain states for completed workflows and
			// may not set correct states for in-progress workflows.
			state = "COMPLETED"
			if wf.EndTime == 0 {
				state = "CANCELLED"
			}
		}
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
	status := localStatus.GetStatus()

	kvs := make(map[string]string)
	kvs["cluster_id"] = strconv.FormatUint(status.GetReplicaClusterId(), 10)

	role := "PRIMARY"
	if status.GetSourceClusterId() != 0 {
		role = "STANDBY"
		kvs["source_cluster_id"] = strconv.FormatUint(status.GetSourceClusterId(), 10)
		kvs["source_pd_addrs"] = strings.Join(status.GetSourcePdAddrs(), ",")
		kvs["log_replication_name"] = status.GetName()
		kvs["log_replication_state"] = status.GetState()
		kvs["switchover_ready"] = status.GetSwitchoverReady()
		kvs["failover_ready"] = status.GetFailoverReady()
		if status.GetCheckpointTs() > 0 {
			kvs["checkpoint_ts"] = strconv.FormatUint(status.GetCheckpointTs(), 10)
			kvs["checkpoint_lag"] = (time.Second * time.Duration(status.GetCheckpointLag())).String()
		} else {
			kvs["checkpoint_ts"] = "NULL"
			kvs["checkpoint_lag"] = "NULL"
		}
		kvs["initializing_progress"] = strconv.FormatFloat(float64(status.GetInitializingProgress()), 'f', -1, 32)

		kvs["degrade_timeout"] = "NULL"
		switch status.GetProtectionMode() {
		case pdpb.ProtectionMode_MaximumPerformance:
			kvs["protection_mode"] = "MAXIMUM_PERFORMANCE"
		case pdpb.ProtectionMode_MaximumProtection:
			kvs["protection_mode"] = "MAXIMUM_PROTECTION"
		case pdpb.ProtectionMode_MaximumAvailability:
			kvs["protection_mode"] = "MAXIMUM_AVAILABILITY"
			kvs["degrade_timeout"] = (time.Duration(status.GetDegradeTimeoutSec()) * time.Second).String()
		default:
			kvs["protection_mode"] = "UNKNOWN"
		}
	}
	kvs["role"] = role
	kvs["has_replica"] = strconv.FormatBool(localStatus.GetHasReplica())
	kvs["last_global_update"] = time.Unix(int64(localStatus.GetLastGlobalUpdateTs()), 0).Format(time.DateTime)

	// Sort by keys to have a deterministic order
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	rows := make([][]types.Datum, 0, len(kvs))
	for _, k := range keys {
		row := make([]types.Datum, len(infoschema.TableLogReplStatusLocalCols))
		row[0].SetString(k, mysql.DefaultCollationName)      // CONFIG_KEY
		row[1].SetString(kvs[k], mysql.DefaultCollationName) // CONFIG_VALUE
		rows = append(rows, row)
	}

	e.rows = rows
	return nil
}
