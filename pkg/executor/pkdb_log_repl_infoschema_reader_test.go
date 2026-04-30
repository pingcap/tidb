// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestBuildLogReplStatusLocalRow(t *testing.T) {
	requireNull := func(t *testing.T, row []types.Datum, idx int) {
		require.Equal(t, types.KindNull, row[idx].Kind())
	}
	requireEnum := func(t *testing.T, row []types.Datum, idx int, name string) {
		require.Equal(t, name, row[idx].GetMysqlEnum().Name)
	}

	t.Run("primary", func(t *testing.T) {
		row, err := buildLogReplStatusLocalRow(&pdpb.LogReplicationLocalStatus{
			Status: &pdpb.LogReplicationStatus{
				ReplicaClusterId: 1001,
			},
			HasReplica:         true,
			LastGlobalUpdateTs: 1710000000,
		})
		require.NoError(t, err)
		require.Len(t, row, 16)
		require.Equal(t, uint64(1001), row[0].GetUint64()) // CLUSTER_ID
		requireEnum(t, row, 1, "PRIMARY")                  // ROLE
		require.Equal(t, int64(1), row[2].GetInt64())      // HAS_REPLICA
		require.Equal(t,
			types.NewTime(types.FromGoTime(time.Unix(1710000000, 0)), mysql.TypeTimestamp, types.DefaultFsp),
			row[3].GetMysqlTime(), // LAST_GLOBAL_UPDATE
		)
		requireNull(t, row, 4)  // LOG_REPLICATION_NAME
		requireNull(t, row, 5)  // SOURCE_CLUSTER_ID
		requireNull(t, row, 6)  // SOURCE_PD_ADDRS
		requireNull(t, row, 7)  // PROTECTION_MODE
		requireNull(t, row, 8)  // DEGRADE_TIMEOUT
		requireNull(t, row, 9)  // LOG_REPLICATION_STATE
		requireNull(t, row, 10) // CHECKPOINT_TS
		requireNull(t, row, 11) // CHECKPOINT_TIME
		requireNull(t, row, 12) // CHECKPOINT_LAG
		requireNull(t, row, 13) // SWITCHOVER_READY
		requireNull(t, row, 14) // FAILOVER_READY
		requireNull(t, row, 15) // INITIALIZING_PROGRESS
	})

	t.Run("standby full status", func(t *testing.T) {
		const checkpointTS = uint64(449123456789000000)
		row, err := buildLogReplStatusLocalRow(&pdpb.LogReplicationLocalStatus{
			Status: &pdpb.LogReplicationStatus{
				Name:                 "dr_east",
				ReplicaClusterId:     1002,
				SourceClusterId:      1001,
				SourcePdAddrs:        []string{"127.0.0.1:2379", "127.0.0.2:2379"},
				State:                "SYNC_REPLICATING",
				ProtectionMode:       pdpb.ProtectionMode_MaximumAvailability,
				DegradeTimeoutSec:    30,
				CheckpointTs:         checkpointTS,
				CheckpointLagSec:     3,
				InitializingProgress: 88.5,
				SwitchoverReady:      "YES",
				FailoverReady:        "NO",
			},
			HasReplica:         false,
			LastGlobalUpdateTs: 1710000001,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(1002), row[0].GetUint64()) // CLUSTER_ID
		requireEnum(t, row, 1, "STANDBY")                  // ROLE
		require.Equal(t, int64(0), row[2].GetInt64())      // HAS_REPLICA
		require.Equal(t, "dr_east", row[4].GetString())
		require.Equal(t, uint64(1001), row[5].GetUint64())
		require.Equal(t, "127.0.0.1:2379,127.0.0.2:2379", row[6].GetString())
		requireEnum(t, row, 7, "MAXIMUM_AVAILABILITY")
		require.Equal(t, uint64(30), row[8].GetUint64())
		require.Equal(t, "SYNC_REPLICATING", row[9].GetString())
		require.Equal(t, checkpointTS, row[10].GetUint64())
		require.Equal(t,
			types.NewTime(types.FromGoTime(oracle.GetTimeFromTS(checkpointTS)), mysql.TypeTimestamp, types.DefaultFsp),
			row[11].GetMysqlTime(),
		)
		require.Equal(t, uint64(3), row[12].GetUint64())
		requireEnum(t, row, 13, "YES")
		requireEnum(t, row, 14, "NO")
		require.Equal(t, float32(88.5), row[15].GetFloat32())
	})

	t.Run("standby empty ready status", func(t *testing.T) {
		row, err := buildLogReplStatusLocalRow(&pdpb.LogReplicationLocalStatus{
			Status: &pdpb.LogReplicationStatus{
				ReplicaClusterId: 1002,
				SourceClusterId:  1001,
				ProtectionMode:   pdpb.ProtectionMode_MaximumPerformance,
			},
		})
		require.NoError(t, err)
		requireEnum(t, row, 7, "MAXIMUM_PERFORMANCE")
		requireNull(t, row, 8) // DEGRADE_TIMEOUT
		requireEnum(t, row, 13, "UNKNOWN")
		requireEnum(t, row, 14, "UNKNOWN")
	})

	t.Run("standby no checkpoint", func(t *testing.T) {
		row, err := buildLogReplStatusLocalRow(&pdpb.LogReplicationLocalStatus{
			Status: &pdpb.LogReplicationStatus{
				ReplicaClusterId: 1002,
				SourceClusterId:  1001,
				CheckpointLagSec: 3,
				ProtectionMode:   pdpb.ProtectionMode_MaximumProtection,
			},
		})
		require.NoError(t, err)
		requireEnum(t, row, 7, "MAXIMUM_PROTECTION")
		requireNull(t, row, 8)  // DEGRADE_TIMEOUT
		requireNull(t, row, 10) // CHECKPOINT_TS
		requireNull(t, row, 11) // CHECKPOINT_TIME
		requireNull(t, row, 12) // CHECKPOINT_LAG
	})

	t.Run("standby unknown checkpoint lag", func(t *testing.T) {
		row, err := buildLogReplStatusLocalRow(&pdpb.LogReplicationLocalStatus{
			Status: &pdpb.LogReplicationStatus{
				ReplicaClusterId: 1002,
				SourceClusterId:  1001,
				CheckpointTs:     449123456789000000,
				CheckpointLagSec: -1,
				ProtectionMode:   pdpb.ProtectionMode_MaximumProtection,
			},
		})
		require.NoError(t, err)
		require.Equal(t, uint64(449123456789000000), row[10].GetUint64())
		require.Equal(t, ^uint64(0), row[12].GetUint64())
	})

	t.Run("unsupported protection mode", func(t *testing.T) {
		_, err := buildLogReplStatusLocalRow(&pdpb.LogReplicationLocalStatus{
			Status: &pdpb.LogReplicationStatus{
				ReplicaClusterId: 1002,
				SourceClusterId:  1001,
				ProtectionMode:   pdpb.ProtectionMode(99),
			},
		})
		require.Error(t, err)
	})
}
