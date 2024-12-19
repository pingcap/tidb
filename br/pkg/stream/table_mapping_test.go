// Copyright 2024 PingCAP, Inc.
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

package stream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

var increaseID int64 = 100

func mockGenGenGlobalID(_ctx context.Context) (int64, error) {
	increaseID++
	return increaseID, nil
}

func TestToProto(t *testing.T) {
	var (
		dbName, tblName            string       = "db1", "t1"
		oldDBID                    UpstreamID   = 100
		newDBID                    DownstreamID = 200
		oldTblID, oldPID1, oldPID2 UpstreamID   = 101, 102, 103
		newTblID, newPID1, newPID2 DownstreamID = 201, 202, 203
	)

	// create table Replace
	tr := NewTableReplace(tblName, newTblID)
	tr.PartitionMap[oldPID1] = newPID1
	tr.PartitionMap[oldPID2] = newPID2

	dr := NewDBReplace(dbName, newDBID)
	dr.TableMap[oldTblID] = tr

	drs := make(map[UpstreamID]*DBReplace)
	drs[oldDBID] = dr

	// create schemas replace and test ToProto().
	tc := NewTableMappingManager(drs, mockGenGenGlobalID)

	dbMap := tc.ToProto()
	require.Equal(t, len(dbMap), 1)
	require.Equal(t, dbMap[0].Name, dbName)
	require.Equal(t, dbMap[0].IdMap.UpstreamId, oldDBID)
	require.Equal(t, dbMap[0].IdMap.DownstreamId, newDBID)

	tableMap := dbMap[0].Tables
	require.Equal(t, len(tableMap), 1)
	require.Equal(t, tableMap[0].Name, tblName)
	require.Equal(t, tableMap[0].IdMap.UpstreamId, oldTblID)
	require.Equal(t, tableMap[0].IdMap.DownstreamId, newTblID)

	partitionMap := tableMap[0].Partitions
	require.Equal(t, len(partitionMap), 2)

	if partitionMap[0].UpstreamId == oldPID1 {
		require.Equal(t, partitionMap[0].DownstreamId, newPID1)
		require.Equal(t, partitionMap[1].UpstreamId, oldPID2)
		require.Equal(t, partitionMap[1].DownstreamId, newPID2)
	} else {
		require.Equal(t, partitionMap[0].DownstreamId, newPID2)
		require.Equal(t, partitionMap[1].UpstreamId, oldPID1)
		require.Equal(t, partitionMap[1].DownstreamId, newPID1)
	}

	// test FromDBMapProto()
	drs2 := FromDBMapProto(dbMap)
	require.Equal(t, drs2, drs)
}
