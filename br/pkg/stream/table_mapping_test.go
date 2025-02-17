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
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

var increaseID int64 = 100

func mockGenGlobalID(_ctx context.Context) (int64, error) {
	increaseID++
	return increaseID, nil
}

func TestToProto(t *testing.T) {
	increaseID = 100
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
	tc := NewTableMappingManager(drs, mockGenGlobalID)

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

func TestParseMetaKvAndUpdateIdMapping(t *testing.T) {
	increaseID = 100
	var (
		dbID      int64  = 40
		dbName           = "test_db"
		tableID   int64  = 100
		tableName        = "test_table"
		pt1ID     int64  = 101
		pt2ID     int64  = 102
		pt1Name          = "pt1"
		pt2Name          = "pt2"
		ts        uint64 = 400036290571534337
	)

	// Create a TableMappingManager with empty maps
	tc := NewTableMappingManager(nil, mockGenGlobalID)

	// Test DB key
	dbKey := meta.DBkey(dbID)
	dbInfo := &model.DBInfo{
		ID:   dbID,
		Name: ast.NewCIStr(dbName),
	}
	dbValue, err := json.Marshal(dbInfo)
	require.NoError(t, err)

	// Encode DB key in a transaction
	txnDBKey := encodeTxnMetaKey([]byte("DBs"), dbKey, ts)
	entry := &kv.Entry{
		Key:   txnDBKey,
		Value: dbValue,
	}

	// Test parsing DB key and value
	err = tc.ParseMetaKvAndUpdateIdMapping(entry, DefaultCF)
	require.NoError(t, err)
	require.Contains(t, tc.DbReplaceMap, dbID)
	require.Equal(t, dbName, tc.DbReplaceMap[dbID].Name)
	require.Equal(t, increaseID, tc.DbReplaceMap[dbID].DbID)

	// Test table key
	pi := model.PartitionInfo{
		Enable:      true,
		Definitions: make([]model.PartitionDefinition, 0),
	}
	pi.Definitions = append(pi.Definitions,
		model.PartitionDefinition{
			ID:   pt1ID,
			Name: ast.NewCIStr(pt1Name),
		},
		model.PartitionDefinition{
			ID:   pt2ID,
			Name: ast.NewCIStr(pt2Name),
		},
	)

	tableInfo := &model.TableInfo{
		ID:        tableID,
		Name:      ast.NewCIStr(tableName),
		Partition: &pi,
	}
	tableValue, err := json.Marshal(tableInfo)
	require.NoError(t, err)

	// Encode table key in a transaction
	txnTableKey := encodeTxnMetaKey(meta.DBkey(dbID), meta.TableKey(tableID), ts)
	tableEntry := &kv.Entry{
		Key:   txnTableKey,
		Value: tableValue,
	}

	// Test parsing table key and value
	err = tc.ParseMetaKvAndUpdateIdMapping(tableEntry, DefaultCF)
	require.NoError(t, err)
	require.Contains(t, tc.DbReplaceMap[dbID].TableMap, tableID)
	require.Equal(t, tableName, tc.DbReplaceMap[dbID].TableMap[tableID].Name)

	// Verify partition IDs are mapped
	require.Contains(t, tc.DbReplaceMap[dbID].TableMap[tableID].PartitionMap, pt1ID)
	require.Contains(t, tc.DbReplaceMap[dbID].TableMap[tableID].PartitionMap, pt2ID)

	// Test non-meta key
	nonMetaEntry := &kv.Entry{
		Key:   []byte("not_a_meta_key"),
		Value: []byte("some_value"),
	}
	err = tc.ParseMetaKvAndUpdateIdMapping(nonMetaEntry, DefaultCF)
	require.NoError(t, err)

	// Test auto increment key with different IDs
	autoIncrDBID := int64(50)
	autoIncrTableID := int64(200)
	autoIncrKey := encodeTxnMetaKey(meta.DBkey(autoIncrDBID), meta.AutoIncrementIDKey(autoIncrTableID), ts)
	autoIncrEntry := &kv.Entry{
		Key:   autoIncrKey,
		Value: []byte("1"),
	}
	err = tc.ParseMetaKvAndUpdateIdMapping(autoIncrEntry, DefaultCF)
	require.NoError(t, err)
	require.Contains(t, tc.DbReplaceMap, autoIncrDBID)
	require.Contains(t, tc.DbReplaceMap[autoIncrDBID].TableMap, autoIncrTableID)

	// Test auto table ID key with different IDs
	autoTableDBID := int64(60)
	autoTableTableID := int64(300)
	autoTableKey := encodeTxnMetaKey(meta.DBkey(autoTableDBID), meta.AutoTableIDKey(autoTableTableID), ts)
	autoTableEntry := &kv.Entry{
		Key:   autoTableKey,
		Value: []byte("1"),
	}
	err = tc.ParseMetaKvAndUpdateIdMapping(autoTableEntry, DefaultCF)
	require.NoError(t, err)
	require.Contains(t, tc.DbReplaceMap, autoTableDBID)
	require.Contains(t, tc.DbReplaceMap[autoTableDBID].TableMap, autoTableTableID)

	// Test sequence key with different IDs
	seqDBID := int64(70)
	seqTableID := int64(400)
	seqKey := encodeTxnMetaKey(meta.DBkey(seqDBID), meta.SequenceKey(seqTableID), ts)
	seqEntry := &kv.Entry{
		Key:   seqKey,
		Value: []byte("1"),
	}
	err = tc.ParseMetaKvAndUpdateIdMapping(seqEntry, DefaultCF)
	require.NoError(t, err)
	require.Contains(t, tc.DbReplaceMap, seqDBID)
	require.Contains(t, tc.DbReplaceMap[seqDBID].TableMap, seqTableID)

	// Test auto random table ID key with different IDs
	autoRandomDBID := int64(80)
	autoRandomTableID := int64(500)
	autoRandomKey := encodeTxnMetaKey(meta.DBkey(autoRandomDBID), meta.AutoRandomTableIDKey(autoRandomTableID), ts)
	autoRandomEntry := &kv.Entry{
		Key:   autoRandomKey,
		Value: []byte("1"),
	}
	err = tc.ParseMetaKvAndUpdateIdMapping(autoRandomEntry, DefaultCF)
	require.NoError(t, err)
	require.Contains(t, tc.DbReplaceMap, autoRandomDBID)
	require.Contains(t, tc.DbReplaceMap[autoRandomDBID].TableMap, autoRandomTableID)
}
