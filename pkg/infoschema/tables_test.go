// Copyright 2025 PingCAP, Inc.
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

package infoschema

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestStorageClassTransitionsTable(t *testing.T) {
	cols := tableNameToColumns[TableStorageClassTransitions]
	require.Len(t, cols, 12)
	require.Equal(t, []string{
		"TABLE_SCHEMA", "TABLE_NAME", "TABLE_ID", "PARTITION_NAME", "PARTITION_ID", "DIRECTION",
		"TOTAL_REPLICAS", "COMPLETED_REPLICAS", "PROGRESS", "START_TIME", "DURATION", "LAST_UPDATE_TIME",
	}, columnNames(cols))
	require.Equal(t, mysql.TypeDatetime, cols[9].tp)
	require.Equal(t, 6, cols[9].decimal)
	require.Equal(t, mysql.TypeLonglong, cols[10].tp)
	require.Equal(t, uint(mysql.UnsignedFlag), cols[10].flag)
	require.Equal(t, mysql.TypeDatetime, cols[11].tp)
	require.Equal(t, 6, cols[11].decimal)
	require.Equal(t, DDLOwner, GetClusterTableCopDestination(TableStorageClassTransitions))
}

func columnNames(cols []columnInfo) []string {
	names := make([]string, len(cols))
	for i := range cols {
		names[i] = cols[i].name
	}
	return names
}

func TestIsTiFlashStore(t *testing.T) {
	// Test with TiFlash store
	tiflashStore := &metapb.Store{
		Labels: []*metapb.StoreLabel{
			{Key: placement.EngineLabelKey, Value: placement.EngineLabelTiFlash},
		},
	}
	require.True(t, isTiFlashStore(tiflashStore))

	// Test with non-TiFlash store
	nonTiflashStore := &metapb.Store{
		Labels: []*metapb.StoreLabel{
			{Key: placement.EngineLabelKey, Value: "tikv"},
		},
	}
	require.False(t, isTiFlashStore(nonTiflashStore))

	// Test with empty labels
	emptyStore := &metapb.Store{
		Labels: []*metapb.StoreLabel{},
	}
	require.False(t, isTiFlashStore(emptyStore))

	// Test with multiple labels including TiFlash
	multiLabelStore := &metapb.Store{
		Labels: []*metapb.StoreLabel{
			{Key: "zone", Value: "zone1"},
			{Key: placement.EngineLabelKey, Value: placement.EngineLabelTiFlash},
			{Key: "region", Value: "us-west"},
		},
	}
	require.True(t, isTiFlashStore(multiLabelStore))
}

func TestIsTiFlashWriteNode(t *testing.T) {
	// Test with TiFlash write node
	writeNode := &metapb.Store{
		Labels: []*metapb.StoreLabel{
			{Key: placement.EngineRoleLabelKey, Value: placement.EngineRoleLabelWrite},
		},
	}
	require.True(t, isTiFlashWriteNode(writeNode))

	// Test with non-write node
	nonWriteNode := &metapb.Store{
		Labels: []*metapb.StoreLabel{
			{Key: placement.EngineRoleLabelKey, Value: "read"},
		},
	}
	require.False(t, isTiFlashWriteNode(nonWriteNode))

	// Test with empty labels
	emptyStore := &metapb.Store{
		Labels: []*metapb.StoreLabel{},
	}
	require.False(t, isTiFlashWriteNode(emptyStore))

	// Test with multiple labels including write role
	multiLabelStore := &metapb.Store{
		Labels: []*metapb.StoreLabel{
			{Key: "zone", Value: "zone1"},
			{Key: placement.EngineRoleLabelKey, Value: placement.EngineRoleLabelWrite},
			{Key: "region", Value: "us-west"},
		},
	}
	require.True(t, isTiFlashWriteNode(multiLabelStore))
}
