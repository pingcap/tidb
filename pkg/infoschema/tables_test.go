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

func TestIssue62639(t *testing.T) {
	// prepare a database table for testing
	colInfo := []columnInfo{  
        {name: "COL1", tp: mysql.TypeVarchar, size: 64},  
        {name: "COL2", tp: mysql.TypeLong, size: 11},  
        {name: "COL3", tp: mysql.TypeDatetime, size: 19},  
    }

	tableInfo := buildTableMeta("TEST_TABLE", colInfo)

	// Validate that column IDs start from 1 (not 0) and are sequential  
	require.Equal(t, int64(1), tableInfo.Columns[0].ID)  
	require.Equal(t, int64(2), tableInfo.Columns[1].ID)  
	require.Equal(t, int64(3), tableInfo.Columns[2].ID)  
	  
	// Also validate column names are preserved correctly  
	require.Equal(t, "COL1", tableInfo.Columns[0].Name.O)  
	require.Equal(t, "COL2", tableInfo.Columns[1].Name.O)  
	require.Equal(t, "COL3", tableInfo.Columns[2].Name.O)  
}