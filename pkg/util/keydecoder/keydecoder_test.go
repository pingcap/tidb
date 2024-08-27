// Copyright 2021 PingCAP, Inc.
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

package keydecoder

import (
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	_ "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/assert"
	"go.opencensus.io/stats/view"
)

func TestDecodeKey(t *testing.T) {
	defer view.Stop()
	table.MockTableFromMeta = tables.MockTableFromMeta
	tableInfo1 := &model.TableInfo{
		ID:   1,
		Name: model.NewCIStr("table1"),
		Indices: []*model.IndexInfo{
			{ID: 1, Name: model.NewCIStr("index1"), State: model.StatePublic},
		},
	}
	tableInfo2 := &model.TableInfo{ID: 2, Name: model.NewCIStr("table2")}
	tableInfo3 := &model.TableInfo{
		ID:   3,
		Name: model.NewCIStr("table3"),
		Columns: []*model.ColumnInfo{
			{ID: 10, Name: model.NewCIStr("col"), State: model.StatePublic},
		},
		Indices: []*model.IndexInfo{
			{ID: 4, Name: model.NewCIStr("index4"), State: model.StatePublic},
		},
		Partition: &model.PartitionInfo{
			Type:   model.PartitionTypeRange,
			Expr:   "`col`",
			Enable: true,
			Definitions: []model.PartitionDefinition{
				{ID: 5, Name: model.NewCIStr("p0"), LessThan: []string{"10"}},
				{ID: 6, Name: model.NewCIStr("p1"), LessThan: []string{"MAXVALUE"}},
			},
		},
	}

	stubTableInfos := []*model.TableInfo{tableInfo1, tableInfo2, tableInfo3}
	stubInfoschema := infoschema.MockInfoSchema(stubTableInfos)

	decodedKey, err := DecodeKey([]byte{
		't',
		// table id = 1
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		'_',
		'r',
		// int handle, value = 1
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
	}, stubInfoschema)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), decodedKey.DbID)
	assert.Equal(t, "test", decodedKey.DbName)
	assert.Equal(t, int64(1), decodedKey.TableID)
	assert.Equal(t, "table1", decodedKey.TableName)
	assert.Equal(t, int64(0), decodedKey.PartitionID)
	assert.Equal(t, "", decodedKey.PartitionName)
	assert.Equal(t, IntHandle, decodedKey.HandleType)
	assert.False(t, decodedKey.IsPartitionHandle)
	assert.Equal(t, "1", decodedKey.HandleValue)
	// These are default values, ie. will be omitted when got marshaled into json
	assert.Equal(t, int64(0), decodedKey.IndexID)
	assert.Equal(t, "", decodedKey.IndexName)
	assert.Nil(t, decodedKey.IndexValues)

	ch := testutil.MustNewCommonHandle(t, 100, "abc")
	encodedCommonKey := ch.Encoded()
	key := []byte{
		't',
		// table id = 2
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
		'_',
		'r',
	}
	key = append(key, encodedCommonKey...)

	decodedKey, err = DecodeKey(key, stubInfoschema)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), decodedKey.DbID)
	assert.Equal(t, "test", decodedKey.DbName)
	assert.Equal(t, int64(2), decodedKey.TableID)
	assert.Equal(t, "table2", decodedKey.TableName)
	assert.Equal(t, int64(0), decodedKey.PartitionID)
	assert.Equal(t, "", decodedKey.PartitionName)
	assert.Equal(t, CommonHandle, decodedKey.HandleType)
	assert.False(t, decodedKey.IsPartitionHandle)
	assert.Equal(t, "{100, abc}", decodedKey.HandleValue)
	// These are default values, ie. will be omitted when got marshaled into json
	assert.Equal(t, int64(0), decodedKey.IndexID)
	assert.Equal(t, "", decodedKey.IndexName)
	assert.Nil(t, decodedKey.IndexValues)

	values := types.MakeDatums("abc", 1)
	sc := stmtctx.NewStmtCtx()
	encodedValue, err := codec.EncodeKey(sc.TimeZone(), nil, values...)
	assert.Nil(t, err)
	key = []byte{
		't',
		// table id = 1
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		'_',
		'i',
		// index id = 1
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
	}
	key = append(key, encodedValue...)

	decodedKey, err = DecodeKey(key, stubInfoschema)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), decodedKey.DbID)
	assert.Equal(t, "test", decodedKey.DbName)
	assert.Equal(t, int64(1), decodedKey.TableID)
	assert.Equal(t, "table1", decodedKey.TableName)
	assert.Equal(t, int64(0), decodedKey.PartitionID)
	assert.Equal(t, "", decodedKey.PartitionName)
	assert.Equal(t, int64(1), decodedKey.IndexID)
	assert.Equal(t, "index1", decodedKey.IndexName)
	assert.Equal(t, []string{"abc", "1"}, decodedKey.IndexValues)
	// These are default values, ie. will be omitted when got marshaled into json
	assert.Equal(t, HandleType(""), decodedKey.HandleType)
	assert.Equal(t, "", decodedKey.HandleValue)
	assert.False(t, decodedKey.IsPartitionHandle)

	// Row key in a partitioned table.
	key = []byte("t\x80\x00\x00\x00\x00\x00\x00\x05_r\x80\x00\x00\x00\x00\x00\x00\x0a")
	decodedKey, err = DecodeKey(key, stubInfoschema)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), decodedKey.DbID)
	assert.Equal(t, "test", decodedKey.DbName)
	assert.Equal(t, int64(3), decodedKey.TableID)
	assert.Equal(t, "table3", decodedKey.TableName)
	assert.Equal(t, int64(5), decodedKey.PartitionID)
	assert.Equal(t, "p0", decodedKey.PartitionName)
	assert.Equal(t, IntHandle, decodedKey.HandleType)
	assert.Equal(t, "10", decodedKey.HandleValue)
	// These are default values, ie. will be omitted when got marshaled into json
	assert.Equal(t, int64(0), decodedKey.IndexID)
	assert.Equal(t, "", decodedKey.IndexName)
	assert.Nil(t, decodedKey.IndexValues)
	assert.False(t, decodedKey.IsPartitionHandle)

	// Index key in a partitioned table.
	values = types.MakeDatums("abcde", 2)
	encodedValue, err = codec.EncodeKey(sc.TimeZone(), nil, values...)
	assert.Nil(t, err)
	key = []byte("t\x80\x00\x00\x00\x00\x00\x00\x06_i\x80\x00\x00\x00\x00\x00\x00\x04")
	key = append(key, encodedValue...)

	decodedKey, err = DecodeKey(key, stubInfoschema)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), decodedKey.DbID)
	assert.Equal(t, "test", decodedKey.DbName)
	assert.Equal(t, int64(3), decodedKey.TableID)
	assert.Equal(t, "table3", decodedKey.TableName)
	assert.Equal(t, int64(6), decodedKey.PartitionID)
	assert.Equal(t, "p1", decodedKey.PartitionName)
	assert.Equal(t, int64(4), decodedKey.IndexID)
	assert.Equal(t, "index4", decodedKey.IndexName)
	assert.Equal(t, []string{"abcde", "2"}, decodedKey.IndexValues)
	// These are default values, ie. will be omitted when got marshaled into json
	assert.Equal(t, HandleType(""), decodedKey.HandleType)
	assert.Equal(t, "", decodedKey.HandleValue)
	assert.False(t, decodedKey.IsPartitionHandle)

	// Totally invalid key
	key = []byte("this-is-a-totally-invalidkey")
	decodedKey, err = DecodeKey(key, stubInfoschema)
	assert.NotNil(t, err)
	// "partly" invalid key, i.e. neither record nor index
	key = []byte{
		't',
		// table id = 1
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
	}
	key = append(key, []byte("rest-part-is-invalid")...)
	decodedKey, err = DecodeKey(key, stubInfoschema)
	assert.NotNil(t, err)

	// Table cannot be found in infoschema
	// This is possible when the schema have changed since when the key is get.
	decodedKey, err = DecodeKey([]byte{
		't',
		// table id = 3
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04,
		'_',
		'r',
		// int handle, value = 1
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
	}, stubInfoschema)
	// We should get as much information as we can
	assert.Nil(t, err)
	assert.Equal(t, int64(4), decodedKey.TableID)
	assert.Equal(t, IntHandle, decodedKey.HandleType)
	assert.Equal(t, "1", decodedKey.HandleValue)

	// Rest information are all default value, ie. omitted when got marshaled into json
	assert.Equal(t, int64(0), decodedKey.DbID)
	assert.Equal(t, "", decodedKey.DbName)
	assert.Equal(t, "", decodedKey.TableName)
	assert.Equal(t, int64(0), decodedKey.PartitionID)
	assert.Equal(t, "", decodedKey.PartitionName)
	assert.Equal(t, int64(0), decodedKey.IndexID)
	assert.Equal(t, "", decodedKey.IndexName)
	assert.False(t, decodedKey.IsPartitionHandle)
	assert.Nil(t, decodedKey.IndexValues)
}
