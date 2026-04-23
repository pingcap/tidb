// Copyright 2018 PingCAP, Inc.
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

package chunk_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func RunNewPool(t *testing.T) {
	pool := chunk.ExportedNewPool(1024)
	require.Equal(t, 1024, pool.initCap)
	require.NotNil(t, pool.varLenColPool)
	require.NotNil(t, pool.fixLenColPool4)
	require.NotNil(t, pool.fixLenColPool8)
	require.NotNil(t, pool.fixLenColPool16)
	require.NotNil(t, pool.fixLenColPool40)
}

func RunPoolGetChunk(t *testing.T) {
	initCap := 1024
	pool := chunk.ExportedNewPool(initCap)

	fieldTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeJSON),
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeNewDecimal),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeLonglong),
		// types.NewFieldType(mysql.TypeTimestamp),
		// types.NewFieldType(mysql.TypeDatetime),
	}

	chk := pool.GetChunk(fieldTypes)
	require.NotNil(t, chk)
	require.Len(t, fieldTypes, chk.NumCols())
	require.Nil(t, chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[0]))
	require.Nil(t, chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[1]))
	require.Equal(t, chunk.ExportedGetFixedLen(fieldTypes[2]), len(chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[2])))
	require.Equal(t, chunk.ExportedGetFixedLen(fieldTypes[3]), len(chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[3])))
	require.Equal(t, chunk.ExportedGetFixedLen(fieldTypes[4]), len(chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[4])))
	require.Equal(t, chunk.ExportedGetFixedLen(fieldTypes[5]), len(chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[5])))
	// require.Equal(t, chunk.ExportedGetFixedLen(fieldTypes[6]), len(chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[6])))
	// require.Equal(t, chunk.ExportedGetFixedLen(fieldTypes[7]), len(chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[7])))

	require.Equal(t, initCap*chunk.ExportedGetFixedLen(fieldTypes[2]), cap(chunk.ExportedGetColumnData(chunk.ExportedGetColumns(chk)[2])))
	require.Equal(t, initCap*chunk.ExportedGetFixedLen(fieldTypes[3]), cap(chunk.ExportedGetColumnData(chunk.ExportedGetColumns(chk)[3])))
	require.Equal(t, initCap*chunk.ExportedGetFixedLen(fieldTypes[4]), cap(chunk.ExportedGetColumnData(chunk.ExportedGetColumns(chk)[4])))
	require.Equal(t, initCap*chunk.ExportedGetFixedLen(fieldTypes[5]), cap(chunk.ExportedGetColumnData(chunk.ExportedGetColumns(chk)[5])))
	// require.Equal(t, initCap*chunk.ExportedGetFixedLen(fieldTypes[6]), cap(chunk.ExportedGetColumnData(chunk.ExportedGetColumns(chk)[6])))
	// require.Equal(t, initCap*chunk.ExportedGetFixedLen(fieldTypes[7]), cap(chunk.ExportedGetColumnData(chunk.ExportedGetColumns(chk)[7])))
}

func RunPoolPutChunk(t *testing.T) {
	initCap := 1024
	pool := chunk.ExportedNewPool(initCap)

	fieldTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeJSON),
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeNewDecimal),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeTimestamp),
		types.NewFieldType(mysql.TypeDatetime),
	}

	chk := pool.GetChunk(fieldTypes)
	pool.PutChunk(fieldTypes, chk)
	require.Equal(t, 0, len(chk.columns))
}

func BenchmarkPoolChunkOperation(b *testing.B) {
	pool := chunk.ExportedNewPool(1024)

	fieldTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeJSON),
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeNewDecimal),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeTimestamp),
		types.NewFieldType(mysql.TypeDatetime),
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.PutChunk(fieldTypes, pool.GetChunk(fieldTypes))
		}
	})
}
