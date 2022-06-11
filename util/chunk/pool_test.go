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

package chunk

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestNewPool(t *testing.T) {
	pool := NewPool(1024)
	require.Equal(t, 1024, pool.initCap)
	require.NotNil(t, pool.varLenColPool)
	require.NotNil(t, pool.fixLenColPool4)
	require.NotNil(t, pool.fixLenColPool8)
	require.NotNil(t, pool.fixLenColPool16)
	require.NotNil(t, pool.fixLenColPool40)
}

func TestPoolGetChunk(t *testing.T) {
	initCap := 1024
	pool := NewPool(initCap)

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
	require.Nil(t, chk.columns[0].elemBuf)
	require.Nil(t, chk.columns[1].elemBuf)
	require.Equal(t, getFixedLen(fieldTypes[2]), len(chk.columns[2].elemBuf))
	require.Equal(t, getFixedLen(fieldTypes[3]), len(chk.columns[3].elemBuf))
	require.Equal(t, getFixedLen(fieldTypes[4]), len(chk.columns[4].elemBuf))
	require.Equal(t, getFixedLen(fieldTypes[5]), len(chk.columns[5].elemBuf))
	// require.Equal(t, getFixedLen(fieldTypes[6]), len(chk.columns[6].elemBuf))
	// require.Equal(t, getFixedLen(fieldTypes[7]), len(chk.columns[7].elemBuf))

	require.Equal(t, initCap*getFixedLen(fieldTypes[2]), cap(chk.columns[2].data))
	require.Equal(t, initCap*getFixedLen(fieldTypes[3]), cap(chk.columns[3].data))
	require.Equal(t, initCap*getFixedLen(fieldTypes[4]), cap(chk.columns[4].data))
	require.Equal(t, initCap*getFixedLen(fieldTypes[5]), cap(chk.columns[5].data))
	// require.Equal(t, initCap*getFixedLen(fieldTypes[6]), cap(chk.columns[6].data))
	// require.Equal(t, initCap*getFixedLen(fieldTypes[7]), cap(chk.columns[7].data))
}

func TestPoolPutChunk(t *testing.T) {
	initCap := 1024
	pool := NewPool(initCap)

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
	pool := NewPool(1024)

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
