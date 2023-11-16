// Copyright 2023 PingCAP, Inc.
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

package testutil

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"sort"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

type MockDataSourceParameters struct {
	DataSchema  *expression.Schema
	GenDataFunc func(row int, typ *types.FieldType) interface{}
	Ndvs        []int  // number of distinct values on columns[i] and zero represents no limit
	Orders      []bool // columns[i] should be ordered if orders[i] is true
	Rows        int    // number of rows the DataSource should output
	Ctx         sessionctx.Context
}

type MockDataSource struct {
	exec.BaseExecutor
	P        MockDataSourceParameters
	GenData  []*chunk.Chunk
	Chunks   []*chunk.Chunk
	ChunkPtr int
}

func (mds *MockDataSource) GenColDatums(col int) (results []interface{}) {
	typ := mds.RetFieldTypes()[col]
	order := false
	if col < len(mds.P.Orders) {
		order = mds.P.Orders[col]
	}
	rows := mds.P.Rows
	NDV := 0
	if col < len(mds.P.Ndvs) {
		NDV = mds.P.Ndvs[col]
	}
	results = make([]interface{}, 0, rows)
	if NDV == 0 {
		if mds.P.GenDataFunc == nil {
			for i := 0; i < rows; i++ {
				results = append(results, mds.RandDatum(typ))
			}
		} else {
			for i := 0; i < rows; i++ {
				results = append(results, mds.P.GenDataFunc(i, typ))
			}
		}
	} else {
		datumSet := make(map[string]bool, NDV)
		datums := make([]interface{}, 0, NDV)
		for len(datums) < NDV {
			d := mds.RandDatum(typ)
			str := fmt.Sprintf("%v", d)
			if datumSet[str] {
				continue
			}
			datumSet[str] = true
			datums = append(datums, d)
		}

		for i := 0; i < rows; i++ {
			results = append(results, datums[rand.Intn(NDV)])
		}
	}

	if order {
		sort.Slice(results, func(i, j int) bool {
			switch typ.GetType() {
			case mysql.TypeLong, mysql.TypeLonglong:
				return results[i].(int64) < results[j].(int64)
			case mysql.TypeDouble:
				return results[i].(float64) < results[j].(float64)
			case mysql.TypeVarString:
				return results[i].(string) < results[j].(string)
			default:
				panic("not implement")
			}
		})
	}

	return
}

func (mds *MockDataSource) RandDatum(typ *types.FieldType) interface{} {
	switch typ.GetType() {
	case mysql.TypeLong, mysql.TypeLonglong:
		return int64(rand.Int())
	case mysql.TypeFloat:
		return rand.Float32()
	case mysql.TypeDouble:
		return rand.Float64()
	case mysql.TypeNewDecimal:
		var d types.MyDecimal
		return d.FromInt(int64(rand.Int()))
	case mysql.TypeVarString:
		buff := make([]byte, 10)
		rand.Read(buff)
		return base64.RawURLEncoding.EncodeToString(buff)
	default:
		panic("not implement")
	}
}

func (mds *MockDataSource) PrepareChunks() {
	mds.Chunks = make([]*chunk.Chunk, len(mds.GenData))
	for i := range mds.Chunks {
		mds.Chunks[i] = mds.GenData[i].CopyConstruct()
	}
	mds.ChunkPtr = 0
}

func (mds *MockDataSource) Next(ctx context.Context, req *chunk.Chunk) error {
	if mds.ChunkPtr >= len(mds.Chunks) {
		req.Reset()
		return nil
	}
	dataChk := mds.Chunks[mds.ChunkPtr]
	dataChk.SwapColumns(req)
	mds.ChunkPtr++
	return nil
}

// MockPhysicalPlan is used to return a specified executor in when build.
// It is mainly used for testing.
type MockPhysicalPlan interface {
	plannercore.PhysicalPlan
	GetExecutor() exec.Executor
}

type MockDataPhysicalPlan struct {
	MockPhysicalPlan
	DataSchema *expression.Schema
	Exec       exec.Executor
}

func (mp *MockDataPhysicalPlan) GetExecutor() exec.Executor {
	return mp.Exec
}

func (mp *MockDataPhysicalPlan) Schema() *expression.Schema {
	return mp.DataSchema
}

func (mp *MockDataPhysicalPlan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		return "mockData_0"
	})
}

func (mp *MockDataPhysicalPlan) ID() int {
	return 0
}

func (mp *MockDataPhysicalPlan) Stats() *property.StatsInfo {
	return nil
}

func (mp *MockDataPhysicalPlan) SelectBlockOffset() int {
	return 0
}

// MemoryUsage of mockDataPhysicalPlan is only for testing
func (mp *MockDataPhysicalPlan) MemoryUsage() (sum int64) {
	return
}

func BuildMockDataPhysicalPlan(ctx sessionctx.Context, srcExec exec.Executor) *MockDataPhysicalPlan {
	return &MockDataPhysicalPlan{
		DataSchema: srcExec.Schema(),
		Exec:       srcExec,
	}
}

func BuildMockDataSource(opt MockDataSourceParameters) *MockDataSource {
	baseExec := exec.NewBaseExecutor(opt.Ctx, opt.DataSchema, 0)
	m := &MockDataSource{baseExec, opt, nil, nil, 0}
	rTypes := exec.RetTypes(m)
	colData := make([][]interface{}, len(rTypes))
	for i := 0; i < len(rTypes); i++ {
		colData[i] = m.GenColDatums(i)
	}

	m.GenData = make([]*chunk.Chunk, (m.P.Rows+m.MaxChunkSize()-1)/m.MaxChunkSize())
	for i := range m.GenData {
		m.GenData[i] = chunk.NewChunkWithCapacity(exec.RetTypes(m), m.MaxChunkSize())
	}

	for i := 0; i < m.P.Rows; i++ {
		idx := i / m.MaxChunkSize()
		retTypes := exec.RetTypes(m)
		for colIdx := 0; colIdx < len(rTypes); colIdx++ {
			switch retTypes[colIdx].GetType() {
			case mysql.TypeLong, mysql.TypeLonglong:
				m.GenData[idx].AppendInt64(colIdx, colData[colIdx][i].(int64))
			case mysql.TypeFloat:
				m.GenData[idx].AppendFloat32(colIdx, colData[colIdx][i].(float32))
			case mysql.TypeDouble:
				m.GenData[idx].AppendFloat64(colIdx, colData[colIdx][i].(float64))
			case mysql.TypeNewDecimal:
				m.GenData[idx].AppendMyDecimal(colIdx, colData[colIdx][i].(*types.MyDecimal))
			case mysql.TypeVarString:
				m.GenData[idx].AppendString(colIdx, colData[colIdx][i].(string))
			default:
				panic("not implement")
			}
		}
	}
	return m
}

func BuildMockDataSourceWithIndex(opt MockDataSourceParameters, index []int) *MockDataSource {
	opt.Orders = make([]bool, len(opt.DataSchema.Columns))
	for _, idx := range index {
		opt.Orders[idx] = true
	}
	return BuildMockDataSource(opt)
}

// markChildrenUsedColsForTest compares each child with the output schema, and mark
// each column of the child is used by output or not.
func markChildrenUsedColsForTest(outputSchema *expression.Schema, childSchemas ...*expression.Schema) (childrenUsed [][]bool) {
	childrenUsed = make([][]bool, 0, len(childSchemas))
	markedOffsets := make(map[int]struct{})
	for _, col := range outputSchema.Columns {
		markedOffsets[col.Index] = struct{}{}
	}
	prefixLen := 0
	for _, childSchema := range childSchemas {
		used := make([]bool, len(childSchema.Columns))
		for i := range childSchema.Columns {
			if _, ok := markedOffsets[prefixLen+i]; ok {
				used[i] = true
			}
		}
		childrenUsed = append(childrenUsed, used)
	}
	for _, child := range childSchemas {
		used := expression.GetUsedList(outputSchema.Columns, child)
		childrenUsed = append(childrenUsed, used)
	}
	return
}
