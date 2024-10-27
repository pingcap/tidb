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
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"math"
	"math/big"
	mathrand "math/rand"
	"sort"
	"strconv"
	"sync/atomic"
	"unicode/utf8"
	"unsafe"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/serialization"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

// MockDataSourceParameters mpcks data source parameters
type MockDataSourceParameters struct {
	Ctx         sessionctx.Context
	DataSchema  *expression.Schema
	GenDataFunc func(row int, typ *types.FieldType) any
	Ndvs        []int
	Orders      []bool

	// Sometimes, user wants to manually provide test data
	// and he can save provided test data at here.
	Datums [][]any

	Rows   int
	HasSel bool
}

// MockDataSource mocks data source
type MockDataSource struct {
	GenData []*chunk.Chunk
	Chunks  []*chunk.Chunk
	P       MockDataSourceParameters
	exec.BaseExecutor
	ChunkPtr int
}

// GenColDatums get column datums
func (mds *MockDataSource) GenColDatums(col int) (results []any) {
	typ := mds.RetFieldTypes()[col]
	order := false
	if col < len(mds.P.Orders) {
		order = mds.P.Orders[col]
	}
	rows := mds.P.Rows
	ndv := 0
	if col < len(mds.P.Ndvs) {
		ndv = mds.P.Ndvs[col]
	}
	results = make([]any, 0, rows)

	if ndv == 0 {
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
		datums := make([]any, 0, max(ndv, 0))
		if ndv == -1 {
			if mds.P.Datums[col] == nil {
				panic("need to provid data")
			}

			datums = mds.P.Datums[col]
		} else {
			datumSet := make(map[string]bool, ndv)
			for len(datums) < ndv {
				d := mds.RandDatum(typ)
				str := fmt.Sprintf("%v", d)
				if datumSet[str] {
					continue
				}
				datumSet[str] = true
				datums = append(datums, d)
			}
		}

		for i := 0; i < rows; i++ {
			val, err := rand.Int(rand.Reader, big.NewInt(int64(len(datums))))
			if err != nil {
				panic("Fail to generate int number")
			}
			results = append(results, datums[val.Int64()])
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

// RandDatum rand datum
func (*MockDataSource) RandDatum(typ *types.FieldType) any {
	val, _ := rand.Int(rand.Reader, big.NewInt(1000000))
	switch typ.GetType() {
	case mysql.TypeLong, mysql.TypeLonglong:
		return val.Int64()
	case mysql.TypeFloat:
		floatVal, _ := val.Float64()
		return float32(floatVal / 1000)
	case mysql.TypeDouble:
		floatVal, _ := val.Float64()
		return floatVal / 1000
	case mysql.TypeNewDecimal:
		var d types.MyDecimal
		return d.FromInt(val.Int64())
	case mysql.TypeVarString:
		buff := make([]byte, 10)
		_, err := rand.Read(buff)
		if err != nil {
			panic("rand.Read returns error")
		}
		return base64.RawURLEncoding.EncodeToString(buff)
	default:
		panic("not implement")
	}
}

// PrepareChunks prepares chunks
func (mds *MockDataSource) PrepareChunks() {
	mds.Chunks = make([]*chunk.Chunk, len(mds.GenData))
	for i := range mds.Chunks {
		mds.Chunks[i] = mds.GenData[i].CopyConstruct()
	}
	mds.ChunkPtr = 0
}

// Next get next chunk
func (mds *MockDataSource) Next(_ context.Context, req *chunk.Chunk) error {
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

// MockDataPhysicalPlan mocks physical plan
type MockDataPhysicalPlan struct {
	MockPhysicalPlan
	DataSchema *expression.Schema
	Exec       exec.Executor
}

// GetExecutor gets executor
func (mp *MockDataPhysicalPlan) GetExecutor() exec.Executor {
	return mp.Exec
}

// Schema returns schema
func (mp *MockDataPhysicalPlan) Schema() *expression.Schema {
	return mp.DataSchema
}

// ExplainID returns explain id
func (*MockDataPhysicalPlan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		return "mockData_0"
	})
}

// ID returns 0
func (*MockDataPhysicalPlan) ID() int {
	return 0
}

// Stats returns nil
func (*MockDataPhysicalPlan) Stats() *property.StatsInfo {
	return nil
}

// QueryBlockOffset returns 0
func (*MockDataPhysicalPlan) QueryBlockOffset() int {
	return 0
}

// MemoryUsage of mockDataPhysicalPlan is only for testing
func (*MockDataPhysicalPlan) MemoryUsage() (sum int64) {
	return
}

// BuildMockDataPhysicalPlan builds MockDataPhysicalPlan
func BuildMockDataPhysicalPlan(_ sessionctx.Context, srcExec exec.Executor) *MockDataPhysicalPlan {
	return &MockDataPhysicalPlan{
		DataSchema: srcExec.Schema(),
		Exec:       srcExec,
	}
}

// BuildMockDataSource builds MockDataSource
func BuildMockDataSource(opt MockDataSourceParameters) *MockDataSource {
	baseExec := exec.NewBaseExecutor(opt.Ctx, opt.DataSchema, 0)
	m := &MockDataSource{
		BaseExecutor: baseExec,
		ChunkPtr:     0,
		P:            opt,
		GenData:      nil,
		Chunks:       nil,
	}
	rTypes := exec.RetTypes(m)
	colData := make([][]any, len(rTypes))
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

	if opt.HasSel {
		for _, chk := range m.GenData {
			rowNum := chk.NumRows()
			sel := make([]int, 0, rowNum/2)
			for i := mathrand.Int31n(2); int(i) < rowNum; i += 2 {
				sel = append(sel, int(i))
			}
			chk.SetSel(sel)
		}
	}

	return m
}

// BuildMockDataSourceWithIndex builds MockDataSourceWithIndex
func BuildMockDataSourceWithIndex(opt MockDataSourceParameters, index []int) *MockDataSource {
	opt.Orders = make([]bool, len(opt.DataSchema.Columns))
	for _, idx := range index {
		opt.Orders[idx] = true
	}
	return BuildMockDataSource(opt)
}

// MockActionOnExceed is for test.
type MockActionOnExceed struct {
	memory.BaseOOMAction
	triggeredNum atomic.Int32
}

// Action add the triggered number.
func (m *MockActionOnExceed) Action(*memory.Tracker) {
	m.triggeredNum.Add(1)
}

// GetPriority get the priority of the Action.
func (*MockActionOnExceed) GetPriority() int64 {
	return memory.DefLogPriority
}

// GetTriggeredNum get the triggered number of the Action
func (m *MockActionOnExceed) GetTriggeredNum() int {
	return int(m.triggeredNum.Load())
}

func genRandomColumn(col *chunk.Column, fieldType *types.FieldType, size int) {
	isNullable := !mysql.HasNotNullFlag(fieldType.GetFlag())
	stringSample := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ;.,!@#$%^&*()-=+_平凯数据库"
	setOrEnumSample := []string{"abc", "bcd", "cde", "def", "efg", "fgh"}
	maxDayArray := []int64{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	genRandomInt := func(upBound int64) int64 {
		return mathrand.Int63n(upBound)
	}
	handleNull := func(col *chunk.Column) bool {
		if isNullable {
			n := genRandomInt(math.MaxInt16)
			if n%10 == 0 {
				col.AppendNull()
				return true
			}
		}
		return false
	}
	getUpBound := func(fieldType *types.FieldType) int64 {
		switch fieldType.GetType() {
		case mysql.TypeLonglong:
			return math.MaxInt64
		case mysql.TypeShort:
			return math.MaxInt16
		case mysql.TypeInt24:
			return (1 << 23) - 1
		case mysql.TypeTiny:
			return math.MaxInt8
		case mysql.TypeLong:
			return math.MaxInt32
		default:
			return math.MaxInt16
		}
	}
	switch fieldType.GetType() {
	case mysql.TypeLonglong, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeTiny:
		upBound := getUpBound(fieldType)
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			n := genRandomInt(upBound)
			if mysql.HasUnsignedFlag(fieldType.GetFlag()) {
				un := uint64(n)
				un *= 2
				col.AppendUint64(un)
			} else {
				if n%3 == 0 {
					col.AppendInt64(n)
				} else {
					col.AppendInt64(-n)
				}
			}
		}
	case mysql.TypeYear:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			n := genRandomInt(255)
			col.AppendInt64(1901 + n)
		}
	case mysql.TypeFloat:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			f := float32(mathrand.NormFloat64())
			col.AppendFloat32(f)
		}
	case mysql.TypeDouble:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			col.AppendFloat64(mathrand.NormFloat64())
		}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		buf := make([]byte, 0)
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			buf = buf[:0]
			length := int(genRandomInt(1024))
			offset := int(genRandomInt(int64(utf8.RuneCountInString(stringSample))))
			runeArray := []rune(stringSample)
			runeLength := len(runeArray)
			if offset+length < len(runeArray) {
				buf = append(buf, []byte(string(runeArray[offset:offset+length]))...)
			} else {
				buf = append(buf, []byte(string(runeArray[offset:runeLength]))...)
				length -= runeLength - offset
				for length > runeLength {
					buf = append(buf, []byte(stringSample)...)
					length -= runeLength
				}
				if length > 0 {
					buf = append(buf, []byte(string(runeArray[:length]))...)
				}
			}
			col.AppendBytes(buf)
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			year := int(genRandomInt(200) + 1900)
			month := int(genRandomInt(12) + 1)
			day := int(genRandomInt(maxDayArray[month-1]))
			hour := 0
			minute := 0
			second := 0
			microSecond := 0
			if fieldType.GetType() != mysql.TypeDate {
				hour = int(genRandomInt(24))
				minute = int(genRandomInt(60))
				second = int(genRandomInt(60))
				microSecond = int(genRandomInt(1000000))
			}
			coreTime := types.FromDate(year, month, day, hour, minute, second, microSecond)
			var time types.Time
			time.SetCoreTime(coreTime)
			col.AppendTime(time)
		}
	case mysql.TypeDuration:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			hour := genRandomInt(839)
			minute := genRandomInt(60)
			second := genRandomInt(60)
			microSecond := genRandomInt(1000000)
			duration := types.NewDuration(int(hour), int(minute), int(second), int(microSecond), 6)
			if hour*minute%3 == 0 {
				col.AppendDuration(duration.Neg())
			} else {
				col.AppendDuration(duration)
			}
		}
	case mysql.TypeNewDecimal:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			base := genRandomInt(math.MaxInt32)
			divide := genRandomInt(10) + 1
			isNeg := genRandomInt(math.MaxInt16)%2 == 1
			if isNeg {
				base = -base
			}
			value := float64(base) / float64(divide)
			decimalValue := &types.MyDecimal{}
			err := decimalValue.FromString([]byte(strconv.FormatFloat(value, 'f', -1, 32)))
			if err != nil {
				panic("should not reach here")
			}
			col.AppendMyDecimal(decimalValue)
		}
	case mysql.TypeEnum:
		fieldType.SetElems(setOrEnumSample)
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			n := genRandomInt(int64(len(setOrEnumSample)))
			col.AppendEnum(types.Enum{Name: setOrEnumSample[n], Value: uint64(n)})
		}
	case mysql.TypeSet:
		fieldType.SetElems(setOrEnumSample)
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			n := genRandomInt(int64(len(setOrEnumSample)))
			col.AppendSet(types.Set{Name: setOrEnumSample[n], Value: uint64(n)})
		}
	case mysql.TypeBit:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			n := genRandomInt(math.MaxInt64)
			col.AppendBytes(unsafe.Slice((*byte)(unsafe.Pointer(&n)), serialization.Uint64Len))
		}
	case mysql.TypeJSON:
		for i := 0; i < size; i++ {
			if handleNull(col) {
				continue
			}
			n := genRandomInt(math.MaxInt16)
			switch n % 6 {
			case 0:
				col.AppendJSON(types.CreateBinaryJSON(n))
			case 1:
				col.AppendJSON(types.CreateBinaryJSON(mathrand.NormFloat64()))
			case 2:
				col.AppendJSON(types.CreateBinaryJSON(nil))
			case 3:
				// array
				array := make([]any, 0, 2)
				array = append(array, genRandomInt(math.MaxInt64))
				array = append(array, genRandomInt(math.MaxInt64))
				col.AppendJSON(types.CreateBinaryJSON(array))
			case 4:
				// map
				maps := make(map[string]any)
				maps[strconv.Itoa(int(genRandomInt(math.MaxInt32)))] = genRandomInt(math.MaxInt64)
				maps[strconv.Itoa(int(genRandomInt(math.MaxInt32)))] = genRandomInt(math.MaxInt64)
				maps[strconv.Itoa(int(genRandomInt(math.MaxInt32)))] = genRandomInt(math.MaxInt64)
				col.AppendJSON(types.CreateBinaryJSON(maps))
			case 5:
				// string
				col.AppendJSON(types.CreateBinaryJSON(stringSample))
			}
		}
	case mysql.TypeNull:
		col.AppendNNulls(size)
	}
}

// GenRandomChunks generate random chunk data
func GenRandomChunks(schema []*types.FieldType, size int) *chunk.Chunk {
	chk := chunk.NewEmptyChunk(schema)
	for index, fieldType := range schema {
		col := chk.Column(index)
		genRandomColumn(col, fieldType, size)
	}
	chk.SetNumVirtualRows(size)
	return chk
}
