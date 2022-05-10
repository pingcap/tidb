// Copyright 2016 PingCAP, Inc.
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
	"context"
	"crypto/tls"
	"runtime"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	plannerutil "github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/tableutil"
	"github.com/stretchr/testify/require"
)

// Note: it's a tricky way to export the `inspectionSummaryRules` and `inspectionRules` for unit test but invisible for normal code
var (
	InspectionSummaryRules = inspectionSummaryRules
	InspectionRules        = inspectionRules
)

// mockSessionManager is a mocked session manager which is used for test.
type mockSessionManager struct {
	PS       []*util.ProcessInfo
	serverID uint64
}

func (msm *mockSessionManager) ShowTxnList() []*txninfo.TxnInfo {
	panic("unimplemented!")
}

// ShowProcessList implements the SessionManager.ShowProcessList interface.
func (msm *mockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo {
	ret := make(map[uint64]*util.ProcessInfo)
	for _, item := range msm.PS {
		ret[item.ID] = item
	}
	return ret
}

func (msm *mockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	for _, item := range msm.PS {
		if item.ID == id {
			return item, true
		}
	}
	return &util.ProcessInfo{}, false
}

// Kill implements the SessionManager.Kill interface.
func (msm *mockSessionManager) Kill(cid uint64, query bool) {
}

func (msm *mockSessionManager) KillAllConnections() {
}

func (msm *mockSessionManager) UpdateTLSConfig(cfg *tls.Config) {
}

func (msm *mockSessionManager) ServerID() uint64 {
	return msm.serverID
}

func (msm *mockSessionManager) SetServerID(serverID uint64) {
	msm.serverID = serverID
}

func (msm *mockSessionManager) StoreInternalSession(se interface{}) {}

func (msm *mockSessionManager) DeleteInternalSession(se interface{}) {}

func (msm *mockSessionManager) GetInternalSessionStartTSList() []uint64 {
	return nil
}

func TestShowProcessList(t *testing.T) {
	// Compose schema.
	names := []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}
	ftypes := []byte{mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar,
		mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeVarchar, mysql.TypeString}
	schema := buildSchema(names, ftypes)

	// Compose a mocked session manager.
	ps := make([]*util.ProcessInfo, 0, 1)
	pi := &util.ProcessInfo{
		ID:      0,
		User:    "test",
		Host:    "127.0.0.1",
		DB:      "test",
		Command: 't',
		State:   1,
		Info:    "",
	}
	ps = append(ps, pi)
	sm := &mockSessionManager{
		PS: ps,
	}
	sctx := mock.NewContext()
	sctx.SetSessionManager(sm)
	sctx.GetSessionVars().User = &auth.UserIdentity{Username: "test"}

	// Compose executor.
	e := &ShowExec{
		baseExecutor: newBaseExecutor(sctx, schema, 0),
		Tp:           ast.ShowProcessList,
	}

	ctx := context.Background()
	err := e.Open(ctx)
	require.NoError(t, err)

	chk := newFirstChunk(e)
	it := chunk.NewIterator4Chunk(chk)
	// Run test and check results.
	for _, p := range ps {
		err = e.Next(context.Background(), chk)
		require.NoError(t, err)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			require.Equal(t, row.GetUint64(0), p.ID)
		}
	}
	err = e.Next(context.Background(), chk)
	require.NoError(t, err)
	require.Equal(t, 0, chk.NumRows())
	err = e.Close()
	require.NoError(t, err)
}

func buildSchema(names []string, ftypes []byte) *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, len(names))...)
	for i := range names {
		col := &expression.Column{
			UniqueID: int64(i),
		}
		// User varchar as the default return column type.
		tp := mysql.TypeVarchar
		if len(ftypes) != 0 && ftypes[0] != mysql.TypeUnspecified {
			tp = ftypes[0]
		}
		fieldType := types.NewFieldType(tp)
		flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.SetFlen(flen)
		fieldType.SetDecimal(decimal)

		charset, collate := types.DefaultCharsetForType(tp)
		fieldType.SetCharset(charset)
		fieldType.SetCollate(collate)
		col.RetType = fieldType
		schema.Append(col)
	}
	return schema
}

func TestBuildKvRangesForIndexJoinWithoutCwc(t *testing.T) {
	indexRanges := make([]*ranger.Range, 0, 6)
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 1, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 2, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 2, 1, 2))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 3, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(2, 1, 1, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(2, 1, 2, 1, 1))

	joinKeyRows := make([]*indexJoinLookUpContent, 0, 5)
	joinKeyRows = append(joinKeyRows, &indexJoinLookUpContent{keys: generateDatumSlice(1, 1)})
	joinKeyRows = append(joinKeyRows, &indexJoinLookUpContent{keys: generateDatumSlice(1, 2)})
	joinKeyRows = append(joinKeyRows, &indexJoinLookUpContent{keys: generateDatumSlice(2, 1)})
	joinKeyRows = append(joinKeyRows, &indexJoinLookUpContent{keys: generateDatumSlice(2, 2)})
	joinKeyRows = append(joinKeyRows, &indexJoinLookUpContent{keys: generateDatumSlice(2, 3)})

	keyOff2IdxOff := []int{1, 3}
	ctx := mock.NewContext()
	kvRanges, err := buildKvRangesForIndexJoin(ctx, 0, 0, joinKeyRows, indexRanges, keyOff2IdxOff, nil, nil, nil)
	require.NoError(t, err)
	// Check the kvRanges is in order.
	for i, kvRange := range kvRanges {
		require.True(t, kvRange.StartKey.Cmp(kvRange.EndKey) < 0)
		if i > 0 {
			require.True(t, kvRange.StartKey.Cmp(kvRanges[i-1].EndKey) >= 0)
		}
	}
}

func generateIndexRange(vals ...int64) *ranger.Range {
	lowDatums := generateDatumSlice(vals...)
	highDatums := make([]types.Datum, len(vals))
	copy(highDatums, lowDatums)
	return &ranger.Range{LowVal: lowDatums, HighVal: highDatums, Collators: collate.GetBinaryCollatorSlice(len(lowDatums))}
}

func generateDatumSlice(vals ...int64) []types.Datum {
	datums := make([]types.Datum, len(vals))
	for i, val := range vals {
		datums[i].SetInt64(val)
	}
	return datums
}

func TestGetFieldsFromLine(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			`"1","a string","100.20"`,
			[]string{"1", "a string", "100.20"},
		},
		{
			`"2","a string containing a , comma","102.20"`,
			[]string{"2", "a string containing a , comma", "102.20"},
		},
		{
			`"3","a string containing a \" quote","102.20"`,
			[]string{"3", "a string containing a \" quote", "102.20"},
		},
		{
			`"4","a string containing a \", quote and comma","102.20"`,
			[]string{"4", "a string containing a \", quote and comma", "102.20"},
		},
		// Test some escape char.
		{
			`"\0\b\n\r\t\Z\\\  \c\'\""`,
			[]string{string([]byte{0, '\b', '\n', '\r', '\t', 26, '\\', ' ', ' ', 'c', '\'', '"'})},
		},
		// Test mixed.
		{
			`"123",456,"\t7890",abcd`,
			[]string{"123", "456", "\t7890", "abcd"},
		},
	}

	ldInfo := LoadDataInfo{
		FieldsInfo: &ast.FieldsClause{
			Enclosed:   '"',
			Terminated: ",",
			Escaped:    '\\',
		},
	}

	for _, test := range tests {
		got, err := ldInfo.getFieldsFromLine([]byte(test.input))
		require.NoErrorf(t, err, "failed: %s", test.input)
		assertEqualStrings(t, got, test.expected)
	}

	_, err := ldInfo.getFieldsFromLine([]byte(`1,a string,100.20`))
	require.NoError(t, err)
}

func assertEqualStrings(t *testing.T, got []field, expect []string) {
	require.Equal(t, len(expect), len(got))
	for i := 0; i < len(got); i++ {
		require.Equal(t, expect[i], string(got[i].str))
	}
}

func TestSlowQueryRuntimeStats(t *testing.T) {
	stats := &slowQueryRuntimeStats{
		totalFileNum: 2,
		readFileNum:  2,
		readFile:     time.Second,
		initialize:   time.Millisecond,
		readFileSize: 1024 * 1024 * 1024,
		parseLog:     int64(time.Millisecond * 100),
		concurrent:   15,
	}
	require.Equal(t, "initialize: 1ms, read_file: 1s, parse_log: {time:100ms, concurrency:15}, total_file: 2, read_file: 2, read_size: 1024 MB", stats.String())
	require.Equal(t, stats.Clone().String(), stats.String())
	stats.Merge(stats.Clone())
	require.Equal(t, "initialize: 2ms, read_file: 2s, parse_log: {time:200ms, concurrency:15}, total_file: 4, read_file: 4, read_size: 2 GB", stats.String())
}

// Test whether the actual buckets in Golang Map is same with the estimated number.
// The test relies the implement of Golang Map. ref https://github.com/golang/go/blob/go1.13/src/runtime/map.go#L114
func TestAggPartialResultMapperB(t *testing.T) {
	if runtime.Version() < `go1.13` {
		t.Skip("Unsupported version")
	}
	type testCase struct {
		rowNum          int
		expectedB       int
		expectedGrowing bool
	}
	cases := []testCase{
		{
			rowNum:          0,
			expectedB:       0,
			expectedGrowing: false,
		},
		{
			rowNum:          100,
			expectedB:       4,
			expectedGrowing: false,
		},
		{
			rowNum:          10000,
			expectedB:       11,
			expectedGrowing: false,
		},
		{
			rowNum:          1000000,
			expectedB:       18,
			expectedGrowing: false,
		},
		{
			rowNum:          851968, // 6.5 * (1 << 17)
			expectedB:       17,
			expectedGrowing: false,
		},
		{
			rowNum:          851969, // 6.5 * (1 << 17) + 1
			expectedB:       18,
			expectedGrowing: true,
		},
		{
			rowNum:          425984, // 6.5 * (1 << 16)
			expectedB:       16,
			expectedGrowing: false,
		},
		{
			rowNum:          425985, // 6.5 * (1 << 16) + 1
			expectedB:       17,
			expectedGrowing: true,
		},
	}

	for _, tc := range cases {
		aggMap := make(aggPartialResultMapper)
		tempSlice := make([]aggfuncs.PartialResult, 10)
		for num := 0; num < tc.rowNum; num++ {
			aggMap[strconv.Itoa(num)] = tempSlice
		}

		require.Equal(t, tc.expectedB, getB(aggMap))
		require.Equal(t, tc.expectedGrowing, getGrowing(aggMap))
	}
}

// A header for a Go map.
// nolint:structcheck
type hmap struct {
	// Note: the format of the hmap is also encoded in cmd/compile/internal/gc/reflect.go.
	// Make sure this stays in sync with the compiler's definition.
	count     int    // nolint:unused // # live cells == size of map.  Must be first (used by len() builtin)
	flags     uint8  // nolint:unused
	B         uint8  // nolint:unused // log_2 of # of buckets (can hold up to loadFactor * 2^B items)
	noverflow uint16 // nolint:unused // approximate number of overflow buckets; see incrnoverflow for details
	hash0     uint32 // nolint:unused // hash seed

	buckets    unsafe.Pointer // nolint:unused // array of 2^B Buckets. may be nil if count==0.
	oldbuckets unsafe.Pointer // nolint:unused // previous bucket array of half the size, non-nil only when growing
	nevacuate  uintptr        // nolint:unused // progress counter for evacuation (buckets less than this have been evacuated)
}

func getB(m aggPartialResultMapper) int {
	point := (**hmap)(unsafe.Pointer(&m))
	value := *point
	return int(value.B)
}

func getGrowing(m aggPartialResultMapper) bool {
	point := (**hmap)(unsafe.Pointer(&m))
	value := *point
	return value.oldbuckets != nil
}

func TestFilterTemporaryTableKeys(t *testing.T) {
	vars := variable.NewSessionVars()
	const tableID int64 = 3
	vars.TxnCtx = &variable.TransactionContext{
		TemporaryTables: map[int64]tableutil.TempTable{tableID: nil},
	}

	res := filterTemporaryTableKeys(vars, []kv.Key{tablecodec.EncodeTablePrefix(tableID), tablecodec.EncodeTablePrefix(42)})
	require.Len(t, res, 1)
}

func TestLoadDataWithDifferentEscapeChar(t *testing.T) {
	tests := []struct {
		input      string
		escapeChar byte
		expected   []string
	}{
		{
			`"{""itemRangeType"":0,""itemContainType"":0,""shopRangeType"":1,""shopJson"":""[{\""id\"":\""A1234\"",\""shopName\"":\""AAAAAA\""}]""}"`,
			byte(0), // escaped by ''
			[]string{`{"itemRangeType":0,"itemContainType":0,"shopRangeType":1,"shopJson":"[{\"id\":\"A1234\",\"shopName\":\"AAAAAA\"}]"}`},
		},
	}

	for _, test := range tests {
		ldInfo := LoadDataInfo{
			FieldsInfo: &ast.FieldsClause{
				Enclosed:   '"',
				Terminated: ",",
				Escaped:    test.escapeChar,
			},
		}
		got, err := ldInfo.getFieldsFromLine([]byte(test.input))
		require.NoErrorf(t, err, "failed: %s", test.input)
		assertEqualStrings(t, got, test.expected)
	}
}

func TestSortSpillDisk(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
	})
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill"))
	}()
	ctx := mock.NewContext()
	ctx.GetSessionVars().MemQuota.MemQuotaQuery = 1
	ctx.GetSessionVars().InitChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	cas := &sortCase{rows: 2048, orderByIdx: []int{0, 1}, ndvs: []int{0, 0}, ctx: ctx}
	opt := mockDataSourceParameters{
		schema: expression.NewSchema(cas.columns()...),
		rows:   cas.rows,
		ctx:    cas.ctx,
		ndvs:   cas.ndvs,
	}
	dataSource := buildMockDataSource(opt)
	exec := &SortExec{
		baseExecutor: newBaseExecutor(cas.ctx, dataSource.schema, 0, dataSource),
		ByItems:      make([]*plannerutil.ByItems, 0, len(cas.orderByIdx)),
		schema:       dataSource.schema,
	}
	for _, idx := range cas.orderByIdx {
		exec.ByItems = append(exec.ByItems, &plannerutil.ByItems{Expr: cas.columns()[idx]})
	}
	tmpCtx := context.Background()
	chk := newFirstChunk(exec)
	dataSource.prepareChunks()
	err := exec.Open(tmpCtx)
	require.NoError(t, err)
	for {
		err = exec.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Test only 1 partition and all data in memory.
	require.Len(t, exec.partitionList, 1)
	require.Equal(t, false, exec.partitionList[0].AlreadySpilledSafeForTest())
	require.Equal(t, 2048, exec.partitionList[0].NumRow())
	err = exec.Close()
	require.NoError(t, err)

	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, 1)
	dataSource.prepareChunks()
	err = exec.Open(tmpCtx)
	require.NoError(t, err)
	for {
		err = exec.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Test 2 partitions and all data in disk.
	// Now spilling is in parallel.
	// Maybe the second add() will called before spilling, depends on
	// Golang goroutine scheduling. So the result has two possibilities.
	if len(exec.partitionList) == 2 {
		require.Len(t, exec.partitionList, 2)
		require.Equal(t, true, exec.partitionList[0].AlreadySpilledSafeForTest())
		require.Equal(t, true, exec.partitionList[1].AlreadySpilledSafeForTest())
		require.Equal(t, 1024, exec.partitionList[0].NumRow())
		require.Equal(t, 1024, exec.partitionList[1].NumRow())
	} else {
		require.Len(t, exec.partitionList, 1)
		require.Equal(t, true, exec.partitionList[0].AlreadySpilledSafeForTest())
		require.Equal(t, 2048, exec.partitionList[0].NumRow())
	}

	err = exec.Close()
	require.NoError(t, err)

	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, 24000)
	dataSource.prepareChunks()
	err = exec.Open(tmpCtx)
	require.NoError(t, err)
	for {
		err = exec.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Test only 1 partition but spill disk.
	require.Len(t, exec.partitionList, 1)
	require.Equal(t, true, exec.partitionList[0].AlreadySpilledSafeForTest())
	require.Equal(t, 2048, exec.partitionList[0].NumRow())
	err = exec.Close()
	require.NoError(t, err)

	// Test partition nums.
	ctx = mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, 16864*50)
	ctx.GetSessionVars().StmtCtx.MemTracker.Consume(16864 * 45)
	cas = &sortCase{rows: 20480, orderByIdx: []int{0, 1}, ndvs: []int{0, 0}, ctx: ctx}
	opt = mockDataSourceParameters{
		schema: expression.NewSchema(cas.columns()...),
		rows:   cas.rows,
		ctx:    cas.ctx,
		ndvs:   cas.ndvs,
	}
	dataSource = buildMockDataSource(opt)
	exec = &SortExec{
		baseExecutor: newBaseExecutor(cas.ctx, dataSource.schema, 0, dataSource),
		ByItems:      make([]*plannerutil.ByItems, 0, len(cas.orderByIdx)),
		schema:       dataSource.schema,
	}
	for _, idx := range cas.orderByIdx {
		exec.ByItems = append(exec.ByItems, &plannerutil.ByItems{Expr: cas.columns()[idx]})
	}
	tmpCtx = context.Background()
	chk = newFirstChunk(exec)
	dataSource.prepareChunks()
	err = exec.Open(tmpCtx)
	require.NoError(t, err)
	for {
		err = exec.Next(tmpCtx, chk)
		require.NoError(t, err)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Don't spill too many partitions.
	require.True(t, len(exec.partitionList) <= 4)
	err = exec.Close()
	require.NoError(t, err)
}
