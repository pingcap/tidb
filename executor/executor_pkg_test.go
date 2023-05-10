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
	"runtime"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannerutil "github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
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
	vars := variable.NewSessionVars(nil)
	const tableID int64 = 3
	vars.TxnCtx = &variable.TransactionContext{
		TxnCtxNoNeedToRestore: variable.TxnCtxNoNeedToRestore{
			TemporaryTables: map[int64]tableutil.TempTable{tableID: nil},
		},
	}

	res := filterTemporaryTableKeys(vars, []kv.Key{tablecodec.EncodeTablePrefix(tableID), tablecodec.EncodeTablePrefix(42)})
	require.Len(t, res, 1)
}

func TestSortSpillDisk(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill"))
	}()
	ctx := mock.NewContext()
	ctx.GetSessionVars().MemQuota.MemQuotaQuery = 1
	ctx.GetSessionVars().InitChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
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

	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, 1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
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

	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, 28000)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
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
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, 16864*50)
	ctx.GetSessionVars().MemTracker.Consume(16864 * 45)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
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
