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

package sortexec

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

func TestInterruptedDuringSort(t *testing.T) {
	rootTracker := memory.NewTracker(-1, -1)
	rootTracker.IsRootTrackerOfSess = true
	rootTracker.Killer = &sqlkiller.SQLKiller{}
	rootTracker.Killer.ConnID.Store(1)

	byItemsDesc := []bool{false}
	keyColumns := []int{0}
	keyCmpFuncs := []chunk.CompareFunc{chunk.GetCompareFunc(types.NewFieldType(mysql.TypeLonglong))}

	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
	}
	sz := 1024

	chk := chunk.NewChunkWithCapacity(fields, sz)
	for i := range sz {
		chk.AppendInt64(0, int64(i))
		chk.AppendInt64(1, int64(i))
		chk.AppendInt64(2, int64(i))
		chk.AppendString(3, "testtesttest")
		chk.AppendInt64(4, int64(i))
	}

	sp := newSortPartition(fields, byItemsDesc, keyColumns, keyCmpFuncs, 1 /* always can spill */, "", time.UTC)
	defer sp.close()
	sp.getMemTracker().AttachTo(rootTracker)
	for range 10240 {
		canadd := sp.add(chk)
		require.True(t, canadd)
	}
	var cancelTime time.Time
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(200 * time.Millisecond)
		rootTracker.Killer.SendKillSignal(sqlkiller.QueryInterrupted)
		cancelTime = time.Now()
		wg.Done()
	}()
	err := sp.sort()
	wg.Wait()
	cancelDuration := time.Since(cancelTime)
	require.Less(t, cancelDuration, 1*time.Second)
	require.True(t, exeerrors.ErrQueryInterrupted.Equal(err))
}

func TestInterruptedDuringSpilling(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
	})
	testFuncName := util.GetFunctionName()

	defer util.CheckNoLeakFiles(t, testFuncName)

	rootTracker := memory.NewTracker(-1, -1)
	rootTracker.IsRootTrackerOfSess = true
	rootTracker.Killer = &sqlkiller.SQLKiller{}
	rootTracker.Killer.ConnID.Store(1)

	byItemsDesc := []bool{false}
	keyColumns := []int{0}
	keyCmpFuncs := []chunk.CompareFunc{chunk.GetCompareFunc(types.NewFieldType(mysql.TypeLonglong))}

	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
	}
	sz := 1024

	chk := chunk.NewChunkWithCapacity(fields, sz)
	for i := range sz {
		chk.AppendInt64(0, int64(i))
		chk.AppendInt64(1, int64(i))
		chk.AppendInt64(2, int64(i))
		chk.AppendString(3, "testtesttesttesttesttesttesttest")
		chk.AppendInt64(4, int64(i))
	}

	sp := newSortPartition(fields, byItemsDesc, keyColumns, keyCmpFuncs, 1 /* always can spill */, testFuncName, time.UTC)
	defer sp.close()
	sp.getMemTracker().AttachTo(rootTracker)
	for range 10240 {
		canadd := sp.add(chk)
		require.True(t, canadd)
	}
	err := sp.sort()
	require.NoError(t, err)
	var cancelTime time.Time
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		time.Sleep(200 * time.Millisecond)
		rootTracker.Killer.SendKillSignal(sqlkiller.QueryInterrupted)
		cancelTime = time.Now()
		wg.Done()
	}()
	err = sp.spillToDisk()
	wg.Wait()
	cancelDuration := time.Since(cancelTime)
	require.Less(t, cancelDuration, 1*time.Second)
	require.True(t, exeerrors.ErrQueryInterrupted.Equal(err))
}

func TestSortPartitionAQSortMatchesStdSort(t *testing.T) {
	defer SetAQSortEnabled(false)

	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
	}
	keyColumns := []int{0, 2} // string, then unique id
	keyCmpFuncs := []chunk.CompareFunc{
		chunk.GetCompareFunc(fields[keyColumns[0]]),
		chunk.GetCompareFunc(fields[keyColumns[1]]),
	}

	chk := chunk.NewChunkWithCapacity(fields, 1024)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 1024; i++ {
		chk.AppendString(0, fmt.Sprintf("seg_%02d", rng.Intn(50)))
		chk.AppendInt64(1, int64(rng.Intn(10_000)))
		chk.AppendInt64(2, int64(i))
	}

	cases := []struct {
		name        string
		byItemsDesc []bool
	}{
		{name: "asc", byItemsDesc: []bool{false, false}},
		{name: "desc", byItemsDesc: []bool{true, false}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			SetAQSortEnabled(false)
			std := newSortPartition(fields, tc.byItemsDesc, keyColumns, keyCmpFuncs, 1<<60, "", time.UTC)
			defer std.close()
			require.True(t, std.add(chk))
			require.NoError(t, std.sort())

			SetAQSortEnabled(true)
			aqs := newSortPartition(fields, tc.byItemsDesc, keyColumns, keyCmpFuncs, 1<<60, "", time.UTC)
			defer aqs.close()
			require.True(t, aqs.add(chk))
			require.NoError(t, aqs.sort())
			SetAQSortEnabled(false)

			require.Equal(t, len(std.savedRows), len(aqs.savedRows))
			for i := range std.savedRows {
				require.Equal(t, std.savedRows[i].GetInt64(2), aqs.savedRows[i].GetInt64(2), "row mismatch at index %d", i)
			}
		})
	}
}

func TestSortPartitionAQSortFallbackToStdSortOnEncodeError(t *testing.T) {
	defer SetAQSortEnabled(false)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/sortexec/AQSortForceEncodeKeyError", `return(true)`)

	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
	}
	keyColumns := []int{0, 2} // string, then unique id
	keyCmpFuncs := []chunk.CompareFunc{
		chunk.GetCompareFunc(fields[keyColumns[0]]),
		chunk.GetCompareFunc(fields[keyColumns[1]]),
	}

	chk := chunk.NewChunkWithCapacity(fields, 1024)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 1024; i++ {
		chk.AppendString(0, fmt.Sprintf("seg_%02d", rng.Intn(50)))
		chk.AppendInt64(1, int64(rng.Intn(10_000)))
		chk.AppendInt64(2, int64(i))
	}

	std := newSortPartition(fields, []bool{false, false}, keyColumns, keyCmpFuncs, 1<<60, "", time.UTC)
	defer std.close()
	require.True(t, std.add(chk))
	require.NoError(t, std.sort())

	aqs := newSortPartition(fields, []bool{false, false}, keyColumns, keyCmpFuncs, 1<<60, "", time.UTC)
	defer aqs.close()
	aqs.aqsortCtrl = newAQSortControl(true, 1, 1)
	require.True(t, aqs.add(chk))
	require.NoError(t, aqs.sort())
	require.False(t, aqs.aqsortCtrl.isEnabled())
	require.True(t, aqs.aqsortCtrl.warned.Load())

	require.Equal(t, len(std.savedRows), len(aqs.savedRows))
	for i := range std.savedRows {
		require.Equal(t, std.savedRows[i].GetInt64(2), aqs.savedRows[i].GetInt64(2), "row mismatch at index %d", i)
	}
}

func TestSortPartitionAQSortMatchesStdSort_MultiTypesAndNulls(t *testing.T) {
	defer SetAQSortEnabled(false)

	ftStr := types.NewFieldType(mysql.TypeVarString)
	ftStr.SetFlen(256)
	ftStr.SetCharset("utf8mb4")
	ftStr.SetCollate("utf8mb4_general_ci")

	ftBytes := types.NewFieldType(mysql.TypeBlob)
	ftBytes.SetFlen(256)

	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftUint := types.NewFieldType(mysql.TypeLonglong)
	ftUint.AddFlag(mysql.UnsignedFlag)

	ftDec := types.NewFieldType(mysql.TypeNewDecimal)
	ftDec.SetFlen(20)
	ftDec.SetDecimal(6)

	ftTS := types.NewFieldType(mysql.TypeTimestamp)
	ftTS.SetDecimal(6)

	ftDur := types.NewFieldType(mysql.TypeDuration)
	ftDur.SetDecimal(6)

	ftJSON := types.NewFieldType(mysql.TypeJSON)

	ftID := types.NewFieldType(mysql.TypeLonglong)

	fields := []*types.FieldType{ftStr, ftBytes, ftInt, ftUint, ftDec, ftTS, ftDur, ftJSON, ftID}
	keyColumns := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	keyCmpFuncs := make([]chunk.CompareFunc, len(keyColumns))
	for i, colIdx := range keyColumns {
		keyCmpFuncs[i] = chunk.GetCompareFunc(fields[colIdx])
	}

	jsonVals := make([]types.BinaryJSON, 0, 6)
	for _, s := range []string{`null`, `0`, `-1`, `"a"`, `"A"`, `{"k":1}`} {
		bj, err := types.ParseBinaryJSONFromString(s)
		require.NoError(t, err)
		jsonVals = append(jsonVals, bj)
	}

	decVals := []*types.MyDecimal{
		types.NewDecFromStringForTest("0"),
		types.NewDecFromStringForTest("1.234567"),
		types.NewDecFromStringForTest("-1.234567"),
		types.NewDecFromStringForTest("9999999999.999999"),
		types.NewDecFromStringForTest("-9999999999.999999"),
	}

	baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	words := []string{"a", "A", "abc", "AbC", "hello", "HeLLo", "", "z"}
	rng := rand.New(rand.NewSource(1))

	buildChunk := func(rowCount int) *chunk.Chunk {
		chk := chunk.NewChunkWithCapacity(fields, rowCount)
		for i := 0; i < rowCount; i++ {
			if rng.Intn(10) == 0 {
				chk.AppendNull(0)
			} else if rng.Intn(10) == 0 {
				// long key to exercise larger encoded keys.
				b := make([]byte, 0, 200)
				for len(b) < 200 {
					b = append(b, words[rng.Intn(len(words))]...)
				}
				chk.AppendString(0, string(b))
			} else {
				chk.AppendString(0, words[rng.Intn(len(words))])
			}

			if rng.Intn(10) == 0 {
				chk.AppendNull(1)
			} else {
				ln := rng.Intn(32)
				b := make([]byte, ln)
				_, _ = rng.Read(b)
				chk.AppendBytes(1, b)
			}

			if rng.Intn(10) == 0 {
				chk.AppendNull(2)
			} else {
				switch rng.Intn(5) {
				case 0:
					chk.AppendInt64(2, 0)
				case 1:
					chk.AppendInt64(2, -1)
				case 2:
					chk.AppendInt64(2, 1)
				default:
					chk.AppendInt64(2, rng.Int63n(2000)-1000)
				}
			}

			if rng.Intn(10) == 0 {
				chk.AppendNull(3)
			} else {
				switch rng.Intn(4) {
				case 0:
					chk.AppendUint64(3, 0)
				case 1:
					chk.AppendUint64(3, 1)
				case 2:
					chk.AppendUint64(3, uint64(rng.Uint32()))
				default:
					chk.AppendUint64(3, uint64(rng.Int63()))
				}
			}

			if rng.Intn(10) == 0 {
				chk.AppendNull(4)
			} else {
				chk.AppendMyDecimal(4, decVals[rng.Intn(len(decVals))])
			}

			if rng.Intn(10) == 0 {
				chk.AppendNull(5)
			} else {
				// Keep timestamps within a safe range.
				ts := baseTime.Add(time.Duration(rng.Intn(365*24)) * time.Hour).Add(time.Duration(rng.Intn(3600)) * time.Second)
				chk.AppendTime(5, types.NewTime(types.FromGoTime(ts), mysql.TypeTimestamp, 6))
			}

			if rng.Intn(10) == 0 {
				chk.AppendNull(6)
			} else {
				dur := types.NewDuration(rng.Intn(48)-24, rng.Intn(60), rng.Intn(60), rng.Intn(1_000_000), 6)
				chk.AppendDuration(6, dur)
			}

			if rng.Intn(10) == 0 {
				chk.AppendNull(7)
			} else {
				chk.AppendJSON(7, jsonVals[rng.Intn(len(jsonVals))])
			}

			chk.AppendInt64(8, int64(i))
		}
		return chk
	}

	descCases := []struct {
		name string
		desc []bool
	}{
		{name: "all_asc", desc: make([]bool, len(keyColumns))},
		{name: "alternating", desc: []bool{true, false, true, false, true, false, true, false, false}},
		{name: "all_desc", desc: []bool{true, true, true, true, true, true, true, true, true}},
	}

	for _, tc := range descCases {
		t.Run(tc.name, func(t *testing.T) {
			chk := buildChunk(512)

			std := newSortPartition(fields, tc.desc, keyColumns, keyCmpFuncs, 1<<60, "", time.UTC)
			defer std.close()
			std.aqsortCtrl = newAQSortControl(false, 1, 1)
			require.True(t, std.add(chk))
			require.NoError(t, std.sort())

			aqs := newSortPartition(fields, tc.desc, keyColumns, keyCmpFuncs, 1<<60, "", time.UTC)
			defer aqs.close()
			aqs.aqsortCtrl = newAQSortControl(true, 1, 1)
			require.True(t, aqs.add(chk))
			require.NoError(t, aqs.sort())

			require.True(t, aqs.aqsortCtrl.isEnabled())
			require.False(t, aqs.aqsortCtrl.warned.Load())

			require.Equal(t, len(std.savedRows), len(aqs.savedRows))
			for i := range std.savedRows {
				require.Equal(t, std.savedRows[i].GetInt64(8), aqs.savedRows[i].GetInt64(8), "row mismatch at index %d", i)
			}
		})
	}
}
