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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
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
	rootTracker.Killer = &sqlkiller.SQLKiller{ConnID: 1}

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
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
		chk.AppendInt64(1, int64(i))
		chk.AppendInt64(2, int64(i))
		chk.AppendString(3, "testtesttest")
		chk.AppendInt64(4, int64(i))
	}

	sp := newSortPartition(fields, byItemsDesc, keyColumns, keyCmpFuncs, 1 /* always can spill */)
	defer sp.close()
	sp.getMemTracker().AttachTo(rootTracker)
	for i := 0; i < 10240; i++ {
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
	rootTracker := memory.NewTracker(-1, -1)
	rootTracker.IsRootTrackerOfSess = true
	rootTracker.Killer = &sqlkiller.SQLKiller{ConnID: 1}

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
	for i := 0; i < sz; i++ {
		chk.AppendInt64(0, int64(i))
		chk.AppendInt64(1, int64(i))
		chk.AppendInt64(2, int64(i))
		chk.AppendString(3, "testtesttesttesttesttesttesttest")
		chk.AppendInt64(4, int64(i))
	}

	sp := newSortPartition(fields, byItemsDesc, keyColumns, keyCmpFuncs, 1 /* always can spill */)
	defer sp.close()
	sp.getMemTracker().AttachTo(rootTracker)
	for i := 0; i < 10240; i++ {
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
