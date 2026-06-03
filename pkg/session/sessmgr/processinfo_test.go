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

package sessmgr

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

func MyFunc(any) map[string]uint64 {
	return nil
}

func TestProcessInfoShallowCP(t *testing.T) {
	mem := memory.NewTracker(-1, -1)
	mem.Consume(1<<30 + 1<<29 + 1<<28 + 1<<27)

	var refCount stmtctx.ReferenceCount = 0
	info := &ProcessInfo{
		ID:                233,
		User:              "PingCAP",
		Host:              "127.0.0.1",
		DB:                "Database",
		Info:              "select * from table where a > 1",
		CurTxnStartTS:     23333,
		StatsInfo:         MyFunc,
		StmtCtx:           stmtctx.NewStmtCtx(),
		RefCountOfStmtCtx: &refCount,
		MemTracker:        mem,
		RedactSQL:         "",
		SessionAlias:      "alias123",
	}

	cp := info.Clone()
	require.False(t, cp == info)
	require.True(t, cp.ID == info.ID)
	require.True(t, cp.User == info.User)
	require.True(t, cp.Host == info.Host)
	require.True(t, cp.DB == info.DB)
	require.True(t, cp.Info == info.Info)
	require.True(t, cp.CurTxnStartTS == info.CurTxnStartTS)
	// reflect.DeepEqual couldn't cmp two function pointer.
	require.True(t, reflect.ValueOf(cp.StatsInfo).Pointer() == reflect.ValueOf(info.StatsInfo).Pointer())
	fmt.Println(reflect.ValueOf(cp.StatsInfo).Pointer(), reflect.ValueOf(info.StatsInfo).Pointer())
	require.True(t, cp.StmtCtx == info.StmtCtx)
	require.True(t, cp.RefCountOfStmtCtx == info.RefCountOfStmtCtx)
	require.True(t, cp.MemTracker == info.MemTracker)
	require.True(t, cp.RedactSQL == info.RedactSQL)
	require.True(t, cp.SessionAlias == info.SessionAlias)
}

func TestProcessInfoToRowWithRefCountProtection(t *testing.T) {
	mem := memory.NewTracker(-1, -1)
	mem.Consume(1 << 30)

	sc := stmtctx.NewStmtCtx()

	var refCount stmtctx.ReferenceCount = 0
	info := &ProcessInfo{
		ID:                233,
		User:              "PingCAP",
		Host:              "127.0.0.1",
		DB:                "Database",
		Info:              "select * from table",
		CurTxnStartTS:     23333,
		StatsInfo:         MyFunc,
		StmtCtx:           sc,
		RefCountOfStmtCtx: &refCount,
		MemTracker:        mem,
		RedactSQL:         "",
		SessionAlias:      "",
	}

	// ToRow should work normally when RefCountOfStmtCtx allows access.
	row := info.ToRow(time.UTC)
	require.NotNil(t, row)

	// ToRow should return gracefully (with zero-valued StmtCtx fields) when
	// the StmtCtx is frozen (simulating concurrent reset).
	freezeOK := refCount.TryFreeze()
	require.True(t, freezeOK)
	rowFrozen := info.ToRow(time.UTC)
	require.NotNil(t, rowFrozen)
	// When frozen, StmtCtx-dependent fields should be zero-valued.
	// bytesConsumed (index 9 in the row: ToRowForShow has 8 items, then Digest, then bytesConsumed)
	require.Equal(t, int64(0), rowFrozen[9])
	refCount.UnFreeze()

	// ToRow with nil RefCountOfStmtCtx should still work (backward compatible).
	info2 := &ProcessInfo{
		ID:                234,
		User:              "PingCAP",
		Host:              "127.0.0.1",
		DB:                "Database",
		Info:              "select * from table",
		StmtCtx:           sc,
		RefCountOfStmtCtx: nil,
		MemTracker:        mem,
		StatsInfo:         MyFunc,
	}
	rowNilRef := info2.ToRow(time.UTC)
	require.NotNil(t, rowNilRef)
}
