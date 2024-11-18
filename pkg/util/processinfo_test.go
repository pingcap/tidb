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

package util

import (
	"fmt"
	"reflect"
	"testing"

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
