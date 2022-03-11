// Copyright 2021 PingCAP, Inc.
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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestGlueGetVersion(t *testing.T) {
	g := tidbGlueSession{}
	version := g.GetVersion()
	require.Contains(t, version, `Release Version`)
	require.Contains(t, version, `Git Commit Hash`)
	require.Contains(t, version, `GoVersion`)
}

func brieTaskInfoToResult(info *brieTaskInfo) string {
	arr := make([]string, 0, 8)
	arr = append(arr, info.storage)
	arr = append(arr, "Wait")
	arr = append(arr, "0")
	arr = append(arr, info.queueTime.String())
	arr = append(arr, info.execTime.String())
	arr = append(arr, info.finishTime.String())
	arr = append(arr, fmt.Sprintf("%d", info.connID))
	if len(info.message) > 0 {
		arr = append(arr, info.message)
	} else {
		arr = append(arr, "NULL")
	}
	return strings.Join(arr, ", ") + "\n"
}

func fetchShowBRIEResult(t *testing.T, e *ShowExec, brieColTypes []*types.FieldType) string {
	e.result = newFirstChunk(e)
	require.NoError(t, e.fetchShowBRIE(ast.BRIEKindBackup))
	return e.result.ToString(brieColTypes)
}

func TestFetchShowBRIE(t *testing.T) {
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

	ctx := context.Background()
	// Compose schema.
	p := parser.New()
	p.SetParserConfig(parser.ParserConfig{EnableWindowFunction: true, EnableStrictDoubleTypeCheck: true})
	stmt, err := p.ParseOneStmt("show backups", "", "")
	require.NoError(t, err)
	plan, _, err := core.BuildLogicalPlanForTest(ctx, sctx, stmt, infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable(), core.MockView()}))
	require.NoError(t, err)
	schema := plan.Schema()

	// Compose executor.
	e := &ShowExec{
		baseExecutor: newBaseExecutor(sctx, schema, 0),
		Tp:           ast.ShowBackups,
	}
	require.NoError(t, e.Open(ctx))

	tp := mysql.TypeDatetime
	lateTime := types.NewTime(types.FromGoTime(time.Now().Add(-outdatedDuration.Duration+1)), tp, 0)
	brieColTypes := make([]*types.FieldType, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		brieColTypes = append(brieColTypes, col.RetType)
	}

	// Register brie task info
	info1 := &brieTaskInfo{
		kind:       ast.BRIEKindBackup,
		connID:     e.ctx.GetSessionVars().ConnectionID,
		queueTime:  lateTime,
		execTime:   lateTime,
		finishTime: lateTime,
		storage:    "noop://test/backup1",
		message:    "killed",
	}
	info1Res := brieTaskInfoToResult(info1)

	globalBRIEQueue.registerTask(ctx, info1)
	require.Equal(t, info1Res, fetchShowBRIEResult(t, e, brieColTypes))

	// Query again, this info should already have been cleaned
	require.Len(t, fetchShowBRIEResult(t, e, brieColTypes), 0)

	// Register this task again, we should be able to fetch this info
	globalBRIEQueue.registerTask(ctx, info1)
	require.Equal(t, info1Res, fetchShowBRIEResult(t, e, brieColTypes))

	// Query again, we should be able to fetch this info again, because we have cleared in last clearInterval
	require.Equal(t, info1Res, fetchShowBRIEResult(t, e, brieColTypes))

	// Reset clear time, we should only fetch info2 this time.
	globalBRIEQueue.lastClearTime = time.Now().Add(-clearInterval - time.Second)
	currTime := types.CurrentTime(tp)
	info2 := &brieTaskInfo{
		kind:       ast.BRIEKindBackup,
		connID:     e.ctx.GetSessionVars().ConnectionID,
		queueTime:  currTime,
		execTime:   currTime,
		finishTime: currTime,
		storage:    "noop://test/backup2",
		message:    "",
	}
	info2Res := brieTaskInfoToResult(info2)
	globalBRIEQueue.registerTask(ctx, info2)
	globalBRIEQueue.clearTask(e.ctx.GetSessionVars().StmtCtx)
	require.Equal(t, info2Res, fetchShowBRIEResult(t, e, brieColTypes))
}
