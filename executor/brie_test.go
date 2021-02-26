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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"
	"strings"
	gotime "time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/mock"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
)

type testBRIESuite struct{}

var _ = Suite(&testBRIESuite{})

func (s *testBRIESuite) TestGlueGetVersion(c *C) {
	g := tidbGlueSession{}
	version := g.GetVersion()
	c.Assert(version, Matches, `(.|\n)*Release Version(.|\n)*`)
	c.Assert(version, Matches, `(.|\n)*Git Commit Hash(.|\n)*`)
	c.Assert(version, Matches, `(.|\n)*GoVersion(.|\n)*`)
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
	arr = append(arr, info.message)
	return strings.Join(arr, ", ")
}

func (s *testBRIESuite) TestFetchShowBRIE(c *C) {
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
	c.Assert(err, IsNil)
	plan, _, err := core.BuildLogicalPlan(ctx, sctx, stmt, infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable(), core.MockView()}))
	c.Assert(err, IsNil)
	schema := plan.Schema()

	// Compose executor.
	e := &ShowExec{
		baseExecutor: newBaseExecutor(sctx, schema, 0),
		Tp:           ast.ShowBackups,
	}
	e.result = newFirstChunk(e)
	c.Assert(e.Open(ctx), IsNil)

	tp := mysql.TypeDatetime
	lateTime := types.NewTime(types.FromGoTime(gotime.Now().Add(-outdatedDuration.Duration+1)), tp, 0)
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
	c.Assert(e.fetchShowBRIE(ast.BRIEKindBackup), IsNil)
	res := e.result.ToString(brieColTypes)
	c.Assert(res, Equals, info1Res)

	// Query again, this info should already have been cleaned
	e.result = newFirstChunk(e)
	c.Assert(e.fetchShowBRIE(ast.BRIEKindBackup), IsNil)
	res = e.result.ToString(brieColTypes)
	c.Assert(res, HasLen, 0)

	// Register this task again, we should be able to fetch this info
	e.result = newFirstChunk(e)
	globalBRIEQueue.registerTask(ctx, info1)
	c.Assert(e.fetchShowBRIE(ast.BRIEKindBackup), IsNil)
	res = e.result.ToString(brieColTypes)
	c.Assert(res, Equals, info1Res)

	// Query again, we should be able to fetch this info again, because we have cleared in last clearInterval
	e.result = newFirstChunk(e)
	c.Assert(e.fetchShowBRIE(ast.BRIEKindBackup), IsNil)
	res = e.result.ToString(brieColTypes)
	c.Assert(res, Equals, info1Res)

	// Reset clear time, we should only fetch info2 this time.
	globalBRIEQueue.lastClearTime = gotime.Now().Add(-clearInterval - gotime.Second)
	currTime := types.CurrentTime(tp)
	info2 := &brieTaskInfo{
		kind:       ast.BRIEKindBackup,
		connID:     e.ctx.GetSessionVars().ConnectionID,
		queueTime:  currTime,
		execTime:   currTime,
		finishTime: currTime,
		storage:    "noop://test/backup2",
		message:    "finished",
	}
	info2Res := brieTaskInfoToResult(info2)
	globalBRIEQueue.registerTask(ctx, info2)
	globalBRIEQueue.clearTask(e.ctx.GetSessionVars().StmtCtx)
	e.result = newFirstChunk(e)
	c.Assert(e.fetchShowBRIE(ast.BRIEKindBackup), IsNil)
	res = e.result.ToString(brieColTypes)
	c.Assert(res, Equals, info2Res)
}
