// Copyright 2022 PingCAP, Inc.
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

package addindextest

import (
	"strconv"

	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var compCtx = CompatibilityContext{}

type testType int8

const (
	// TestNonUnique test type of create none unique index.
	TestNonUnique testType = iota
	// TestUnique test type of create unique index.
	TestUnique
	// TestPK test type of create Primary key.
	TestPK
	// TestGenIndex test type of create generated col index.
	TestGenIndex
	// TestMultiCols test type of multi columns in one index.
	TestMultiCols
)

// CompatibilityContext is context of compatibility test.
type CompatibilityContext struct {
	isMultiSchemaChange bool
	isConcurrentDDL     bool
	isPiTR              bool
	executor            []*executor
	colIIDs             [][]int
	colJIDs             [][]int
	tType               testType
}

type paraDDLChan struct {
	err      error
	finished bool
}

type executor struct {
	id     int
	tk     *testkit.TestKit
	PDChan chan *paraDDLChan
}

func newExecutor(tableID int) *executor {
	er := executor{
		id:     tableID,
		PDChan: make(chan *paraDDLChan, 1),
	}
	return &er
}

func initCompCtxParams(ctx *suiteContext) {
	ctx.CompCtx = &compCtx
	compCtx.isConcurrentDDL = false
	compCtx.isMultiSchemaChange = false
	compCtx.isPiTR = false
}

func (cCtx *CompatibilityContext) start(ctx *suiteContext) {
	cCtx.executor = cCtx.executor[:0]
	for i := 0; i < 3; i++ {
		er := newExecutor(i)
		er.tk = ctx.getTestKit()
		cCtx.executor = append(cCtx.executor, er)
		go cCtx.executor[i].run(ctx)
	}
}

func (cCtx *CompatibilityContext) stop(ctx *suiteContext) error {
	count := 3
	for i := 0; i < 3; i++ {
		pdChan := <-cCtx.executor[i].PDChan
		if pdChan.err != nil {
			require.NoError(ctx.t, pdChan.err)
			return pdChan.err
		}
		if pdChan.finished {
			count--
			logutil.BgLogger().Info("xlc test worker", zap.Int("count", count), zap.Int("er id", i))
			ctx.putTestKit(ctx.CompCtx.executor[i].tk)
		}
		if count == 0 {
			break
		}
	}
	return nil
}

func (e *executor) run(ctx *suiteContext) {
	var (
		err    error
		erChan paraDDLChan
	)
	switch ctx.CompCtx.tType {
	case TestNonUnique:
		err = testOneColFramePara(ctx, e.id, ctx.CompCtx.colIIDs, addIndexNonUnique)
	case TestUnique:
		err = testOneColFramePara(ctx, e.id, ctx.CompCtx.colIIDs, addIndexUnique)
	case TestPK:
		err = testOneIndexFramePara(ctx, e.id, 0, addIndexPK)
	case TestGenIndex:
		err = testOneIndexFramePara(ctx, e.id, 29, addIndexGenCol)
	case TestMultiCols:
		err = testTwoColsFramePara(ctx, e.id, ctx.CompCtx.colIIDs, ctx.CompCtx.colJIDs, addIndexMultiCols)
	default:
	}
	erChan.err = err
	erChan.finished = true
	e.PDChan <- &erChan
}

func testOneColFramePara(ctx *suiteContext, tableID int, colIDs [][]int, f func(*suiteContext, int, string, int) error) (err error) {
	tableName := "addindex.t" + strconv.Itoa(tableID)
	for _, i := range colIDs[tableID] {
		err = f(ctx, tableID, tableName, i)
		if err != nil {
			if ctx.isUnique || ctx.isPK {
				require.Contains(ctx.t, err.Error(), "Duplicate entry")
				err = nil
				continue
			}
			logutil.BgLogger().Error("[add index test] add index failed", zap.Error(err))
			require.NoError(ctx.t, err)
			break
		}
		checkResult(ctx, tableName, i, tableID)
	}
	return err
}

func testTwoColsFramePara(ctx *suiteContext, tableID int, iIDs [][]int, jIDs [][]int, f func(*suiteContext, int, string, int, int, int) error) (err error) {
	tableName := "addindex.t" + strconv.Itoa(tableID)
	indexID := 0
	for _, i := range iIDs[tableID] {
		for _, j := range jIDs[tableID] {
			err = f(ctx, tableID, tableName, indexID, i, j)
			if err != nil {
				logutil.BgLogger().Error("[add index test] add index failed", zap.Error(err))
			}
			require.NoError(ctx.t, err)
			if err == nil && i != j {
				checkResult(ctx, tableName, indexID, tableID)
			}
			indexID++
			if err != nil {
				return err
			}
		}
	}
	return err
}

func testOneIndexFramePara(ctx *suiteContext, tableID int, colID int, f func(*suiteContext, int, string, int) error) (err error) {
	tableName := "addindex.t" + strconv.Itoa(tableID)
	err = f(ctx, tableID, tableName, colID)
	if err != nil {
		logutil.BgLogger().Error("[add index test] add index failed", zap.Error(err))
	}
	require.NoError(ctx.t, err)
	if err == nil {
		if ctx.isPK {
			checkTableResult(ctx, tableName, tableID)
		} else {
			checkResult(ctx, tableName, colID, tableID)
		}
	}
	return err
}
