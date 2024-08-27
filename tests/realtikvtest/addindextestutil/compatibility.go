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

package addindextestutil

import (
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
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
	IsMultiSchemaChange bool
	IsConcurrentDDL     bool
	IsPiTR              bool
	executor            []*executor
	colIIDs             [][]int
	colJIDs             [][]int
	tType               testType
}

// InitCompCtx inits SuiteContext for compatibility tests.
func InitCompCtx(t *testing.T) *SuiteContext {
	ctx := InitTest(t)
	InitCompCtxParams(ctx)
	return ctx
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

// InitCompCtxParams inits params for compatibility tests.
func InitCompCtxParams(ctx *SuiteContext) {
	ctx.CompCtx = &compCtx
	compCtx.IsConcurrentDDL = false
	compCtx.IsMultiSchemaChange = false
	compCtx.IsPiTR = false
}

// InitConcurrentDDLTest inits params for compatibility tests with concurrent ddl.
func InitConcurrentDDLTest(t *testing.T, colIIDs [][]int, colJIDs [][]int, tType testType) *SuiteContext {
	ctx := InitCompCtx(t)
	ctx.CompCtx.IsConcurrentDDL = true
	ctx.CompCtx.tType = tType
	ctx.CompCtx.colIIDs = colIIDs
	ctx.CompCtx.colJIDs = colJIDs
	return ctx
}

// Start start the compatibility tests.
func (cCtx *CompatibilityContext) Start(ctx *SuiteContext) {
	cCtx.executor = cCtx.executor[:0]
	for i := 0; i < 3; i++ {
		er := newExecutor(i)
		er.tk = ctx.getTestKit()
		cCtx.executor = append(cCtx.executor, er)
		go cCtx.executor[i].run(ctx)
	}
}

// Stop stop the compatibility tests.
func (cCtx *CompatibilityContext) Stop(ctx *SuiteContext) error {
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

func (e *executor) run(ctx *SuiteContext) {
	var (
		err    error
		erChan paraDDLChan
	)
	switch ctx.CompCtx.tType {
	case TestNonUnique:
		err = testOneColFramePara(ctx, e.id, ctx.CompCtx.colIIDs, AddIndexNonUnique)
	case TestUnique:
		err = testOneColFramePara(ctx, e.id, ctx.CompCtx.colIIDs, AddIndexUnique)
	case TestPK:
		err = testOneIndexFramePara(ctx, e.id, 0, AddIndexPK)
	case TestGenIndex:
		err = testOneIndexFramePara(ctx, e.id, 29, AddIndexGenCol)
	case TestMultiCols:
		err = testTwoColsFramePara(ctx, e.id, ctx.CompCtx.colIIDs, ctx.CompCtx.colJIDs, AddIndexMultiCols)
	default:
	}
	erChan.err = err
	erChan.finished = true
	e.PDChan <- &erChan
}

func testOneColFramePara(ctx *SuiteContext, tableID int, colIDs [][]int, f func(*SuiteContext, int, string, int) error) (err error) {
	tableName := "addindex.t" + strconv.Itoa(tableID)
	for _, i := range colIDs[tableID] {
		err = f(ctx, tableID, tableName, i)
		if err != nil {
			if ctx.isUnique || ctx.isPK {
				require.Contains(ctx.t, err.Error(), "Duplicate entry")
				err = nil
				continue
			}
			logutil.BgLogger().Error("add index failed", zap.String("category", "add index test"), zap.Error(err))
			require.NoError(ctx.t, err)
			break
		}
		checkResult(ctx, tableName, i, tableID)
	}
	return err
}

func testTwoColsFramePara(ctx *SuiteContext, tableID int, iIDs [][]int, jIDs [][]int, f func(*SuiteContext, int, string, int, int, int) error) (err error) {
	tableName := "addindex.t" + strconv.Itoa(tableID)
	indexID := 0
	for _, i := range iIDs[tableID] {
		for _, j := range jIDs[tableID] {
			err = f(ctx, tableID, tableName, indexID, i, j)
			if err != nil {
				logutil.BgLogger().Error("add index failed", zap.String("category", "add index test"), zap.Error(err))
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

func testOneIndexFramePara(ctx *SuiteContext, tableID int, colID int, f func(*SuiteContext, int, string, int) error) (err error) {
	tableName := "addindex.t" + strconv.Itoa(tableID)
	err = f(ctx, tableID, tableName, colID)
	if err != nil {
		logutil.BgLogger().Error("add index failed", zap.String("category", "add index test"), zap.Error(err))
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
