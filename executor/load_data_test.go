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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"fmt"
	"math"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestLoadDataWorker_initDefaultOptions(t *testing.T) {
	e := LoadDataWorker{}
	e.initDefaultOptions()
	require.Equal(t, logicalImportMode, e.importMode)
	require.Equal(t, config.ByteSize(50<<30), e.diskQuota)
	require.Equal(t, config.OpLevelRequired, e.checksum)
	require.Equal(t, true, e.addIndex)
	require.Equal(t, config.OpLevelOptional, e.analyze)
	require.Equal(t, int64(runtime.NumCPU()), e.threadCnt)
	require.Equal(t, config.ByteSize(100<<20), e.batchSize)
	require.Equal(t, unlimitedWriteSpeed, e.maxWriteSpeed)
	require.Equal(t, false, e.splitFile)
	require.Equal(t, int64(100), e.maxRecordedErrors)
	require.Equal(t, false, e.detached)

	e = LoadDataWorker{format: LoadDataFormatParquet}
	e.initDefaultOptions()
	require.Greater(t, e.threadCnt, int64(0))
	require.Equal(t, int64(math.Max(1, float64(runtime.NumCPU())*0.75)), e.threadCnt)
}

func TestLoadDataWorker_initOptions(t *testing.T) {
	cases := []struct {
		OptionStr string
		Err       error
	}{
		{OptionStr: "xx=1", Err: ErrUnknownOption},
		{OptionStr: detachedOption + "=1", Err: ErrInvalidOptionVal},
		{OptionStr: addIndexOption, Err: ErrInvalidOptionVal},
		{OptionStr: detachedOption + ", " + detachedOption, Err: ErrDuplicateOption},
		{OptionStr: importModeOption + "='logical', " + diskQuotaOption + "='100GiB'", Err: ErrLoadDataUnsupportedOption},
		{OptionStr: importModeOption + "='logical', " + checksumOption + "='optional'", Err: ErrLoadDataUnsupportedOption},
		{OptionStr: importModeOption + "='logical', " + addIndexOption + "=false", Err: ErrLoadDataUnsupportedOption},
		{OptionStr: importModeOption + "='logical', " + analyzeOption + "='optional'", Err: ErrLoadDataUnsupportedOption},

		{OptionStr: importModeOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "=1", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "=false", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "=null", Err: ErrInvalidOptionVal},

		{OptionStr: importModeOption + "='physical', " + diskQuotaOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "='physical', " + diskQuotaOption + "='220MiBxxx'", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "='physical', " + diskQuotaOption + "=false", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "='physical', " + diskQuotaOption + "=null", Err: ErrInvalidOptionVal},

		{OptionStr: importModeOption + "='physical', " + checksumOption + "=''", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "='physical', " + checksumOption + "=123", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "='physical', " + checksumOption + "=false", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "='physical', " + checksumOption + "=null", Err: ErrInvalidOptionVal},

		{OptionStr: importModeOption + "='physical', " + addIndexOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "='physical', " + addIndexOption + "=123", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "='physical', " + addIndexOption + "=null", Err: ErrInvalidOptionVal},

		{OptionStr: importModeOption + "='physical', " + analyzeOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "='physical', " + analyzeOption + "=123", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "='physical', " + analyzeOption + "=false", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "='physical', " + analyzeOption + "=null", Err: ErrInvalidOptionVal},

		{OptionStr: threadOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: threadOption + "=0", Err: ErrInvalidOptionVal},
		{OptionStr: threadOption + "=false", Err: ErrInvalidOptionVal},
		{OptionStr: threadOption + "=-100", Err: ErrInvalidOptionVal},
		{OptionStr: threadOption + "=null", Err: ErrInvalidOptionVal},

		{OptionStr: batchSizeOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: batchSizeOption + "='11aa'", Err: ErrInvalidOptionVal},
		{OptionStr: batchSizeOption + "=false", Err: ErrInvalidOptionVal},
		{OptionStr: batchSizeOption + "=null", Err: ErrInvalidOptionVal},

		{OptionStr: maxWriteSpeedOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: maxWriteSpeedOption + "='11aa'", Err: ErrInvalidOptionVal},
		{OptionStr: maxWriteSpeedOption + "=false", Err: ErrInvalidOptionVal},
		{OptionStr: maxWriteSpeedOption + "=null", Err: ErrInvalidOptionVal},

		{OptionStr: splitFileOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: splitFileOption + "=111", Err: ErrInvalidOptionVal},
		{OptionStr: splitFileOption + "='false'", Err: ErrInvalidOptionVal},
		{OptionStr: splitFileOption + "=null", Err: ErrInvalidOptionVal},

		{OptionStr: recordErrorsOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: recordErrorsOption + "='111aa'", Err: ErrInvalidOptionVal},
		{OptionStr: recordErrorsOption + "=-123", Err: ErrInvalidOptionVal},
		{OptionStr: recordErrorsOption + "=null", Err: ErrInvalidOptionVal},
	}

	ctx := mock.NewContext()
	defer ctx.Close()

	convertOptions := func(inOptions []*ast.LoadDataOpt) []*plannercore.LoadDataOpt {
		options := []*plannercore.LoadDataOpt{}
		var err error
		for _, opt := range inOptions {
			loadDataOpt := plannercore.LoadDataOpt{Name: opt.Name}
			if opt.Value != nil {
				loadDataOpt.Value, err = expression.RewriteSimpleExprWithNames(ctx, opt.Value, nil, nil)
				require.NoError(t, err)
			}
			options = append(options, &loadDataOpt)
		}
		return options
	}

	sqlTemplate := "load data infile '/xx' into table t with %s"
	p := parser.New()
	for _, c := range cases {
		sql := fmt.Sprintf(sqlTemplate, c.OptionStr)
		stmt, err2 := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err2, sql)
		e := LoadDataWorker{Ctx: ctx}
		err := e.initOptions(convertOptions(stmt.(*ast.LoadDataStmt).Options))
		require.ErrorIs(t, err, c.Err, sql)
	}
	e := LoadDataWorker{Ctx: ctx}
	sql := fmt.Sprintf(sqlTemplate, importModeOption+"='physical', "+
		diskQuotaOption+"='100gib', "+
		checksumOption+"='optional', "+
		addIndexOption+"=false, "+
		analyzeOption+"='required', "+
		threadOption+"='100000', "+
		batchSizeOption+"='100mib', "+
		maxWriteSpeedOption+"='200mib', "+
		splitFileOption+"=true, "+
		recordErrorsOption+"=123, "+
		detachedOption)
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err, sql)
	err = e.initOptions(convertOptions(stmt.(*ast.LoadDataStmt).Options))
	require.NoError(t, err, sql)
	require.Equal(t, physicalImportMode, e.importMode, sql)
	require.Equal(t, config.ByteSize(100<<30), e.diskQuota, sql)
	require.Equal(t, config.OpLevelOptional, e.checksum, sql)
	require.False(t, e.addIndex, sql)
	require.Equal(t, config.OpLevelRequired, e.analyze, sql)
	require.Equal(t, int64(runtime.NumCPU()), e.threadCnt, sql)
	require.Equal(t, config.ByteSize(100<<20), e.batchSize, sql)
	require.Equal(t, config.ByteSize(200<<20), e.maxWriteSpeed, sql)
	require.True(t, e.splitFile, sql)
	require.Equal(t, int64(123), e.maxRecordedErrors, sql)
	require.True(t, e.detached, sql)
}

func TestLoadDataWorker_adjustOptions(t *testing.T) {
	e := LoadDataWorker{
		diskQuota:     1,
		threadCnt:     100000000,
		batchSize:     1,
		maxWriteSpeed: 10,
	}
	e.adjustOptions()
	require.Equal(t, minDiskQuota, e.diskQuota)
	require.Equal(t, int64(runtime.NumCPU()), e.threadCnt)
	require.Equal(t, minBatchSize, e.batchSize)
	require.Equal(t, minWriteSpeed, e.maxWriteSpeed)
}
