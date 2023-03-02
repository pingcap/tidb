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
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
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

		{OptionStr: importModeOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: importModeOption + "=1", Err: ErrInvalidOptionVal},

		{OptionStr: diskQuotaOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: diskQuotaOption + "='220MiBxxx'", Err: ErrInvalidOptionVal},
		{OptionStr: diskQuotaOption + "=false", Err: ErrInvalidOptionVal},

		{OptionStr: checksumOption + "=''", Err: ErrInvalidOptionVal},
		{OptionStr: checksumOption + "=123", Err: ErrInvalidOptionVal},

		{OptionStr: addIndexOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: addIndexOption + "=123", Err: ErrInvalidOptionVal},

		{OptionStr: analyzeOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: analyzeOption + "=123", Err: ErrInvalidOptionVal},

		{OptionStr: threadOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: threadOption + "=0", Err: ErrInvalidOptionVal},
		{OptionStr: threadOption + "=-100", Err: ErrInvalidOptionVal},

		{OptionStr: batchSizeOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: batchSizeOption + "=false", Err: ErrInvalidOptionVal},

		{OptionStr: maxWriteSpeedOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: maxWriteSpeedOption + "=false", Err: ErrInvalidOptionVal},

		{OptionStr: splitFileOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: splitFileOption + "=111", Err: ErrInvalidOptionVal},

		{OptionStr: recordErrorsOption + "='aa'", Err: ErrInvalidOptionVal},
		{OptionStr: recordErrorsOption + "='111aa'", Err: ErrInvalidOptionVal},
		{OptionStr: recordErrorsOption + "=-123", Err: ErrInvalidOptionVal},
	}
	ctx := mock.NewContext()
	defer ctx.Close()
	sqlTemplate := "load data infile '/xx' into table t with %s"
	p := parser.New()
	for _, c := range cases {
		sql := fmt.Sprintf(sqlTemplate, c.OptionStr)
		stmt, err2 := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err2, sql)
		e := LoadDataWorker{Ctx: ctx}
		err := e.initOptions((stmt.(*ast.LoadDataStmt)).Options)
		require.ErrorIs(t, err, c.Err, sql)
	}
	e := LoadDataWorker{Ctx: ctx}
	sql := fmt.Sprintf(sqlTemplate, importModeOption+"='logical', "+
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
	err = e.initOptions((stmt.(*ast.LoadDataStmt)).Options)
	require.NoError(t, err, sql)
	require.Equal(t, logicalImportMode, e.importMode, sql)
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
