// Copyright 2023 PingCAP, Inc.
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

package importer

import (
	"fmt"
	"math"
	"runtime"
	"testing"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestInitDefaultOptions(t *testing.T) {
	plan := &Plan{}
	plan.initDefaultOptions()
	require.Equal(t, config.ByteSize(50<<30), plan.DiskQuota)
	require.Equal(t, config.OpLevelRequired, plan.Checksum)
	require.Equal(t, config.OpLevelOptional, plan.Analyze)
	require.Equal(t, int64(runtime.NumCPU()), plan.ThreadCnt)
	require.Equal(t, unlimitedWriteSpeed, plan.MaxWriteSpeed)
	require.Equal(t, false, plan.SplitFile)
	require.Equal(t, int64(100), plan.MaxRecordedErrors)
	require.Equal(t, false, plan.Detached)
	require.Equal(t, "utf8mb4", *plan.Charset)

	plan = &Plan{Format: DataFormatParquet}
	plan.initDefaultOptions()
	require.Greater(t, plan.ThreadCnt, int64(0))
	require.Equal(t, int64(math.Max(1, float64(runtime.NumCPU())*0.75)), plan.ThreadCnt)
}

func TestInitOptions(t *testing.T) {
	cases := []struct {
		OptionStr string
		Err       error
	}{
		{OptionStr: "xx=1", Err: exeerrors.ErrUnknownOption},
		{OptionStr: detachedOption + "=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: characterSetOption, Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: detachedOption + ", " + detachedOption, Err: exeerrors.ErrDuplicateOption},

		{OptionStr: characterSetOption + "=true", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: characterSetOption + "=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: characterSetOption + "=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: characterSetOption + "=true", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: characterSetOption + "=''", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: characterSetOption + "='aa'", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: fieldsTerminatedByOption + "=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: fieldsTerminatedByOption + "=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: fieldsTerminatedByOption + "=true", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: fieldsTerminatedByOption + "=''", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: fieldsEnclosedByOption + "=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: fieldsEnclosedByOption + "='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: fieldsEnclosedByOption + "=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: fieldsEnclosedByOption + "=true", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: fieldsEscapedByOption + "=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: fieldsEscapedByOption + "='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: fieldsEscapedByOption + "=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: fieldsEscapedByOption + "=true", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: fieldsDefinedNullByOption + "=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: fieldsDefinedNullByOption + "=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: fieldsDefinedNullByOption + "=true", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: linesTerminatedByOption + "=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: linesTerminatedByOption + "=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: linesTerminatedByOption + "=true", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: linesTerminatedByOption + "=''", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: skipRowsOption + "=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: skipRowsOption + "=''", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: skipRowsOption + "=-1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: skipRowsOption + "=true", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: diskQuotaOption + "='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: diskQuotaOption + "='220MiBxxx'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: diskQuotaOption + "=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: diskQuotaOption + "=false", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: diskQuotaOption + "=null", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: checksumTableOption + "=''", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: checksumTableOption + "=123", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: checksumTableOption + "=false", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: checksumTableOption + "=null", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: analyzeTableOption + "='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: analyzeTableOption + "=123", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: analyzeTableOption + "=false", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: analyzeTableOption + "=null", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: threadOption + "='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: threadOption + "=0", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: threadOption + "=false", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: threadOption + "=-100", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: threadOption + "=null", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: maxWriteSpeedOption + "='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: maxWriteSpeedOption + "='11aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: maxWriteSpeedOption + "=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: maxWriteSpeedOption + "=-1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: maxWriteSpeedOption + "=false", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: splitFileOption + "='aa'", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: recordErrorsOption + "='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: recordErrorsOption + "='111aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: recordErrorsOption + "=-123", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: recordErrorsOption + "=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: recordErrorsOption + "=true", Err: exeerrors.ErrInvalidOptionVal},
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

	sqlTemplate := "import into t from '/file.csv' with %s"
	p := parser.New()
	for _, c := range cases {
		sql := fmt.Sprintf(sqlTemplate, c.OptionStr)
		stmt, err2 := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err2, sql)
		plan := &Plan{}
		err := plan.initOptions(ctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
		require.ErrorIs(t, err, c.Err, sql)
	}
	plan := &Plan{}
	sql := fmt.Sprintf(sqlTemplate, characterSetOption+"='utf8', "+
		fieldsTerminatedByOption+"='aaa', "+
		fieldsEnclosedByOption+"='|', "+
		fieldsEscapedByOption+"='', "+
		fieldsDefinedNullByOption+"='N', "+
		linesTerminatedByOption+"='END', "+
		skipRowsOption+"=3, "+
		diskQuotaOption+"='100gib', "+
		checksumTableOption+"='optional', "+
		analyzeTableOption+"='required', "+
		threadOption+"=100000, "+
		maxWriteSpeedOption+"='200mib', "+
		splitFileOption+", "+
		recordErrorsOption+"=123, "+
		detachedOption)
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err, sql)
	err = plan.initOptions(ctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql)
	require.Equal(t, "utf8", *plan.Charset, sql)
	require.Equal(t, "aaa", plan.FieldsTerminatedBy, sql)
	require.Equal(t, "|", plan.FieldsEnclosedBy, sql)
	require.Equal(t, "", plan.FieldsEscapedBy, sql)
	require.Equal(t, []string{"N"}, plan.FieldNullDef, sql)
	require.Equal(t, "END", plan.LinesTerminatedBy, sql)
	require.Equal(t, uint64(3), plan.IgnoreLines, sql)
	require.Equal(t, config.ByteSize(100<<30), plan.DiskQuota, sql)
	require.Equal(t, config.OpLevelOptional, plan.Checksum, sql)
	require.Equal(t, config.OpLevelRequired, plan.Analyze, sql)
	require.Equal(t, int64(runtime.NumCPU()), plan.ThreadCnt, sql) // it's adjusted to the number of CPUs
	require.Equal(t, config.ByteSize(200<<20), plan.MaxWriteSpeed, sql)
	require.True(t, plan.SplitFile, sql)
	require.Equal(t, int64(123), plan.MaxRecordedErrors, sql)
	require.True(t, plan.Detached, sql)
}

func TestAdjustOptions(t *testing.T) {
	plan := &Plan{
		DiskQuota:     1,
		ThreadCnt:     100000000,
		MaxWriteSpeed: 10,
	}
	plan.adjustOptions()
	require.Equal(t, minDiskQuota, plan.DiskQuota)
	require.Equal(t, int64(runtime.NumCPU()), plan.ThreadCnt)
	require.Equal(t, config.ByteSize(10), plan.MaxWriteSpeed) // not adjusted
}

func TestGetMsgFromBRError(t *testing.T) {
	var berr error = berrors.ErrStorageInvalidConfig
	require.Equal(t, "[BR:ExternalStorage:ErrStorageInvalidConfig]invalid external storage config", berr.Error())
	require.Equal(t, "invalid external storage config", GetMsgFromBRError(berr))
	berr = errors.Annotatef(berr, "some message about error reason")
	require.Equal(t, "some message about error reason: [BR:ExternalStorage:ErrStorageInvalidConfig]invalid external storage config", berr.Error())
	require.Equal(t, "some message about error reason", GetMsgFromBRError(berr))
}

func TestASTArgsFromStmt(t *testing.T) {
	stmt := "IMPORT INTO tb (a, é) FROM 'gs://test-load/test.tsv';"
	stmtNode, err := parser.New().ParseOneStmt(stmt, "latin1", "latin1_bin")
	require.NoError(t, err)
	text := stmtNode.Text()
	require.Equal(t, stmt, text)
	astArgs, err := ASTArgsFromStmt(text)
	require.NoError(t, err)
	importIntoStmt := stmtNode.(*ast.ImportIntoStmt)
	require.Equal(t, astArgs.ColumnAssignments, importIntoStmt.ColumnAssignments)
	require.Equal(t, astArgs.ColumnsAndUserVars, importIntoStmt.ColumnsAndUserVars)
}
