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
	"runtime"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestInitDefaultOptions(t *testing.T) {
	plan := &Plan{}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/importer/mockNumCpu", "return(1)"))
	plan.initDefaultOptions()
	require.Equal(t, config.ByteSize(0), plan.DiskQuota)
	require.Equal(t, config.OpLevelRequired, plan.Checksum)
	require.Equal(t, int64(1), plan.ThreadCnt)
	require.Equal(t, unlimitedWriteSpeed, plan.MaxWriteSpeed)
	require.Equal(t, false, plan.SplitFile)
	require.Equal(t, int64(100), plan.MaxRecordedErrors)
	require.Equal(t, false, plan.Detached)
	require.Equal(t, "utf8mb4", *plan.Charset)
	require.Equal(t, false, plan.DisableTiKVImportMode)
	require.Equal(t, config.ByteSize(defaultMaxEngineSize), plan.MaxEngineSize)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/importer/mockNumCpu", "return(10)"))
	plan.initDefaultOptions()
	require.Equal(t, int64(5), plan.ThreadCnt)
}

// for negative case see TestImportIntoOptionsNegativeCase
func TestInitOptionsPositiveCase(t *testing.T) {
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
	plan := &Plan{Format: DataFormatCSV}
	sql := fmt.Sprintf(sqlTemplate, characterSetOption+"='utf8', "+
		fieldsTerminatedByOption+"='aaa', "+
		fieldsEnclosedByOption+"='|', "+
		fieldsEscapedByOption+"='', "+
		fieldsDefinedNullByOption+"='N', "+
		linesTerminatedByOption+"='END', "+
		skipRowsOption+"=3, "+
		diskQuotaOption+"='100gib', "+
		checksumTableOption+"='optional', "+
		threadOption+"=100000, "+
		maxWriteSpeedOption+"='200mib', "+
		splitFileOption+", "+
		recordErrorsOption+"=123, "+
		detachedOption+", "+
		disableTiKVImportModeOption+", "+
		maxEngineSizeOption+"='100gib'",
	)
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
	require.Equal(t, int64(runtime.GOMAXPROCS(0)), plan.ThreadCnt, sql) // it's adjusted to the number of CPUs
	require.Equal(t, config.ByteSize(200<<20), plan.MaxWriteSpeed, sql)
	require.True(t, plan.SplitFile, sql)
	require.Equal(t, int64(123), plan.MaxRecordedErrors, sql)
	require.True(t, plan.Detached, sql)
	require.True(t, plan.DisableTiKVImportMode, sql)
	require.Equal(t, config.ByteSize(100<<30), plan.MaxEngineSize, sql)
}

func TestAdjustOptions(t *testing.T) {
	plan := &Plan{
		DiskQuota:     1,
		ThreadCnt:     100000000,
		MaxWriteSpeed: 10,
	}
	plan.adjustOptions()
	require.Equal(t, int64(runtime.GOMAXPROCS(0)), plan.ThreadCnt)
	require.Equal(t, config.ByteSize(10), plan.MaxWriteSpeed) // not adjusted
}

func TestAdjustDiskQuota(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/common/GetStorageSize", "return(2048)")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/common/GetStorageSize")
	}()
	d := t.TempDir()
	require.Equal(t, int64(1638), adjustDiskQuota(0, d, logutil.BgLogger()))
	require.Equal(t, int64(1), adjustDiskQuota(1, d, logutil.BgLogger()))
	require.Equal(t, int64(1638), adjustDiskQuota(2000, d, logutil.BgLogger()))
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
