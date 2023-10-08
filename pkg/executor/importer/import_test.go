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
	"context"
	"fmt"
	"net/url"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestInitDefaultOptions(t *testing.T) {
	plan := &Plan{}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/mockNumCpu", "return(1)"))
	variable.CloudStorageURI.Store("s3://bucket/path")
	t.Cleanup(func() {
		variable.CloudStorageURI.Store("")
	})
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
	require.Equal(t, "s3://bucket/path", plan.CloudStorageURI)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/mockNumCpu", "return(10)"))
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
	sql := fmt.Sprintf(sqlTemplate, characterSetOption+"='utf8', "+
		fieldsTerminatedByOption+"='aaa', "+
		fieldsEnclosedByOption+"='|', "+
		fieldsEscapedByOption+"='', "+
		fieldsDefinedNullByOption+"='N', "+
		linesTerminatedByOption+"='END', "+
		skipRowsOption+"=1, "+
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
	plan := &Plan{Format: DataFormatCSV}
	err = plan.initOptions(ctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql)
	require.Equal(t, "utf8", *plan.Charset, sql)
	require.Equal(t, "aaa", plan.FieldsTerminatedBy, sql)
	require.Equal(t, "|", plan.FieldsEnclosedBy, sql)
	require.Equal(t, "", plan.FieldsEscapedBy, sql)
	require.Equal(t, []string{"N"}, plan.FieldNullDef, sql)
	require.Equal(t, "END", plan.LinesTerminatedBy, sql)
	require.Equal(t, uint64(1), plan.IgnoreLines, sql)
	require.Equal(t, config.ByteSize(100<<30), plan.DiskQuota, sql)
	require.Equal(t, config.OpLevelOptional, plan.Checksum, sql)
	require.Equal(t, int64(runtime.GOMAXPROCS(0)), plan.ThreadCnt, sql) // it's adjusted to the number of CPUs
	require.Equal(t, config.ByteSize(200<<20), plan.MaxWriteSpeed, sql)
	require.True(t, plan.SplitFile, sql)
	require.Equal(t, int64(123), plan.MaxRecordedErrors, sql)
	require.True(t, plan.Detached, sql)
	require.True(t, plan.DisableTiKVImportMode, sql)
	require.Equal(t, config.ByteSize(100<<30), plan.MaxEngineSize, sql)
	require.Empty(t, plan.CloudStorageURI, sql)

	// set cloud storage uri
	variable.CloudStorageURI.Store("s3://bucket/path")
	t.Cleanup(func() {
		variable.CloudStorageURI.Store("")
	})
	plan = &Plan{Format: DataFormatCSV}
	err = plan.initOptions(ctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql)
	require.Equal(t, "s3://bucket/path", plan.CloudStorageURI, sql)

	// override cloud storage uri using option
	sql2 := sql + ", " + cloudStorageURIOption + "='s3://bucket/path2'"
	stmt, err = p.ParseOneStmt(sql2, "", "")
	require.NoError(t, err, sql2)
	plan = &Plan{Format: DataFormatCSV}
	err = plan.initOptions(ctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql2)
	require.Equal(t, "s3://bucket/path2", plan.CloudStorageURI, sql2)
	// override with gs
	sql3 := sql + ", " + cloudStorageURIOption + "='gs://bucket/path2'"
	stmt, err = p.ParseOneStmt(sql3, "", "")
	require.NoError(t, err, sql3)
	plan = &Plan{Format: DataFormatCSV}
	err = plan.initOptions(ctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql3)
	require.Equal(t, "gs://bucket/path2", plan.CloudStorageURI, sql3)
	// override with empty string, force use local sort
	sql4 := sql + ", " + cloudStorageURIOption + "=''"
	stmt, err = p.ParseOneStmt(sql4, "", "")
	require.NoError(t, err, sql4)
	plan = &Plan{Format: DataFormatCSV}
	err = plan.initOptions(ctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql4)
	require.Equal(t, "", plan.CloudStorageURI, sql4)
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

func TestGetFileRealSize(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/mydump/SampleFileCompressPercentage", "return(250)")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/mydump/SampleFileCompressPercentage")
	}()
	fileMeta := mydump.SourceFileMeta{Compression: mydump.CompressionNone, FileSize: 100}
	c := &LoadDataController{logger: log.L()}
	require.Equal(t, int64(100), c.getFileRealSize(context.Background(), fileMeta, nil))
	fileMeta.Compression = mydump.CompressionGZ
	require.Equal(t, int64(250), c.getFileRealSize(context.Background(), fileMeta, nil))
	err = failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/mydump/SampleFileCompressPercentage", `return("test err")`)
	require.NoError(t, err)
	require.Equal(t, int64(100), c.getFileRealSize(context.Background(), fileMeta, nil))
}

func urlEqual(t *testing.T, expected, actual string) {
	urlExpected, err := url.Parse(expected)
	require.NoError(t, err)
	urlGot, err := url.Parse(actual)
	require.NoError(t, err)
	// order of query parameters might change
	require.Equal(t, urlExpected.Query(), urlGot.Query())
	urlExpected.RawQuery, urlGot.RawQuery = "", ""
	require.Equal(t, urlExpected.String(), urlGot.String())
}

func TestInitParameters(t *testing.T) {
	// test redacted
	p := &Plan{
		Format: DataFormatCSV,
		Path:   "s3://bucket/path?access-key=111111&secret-access-key=222222",
	}
	require.NoError(t, p.initParameters(&plannercore.ImportInto{
		Options: []*plannercore.LoadDataOpt{
			{
				Name: cloudStorageURIOption,
				Value: &expression.Constant{
					Value: types.NewStringDatum("s3://this-is-for-storage/path?access-key=aaaaaa&secret-access-key=bbbbbb"),
				},
			},
		},
	}))
	urlEqual(t, "s3://bucket/path?access-key=xxxxxx&secret-access-key=xxxxxx", p.Parameters.FileLocation)
	require.Len(t, p.Parameters.Options, 1)
	urlEqual(t, "s3://this-is-for-storage/path?access-key=xxxxxx&secret-access-key=xxxxxx",
		p.Parameters.Options[cloudStorageURIOption].(string))

	// test other options
	require.NoError(t, p.initParameters(&plannercore.ImportInto{
		Options: []*plannercore.LoadDataOpt{
			{
				Name: detachedOption,
			},
			{
				Name: threadOption,
				Value: &expression.Constant{
					Value: types.NewIntDatum(3),
				},
			},
		},
	}))
	require.Len(t, p.Parameters.Options, 2)
	require.Contains(t, p.Parameters.Options, detachedOption)
	require.Equal(t, "3", p.Parameters.Options[threadOption])
}

func TestGetLocalBackendCfg(t *testing.T) {
	c := &LoadDataController{
		Plan: &Plan{},
	}
	cfg := c.getLocalBackendCfg("http://1.1.1.1:1234", "/tmp")
	require.Equal(t, "http://1.1.1.1:1234", cfg.PDAddr)
	require.Equal(t, "/tmp", cfg.LocalStoreDir)
	require.True(t, cfg.DisableAutomaticCompactions)
	require.Zero(t, cfg.RaftKV2SwitchModeDuration)

	c.Plan.IsRaftKV2 = true
	cfg = c.getLocalBackendCfg("http://1.1.1.1:1234", "/tmp")
	require.Greater(t, cfg.RaftKV2SwitchModeDuration, time.Duration(0))
	require.Equal(t, config.DefaultSwitchTiKVModeInterval, cfg.RaftKV2SwitchModeDuration)
}
