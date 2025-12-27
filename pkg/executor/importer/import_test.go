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
	"os"
	"os/user"
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/expression"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

func TestInitDefaultOptions(t *testing.T) {
	plan := &Plan{
		DataSourceType: DataSourceTypeQuery,
	}
	plan.initDefaultOptions(context.Background(), 10, nil)
	require.Equal(t, 2, plan.ThreadCnt)

	plan = &Plan{
		DataSourceType: DataSourceTypeFile,
	}
	vardef.CloudStorageURI.Store("s3://bucket/path")
	t.Cleanup(func() {
		vardef.CloudStorageURI.Store("")
	})
	plan.initDefaultOptions(context.Background(), 1, nil)
	require.Equal(t, config.ByteSize(0), plan.DiskQuota)
	require.Equal(t, config.OpLevelRequired, plan.Checksum)
	require.Equal(t, 1, plan.ThreadCnt)
	require.Equal(t, unlimitedWriteSpeed, plan.MaxWriteSpeed)
	require.Equal(t, false, plan.SplitFile)
	require.Equal(t, int64(100), plan.MaxRecordedErrors)
	require.Equal(t, false, plan.Detached)
	require.Equal(t, "utf8mb4", *plan.Charset)
	require.Equal(t, false, plan.DisableTiKVImportMode)
	if kerneltype.IsNextGen() {
		require.Equal(t, config.DefaultBatchSize, plan.MaxEngineSize)
	} else {
		require.Equal(t, config.ByteSize(defaultMaxEngineSize), plan.MaxEngineSize)
	}

	require.Equal(t, "s3://bucket/path", plan.CloudStorageURI)

	plan.initDefaultOptions(context.Background(), 10, nil)
	require.Equal(t, 5, plan.ThreadCnt)
}

// for negative case see TestImportIntoOptionsNegativeCase
func TestInitOptionsPositiveCase(t *testing.T) {
	sctx := mock.NewContext()
	defer sctx.Close()
	ctx := tikvutil.WithInternalSourceType(context.Background(), tidbkv.InternalImportInto)

	convertOptions := func(inOptions []*ast.LoadDataOpt) []*plannercore.LoadDataOpt {
		options := []*plannercore.LoadDataOpt{}
		var err error
		for _, opt := range inOptions {
			loadDataOpt := plannercore.LoadDataOpt{Name: opt.Name}
			if opt.Value != nil {
				loadDataOpt.Value, err = plannerutil.RewriteAstExprWithPlanCtx(sctx, opt.Value, nil, nil, false)
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
		maxEngineSizeOption+"='100gib', "+
		disablePrecheckOption,
	)
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err, sql)
	plan := &Plan{Format: DataFormatCSV}
	err = plan.initOptions(ctx, sctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
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
	require.Equal(t, runtime.GOMAXPROCS(0), plan.ThreadCnt, sql) // it's adjusted to the number of CPUs
	require.Equal(t, config.ByteSize(200<<20), plan.MaxWriteSpeed, sql)
	require.True(t, plan.SplitFile, sql)
	require.Equal(t, int64(123), plan.MaxRecordedErrors, sql)
	require.True(t, plan.Detached, sql)
	require.True(t, plan.DisableTiKVImportMode, sql)
	require.Equal(t, config.ByteSize(100<<30), plan.MaxEngineSize, sql)
	require.Empty(t, plan.CloudStorageURI, sql)
	require.True(t, plan.DisablePrecheck, sql)

	// set cloud storage uri
	vardef.CloudStorageURI.Store("s3://bucket/path")
	t.Cleanup(func() {
		vardef.CloudStorageURI.Store("")
	})
	plan = &Plan{Format: DataFormatCSV}
	err = plan.initOptions(ctx, sctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql)
	require.Equal(t, "s3://bucket/path", plan.CloudStorageURI, sql)

	// override cloud storage uri using option
	sql2 := sql + ", " + cloudStorageURIOption + "='s3://bucket/path2'"
	stmt, err = p.ParseOneStmt(sql2, "", "")
	require.NoError(t, err, sql2)
	plan = &Plan{Format: DataFormatCSV}
	err = plan.initOptions(ctx, sctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql2)
	require.Equal(t, "s3://bucket/path2", plan.CloudStorageURI, sql2)
	// override with gs
	sql3 := sql + ", " + cloudStorageURIOption + "='gs://bucket/path2'"
	stmt, err = p.ParseOneStmt(sql3, "", "")
	require.NoError(t, err, sql3)
	plan = &Plan{Format: DataFormatCSV}
	err = plan.initOptions(ctx, sctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql3)
	require.Equal(t, "gs://bucket/path2", plan.CloudStorageURI, sql3)
	// override with empty string, force use local sort
	sql4 := sql + ", " + cloudStorageURIOption + "=''"
	stmt, err = p.ParseOneStmt(sql4, "", "")
	require.NoError(t, err, sql4)
	plan = &Plan{Format: DataFormatCSV}
	err = plan.initOptions(ctx, sctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql4)
	require.Equal(t, "", plan.CloudStorageURI, sql4)
}

func TestAdjustOptions(t *testing.T) {
	plan := &Plan{
		DiskQuota:      1,
		ThreadCnt:      100000000,
		MaxWriteSpeed:  10,
		DataSourceType: DataSourceTypeFile,
	}
	plan.adjustOptions(16)
	require.Equal(t, 16, plan.ThreadCnt)
	require.Equal(t, config.ByteSize(10), plan.MaxWriteSpeed) // not adjusted
	require.False(t, plan.DisableTiKVImportMode)

	plan.ThreadCnt = 100000000
	plan.DataSourceType = DataSourceTypeQuery
	plan.adjustOptions(16)
	require.Equal(t, 32, plan.ThreadCnt)
	require.False(t, plan.DisableTiKVImportMode)

	plan.CloudStorageURI = "s3://bucket/path"
	plan.adjustOptions(16)
	require.True(t, plan.DisableTiKVImportMode)
}

func TestAdjustDiskQuota(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tidb/pkg/lightning/common/GetStorageSize", "return(2048)")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/lightning/common/GetStorageSize")
	}()
	d := t.TempDir()
	require.Equal(t, int64(1638), adjustDiskQuota(0, d, logutil.BgLogger()))
	require.Equal(t, int64(1), adjustDiskQuota(1, d, logutil.BgLogger()))
	require.Equal(t, int64(1638), adjustDiskQuota(2000, d, logutil.BgLogger()))
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
	cfg := c.getLocalBackendCfg("", "http://1.1.1.1:1234", "/tmp")
	require.Equal(t, "http://1.1.1.1:1234", cfg.PDAddr)
	require.Equal(t, "/tmp", cfg.LocalStoreDir)
	require.True(t, cfg.DisableAutomaticCompactions)
	require.Zero(t, cfg.RaftKV2SwitchModeDuration)

	c.Plan.IsRaftKV2 = true
	cfg = c.getLocalBackendCfg("", "http://1.1.1.1:1234", "/tmp")
	require.Greater(t, cfg.RaftKV2SwitchModeDuration, time.Duration(0))
	require.Equal(t, config.DefaultSwitchTiKVModeInterval, cfg.RaftKV2SwitchModeDuration)
}

func TestInitCompressedFiles(t *testing.T) {
	username, err := user.Current()
	require.NoError(t, err)
	if username.Name == "root" {
		t.Skip("it cannot run as root")
	}
	tempDir := t.TempDir()
	ctx := context.Background()

	for i := range 2048 {
		fileName := filepath.Join(tempDir, fmt.Sprintf("test_%d.csv.gz", i))
		require.NoError(t, os.WriteFile(fileName, []byte{}, 0o644))
	}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/lightning/mydump/SampleFileCompressPercentage", `return(250)`)
	c := LoadDataController{
		Plan: &Plan{
			Format:         DataFormatCSV,
			InImportInto:   true,
			Charset:        &defaultCharacterSet,
			LineFieldsInfo: newDefaultLineFieldsInfo(),
			FieldNullDef:   defaultFieldNullDef,
			Parameters:     &ImportParameters{},
		},
		logger: zap.NewExample(),
	}

	c.Path = filepath.Join(tempDir, "*.gz")
	require.NoError(t, c.InitDataFiles(ctx))
}

func TestSupportedSuffixForServerDisk(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("nextgen doesn't support import from server disk")
	}
	username, err := user.Current()
	require.NoError(t, err)
	if username.Name == "root" {
		t.Skip("it cannot run as root")
	}
	tempDir := t.TempDir()
	ctx := context.Background()

	fileName := filepath.Join(tempDir, "test.csv")
	require.NoError(t, os.WriteFile(fileName, []byte{}, 0o644))
	fileName2 := filepath.Join(tempDir, "test.csv.gz")
	require.NoError(t, os.WriteFile(fileName2, []byte{}, 0o644))
	c := LoadDataController{
		Plan: &Plan{
			Format:         DataFormatCSV,
			InImportInto:   true,
			Charset:        &defaultCharacterSet,
			LineFieldsInfo: newDefaultLineFieldsInfo(),
			FieldNullDef:   defaultFieldNullDef,
			Parameters:     &ImportParameters{},
		},
		logger: zap.NewExample(),
	}
	// no suffix
	c.Path = filepath.Join(tempDir, "test")
	require.ErrorIs(t, c.InitDataFiles(ctx), exeerrors.ErrLoadDataInvalidURI)
	// unknown suffix
	c.Path = filepath.Join(tempDir, "test.abc")
	require.ErrorIs(t, c.InitDataFiles(ctx), exeerrors.ErrLoadDataInvalidURI)
	c.Path = fileName
	require.NoError(t, c.InitDataFiles(ctx))
	c.Path = fileName2
	require.NoError(t, c.InitDataFiles(ctx))

	var allData []string
	for i := range 3 {
		fileName := fmt.Sprintf("server-%d.csv", i)
		var content []byte
		rowCnt := 2
		for j := range rowCnt {
			content = append(content, fmt.Appendf(nil, "%d,test-%d\n", i*rowCnt+j, i*rowCnt+j)...)
			allData = append(allData, fmt.Sprintf("%d test-%d", i*rowCnt+j, i*rowCnt+j))
		}
		require.NoError(t, os.WriteFile(path.Join(tempDir, fileName), content, 0o644))
	}
	// directory without permission
	require.NoError(t, os.MkdirAll(path.Join(tempDir, "no-perm"), 0o700))
	require.NoError(t, os.WriteFile(path.Join(tempDir, "no-perm", "no-perm.csv"), []byte("1,1"), 0o644))
	require.NoError(t, os.Chmod(path.Join(tempDir, "no-perm"), 0o000))
	t.Cleanup(func() {
		// make sure TempDir RemoveAll cleanup works
		_ = os.Chmod(path.Join(tempDir, "no-perm"), 0o700)
	})
	// file without permission
	require.NoError(t, os.WriteFile(path.Join(tempDir, "no-perm.csv"), []byte("1,1"), 0o644))
	require.NoError(t, os.Chmod(path.Join(tempDir, "no-perm.csv"), 0o000))

	// relative path
	c.Path = "~/file.csv"
	err2 := c.InitDataFiles(ctx)
	require.ErrorIs(t, err2, exeerrors.ErrLoadDataInvalidURI)
	require.ErrorContains(t, err2, "URI of data source is invalid")
	// non-exist parent directory
	c.Path = "/path/to/non/exists/file.csv"
	err = c.InitDataFiles(ctx)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataInvalidURI)
	require.ErrorContains(t, err, "no such file or directory")
	// without permission to parent dir
	c.Path = path.Join(tempDir, "no-perm", "no-perm.csv")
	err = c.InitDataFiles(ctx)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataCantRead)
	require.ErrorContains(t, err, "permission denied")
	// file not exists
	c.Path = path.Join(tempDir, "not-exists.csv")
	err = c.InitDataFiles(ctx)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataCantRead)
	require.ErrorContains(t, err, "no such file or directory")
	// file without permission
	c.Path = path.Join(tempDir, "no-perm.csv")
	err = c.InitDataFiles(ctx)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataCantRead)
	require.ErrorContains(t, err, "permission denied")
	// we don't have read access to 'no-perm' directory, so walk-dir fails
	c.Path = path.Join(tempDir, "server-*.csv")
	err = c.InitDataFiles(ctx)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataCantRead)
	require.ErrorContains(t, err, "permission denied")
	// grant read access to 'no-perm' directory, should ok now.
	require.NoError(t, os.Chmod(path.Join(tempDir, "no-perm"), 0o400))
	c.Path = path.Join(tempDir, "server-*.csv")
	require.NoError(t, c.InitDataFiles(ctx))
	// test glob matching pattern [12]
	err = os.WriteFile(path.Join(tempDir, "glob-1.csv"), []byte("1,1"), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(path.Join(tempDir, "glob-2.csv"), []byte("2,2"), 0o644)
	require.NoError(t, err)
	err = os.WriteFile(path.Join(tempDir, "glob-3.csv"), []byte("3,3"), 0o644)
	require.NoError(t, err)
	c.Path = path.Join(tempDir, "glob-[12].csv")
	require.NoError(t, c.InitDataFiles(ctx))
	gotPath := make([]string, 0, len(c.dataFiles))
	for _, f := range c.dataFiles {
		gotPath = append(gotPath, f.Path)
	}
	require.ElementsMatch(t, []string{"glob-1.csv", "glob-2.csv"}, gotPath)
	// test glob matching pattern [2-3]
	c.Path = path.Join(tempDir, "glob-[2-3].csv")
	require.NoError(t, c.InitDataFiles(ctx))
	gotPath = make([]string, 0, len(c.dataFiles))
	for _, f := range c.dataFiles {
		gotPath = append(gotPath, f.Path)
	}
	require.ElementsMatch(t, []string{"glob-2.csv", "glob-3.csv"}, gotPath)

	testcases := []struct {
		fileNames    []string
		expectFormat string
	}{
		{
			expectFormat: DataFormatCSV,
			fileNames:    []string{"file1.CSV", "file1.csv.gz", "file1.csv.gz", "file1.CSV.GZIP", "file1.CSV.gzip", "file1.csv.zstd", "file1.csv.zst", "file1.csv.snappy"},
		},
		{
			expectFormat: DataFormatSQL,
			fileNames:    []string{"file2.SQL", "file2.sql.gz", "file2.SQL.GZIP", "file2.sql.zstd", "file2.sql.zstd", "file2.sql.zst", "file2.sql.zst", "file2.sql.snappy"},
		},
		{
			expectFormat: DataFormatParquet,
			fileNames:    []string{"file3.PARQUET", "file3.parquet.gz", "file3.PARQUET.GZIP", "file3.parquet.zstd", "file3.parquet.zst", "file3.parquet.snappy", "file3.parquet.snappy"},
		},
	}

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/importer/skipCheckForParquet", "return")
	for _, testcase := range testcases {
		for _, fileName := range testcase.fileNames {
			c.Format = DataFormatAuto
			c.Path = path.Join(tempDir, fileName)
			err = os.WriteFile(c.Path, []byte{}, 0o644)
			require.NoError(t, err)
			require.NoError(t, c.InitDataFiles(ctx))
			require.Equal(t, testcase.expectFormat, c.Format)
		}
	}
}

func TestGetDataSourceType(t *testing.T) {
	require.Equal(t, DataSourceTypeQuery, getDataSourceType(&plannercore.ImportInto{
		SelectPlan: &physicalop.PhysicalSelection{},
	}))
	require.Equal(t, DataSourceTypeFile, getDataSourceType(&plannercore.ImportInto{}))
}
func TestParseFileType(t *testing.T) {
	testCases := []struct {
		name     string
		path     string
		expected string
	}{
		// Basic file extensions
		{name: "sql extension", path: "test.sql", expected: DataFormatSQL},
		{name: "parquet extension", path: "data.parquet", expected: DataFormatParquet},
		{name: "csv extension", path: "file.csv", expected: DataFormatCSV},
		{name: "no extension", path: "noext", expected: DataFormatCSV},
		// Single compression extension
		{name: "sql with gz", path: "test.sql.gz", expected: DataFormatSQL},
		{name: "parquet with zstd", path: "data.parquet.zst", expected: DataFormatParquet},
		{name: "csv with snappy", path: "file.csv.snappy", expected: DataFormatCSV},
		// Edge cases after removing compression
		{name: "only compression extension", path: "file.gz", expected: DataFormatCSV},
		{name: "non-recognized extension after compression", path: "document.txt.gz", expected: DataFormatCSV},
		// Case insensitivity
		{name: "uppercase extension", path: "TEST.SQL.GZ", expected: DataFormatSQL},
		{name: "mixed case extension", path: "file.PARQUET.zst", expected: DataFormatParquet},
		// Multiple dots in filename
		{name: "multiple dots in name", path: "backup.file.sql.gz", expected: DataFormatSQL},
		{name: "hidden file with compression", path: ".hidden.sql.gz", expected: DataFormatSQL},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := parseFileType(tc.path)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestGetDefMaxEngineSize(t *testing.T) {
	if kerneltype.IsClassic() {
		require.Equal(t, config.ByteSize(500*units.GiB), getDefMaxEngineSize())
	} else {
		require.Equal(t, config.ByteSize(100*units.GiB), getDefMaxEngineSize())
	}
}
