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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/dumpformat/parquetfile"
	"github.com/pingcap/tidb/pkg/dumpformat/testutils"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/importinto"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type keyspaceOnlyStore struct {
	tidbkv.Storage
}

func (keyspaceOnlyStore) GetKeyspace() string {
	return ""
}

// Explicit CSV fixture values; preparation defaults are tested in importinto.
var (
	defaultCharacterSet = "utf8mb4"
	defaultFieldNullDef = []string{`\N`}
)

func TestInitDataStoreWithoutSource(t *testing.T) {
	ctx := context.Background()
	// A query import has no source path, and only opens the global sort store.
	for _, uri := range []string{"", "noop://sort"} {
		controller := &LoadDataController{Plan: &Plan{CloudStorageURI: uri}}
		require.NoError(t, controller.InitDataStore(ctx))
		require.Nil(t, controller.dataStore)
		require.Equal(t, uri != "", controller.globalSortStore != nil)
		controller.Close()
	}
	// A failed source store does not leak the global sort store.
	for _, source := range []string{"s3://source/%zz", "unsupported://source/file"} {
		controller := &LoadDataController{Plan: &Plan{CloudStorageURI: "noop://sort", Path: source}}
		require.Error(t, controller.InitDataStore(ctx))
		require.Nil(t, controller.globalSortStore)
		require.Nil(t, controller.dataStore)
		controller.Close()
	}
}

// for negative case see TestImportIntoOptionsNegativeCase

func TestGetConflictHandlingMode(t *testing.T) {
	plan := &Plan{}
	require.Equal(t, OnDupKeyModeError, plan.GetOnDupKeyMode())

	plan.OnDupKey = OnDupKeyModeCapture
	require.Equal(t, OnDupKeyModeCapture, plan.GetOnDupKeyMode())

	plan.OnDupKey = OnDupKeyModeError
	require.Equal(t, OnDupKeyModeError, plan.GetOnDupKeyMode())
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

func newParquetPlanAndControllerForTest(ctx context.Context, t *testing.T, sctx *mock.Context) (*Plan, *LoadDataController, error) {
	t.Helper()
	sctx.Store = keyspaceOnlyStore{}

	node, err := parser.New().ParseOneStmt("create table t(a timestamp)", "", "")
	require.NoError(t, err)
	tblInfo, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	tblInfo.State = model.StatePublic
	table := tables.MockTableFromMeta(tblInfo)

	fileName := filepath.Join(t.TempDir(), "data.parquet")
	require.NoError(t, os.WriteFile(fileName, nil, 0o644))
	format := DataFormatParquet
	plan, err := NewImportPlan(ctx, sctx, plannercore.ImportInto{
		Path:   fileName,
		Format: &format,
		Table: &resolve.TableNameW{
			TableName: &ast.TableName{
				Schema: ast.NewCIStr("test"),
				Name:   ast.NewCIStr("t"),
			},
			DBInfo: &model.DBInfo{
				Name: ast.NewCIStr("test"),
				ID:   1,
			},
		},
	}.Init(sctx.GetPlanCtx()), table)
	require.NoError(t, err)

	controller, err := NewLoadDataController(plan, table, &ASTArgs{})
	return plan, controller, err
}

func TestImportPlanParquetLocation(t *testing.T) {
	ctx := context.Background()

	t.Run("import_plan_parquet_location", func(t *testing.T) {
		sctx := mock.NewContext()
		defer sctx.Close()
		asiaShanghai, err := time.LoadLocation("Asia/Shanghai")
		require.NoError(t, err)
		sctx.GetSessionVars().TimeZone = asiaShanghai
		sctx.GetSessionVars().StmtCtx.SetTimeZone(asiaShanghai)

		plan, controller, err := newParquetPlanAndControllerForTest(ctx, t, sctx)
		require.NoError(t, err)
		require.Equal(t, "Asia/Shanghai", plan.LocationID)
		require.NotNil(t, controller.ParquetLocation())
		require.Equal(t, "Asia/Shanghai", controller.ParquetLocation().String())

		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/importer/skipEstimateCompressionForParquet", "return(true)")
		require.NoError(t, controller.InitDataFiles(ctx))
		require.Len(t, controller.dataFiles, 1)
		require.Same(t, controller.ParquetLocation(), controller.dataFiles[0].ParquetMeta.Loc)
	})

	t.Run("import_plan_parquet_fixed_offset_location", func(t *testing.T) {
		sctx := mock.NewContext()
		defer sctx.Close()
		require.NoError(t, sctx.GetSessionVars().SetSystemVar(vardef.TimeZone, "+08:00"))

		plan, controller, err := newParquetPlanAndControllerForTest(ctx, t, sctx)
		require.NoError(t, err)
		require.Equal(t, "+08:00", plan.LocationID)
		require.NotNil(t, controller.ParquetLocation())
		_, offset := time.Now().In(controller.ParquetLocation()).Zone()
		require.Equal(t, 8*3600, offset)

		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/importer/skipEstimateCompressionForParquet", "return(true)")
		require.NoError(t, controller.InitDataFiles(ctx))
		require.Len(t, controller.dataFiles, 1)
		require.Same(t, controller.ParquetLocation(), controller.dataFiles[0].ParquetMeta.Loc)
	})

	t.Run("import_plan_parquet_named_fixed_zone_location_is_rejected", func(t *testing.T) {
		sctx := mock.NewContext()
		defer sctx.Close()
		loc := time.FixedZone("UTC+8", 8*3600)
		sctx.GetSessionVars().TimeZone = loc
		sctx.GetSessionVars().StmtCtx.SetTimeZone(loc)

		plan, controller, err := newParquetPlanAndControllerForTest(ctx, t, sctx)
		require.Equal(t, "UTC+8", plan.LocationID)
		require.Nil(t, controller)
		require.ErrorContains(t, err, "invalid location UTC+8")
	})

	t.Run("import_plan_parquet_unnamed_fixed_zone_location", func(t *testing.T) {
		sctx := mock.NewContext()
		defer sctx.Close()
		loc := time.FixedZone("", -6*3600)
		sctx.GetSessionVars().TimeZone = loc
		sctx.GetSessionVars().StmtCtx.SetTimeZone(loc)

		plan, controller, err := newParquetPlanAndControllerForTest(ctx, t, sctx)
		require.NoError(t, err)
		require.Equal(t, "-06:00", plan.LocationID)
		require.NotNil(t, controller.ParquetLocation())
		_, offset := time.Now().In(controller.ParquetLocation()).Zone()
		require.Equal(t, -6*3600, offset)
	})

	t.Run("legacy_import_task_meta_without_location_id_uses_utc", func(t *testing.T) {
		sctx := mock.NewContext()
		defer sctx.Close()
		asiaShanghai, err := time.LoadLocation("Asia/Shanghai")
		require.NoError(t, err)
		sctx.GetSessionVars().TimeZone = asiaShanghai
		sctx.GetSessionVars().StmtCtx.SetTimeZone(asiaShanghai)

		plan, controller, err := newParquetPlanAndControllerForTest(ctx, t, sctx)
		require.NoError(t, err)
		taskMetaBytes, err := json.Marshal(struct {
			Plan *Plan
		}{
			Plan: plan,
		})
		require.NoError(t, err)

		var taskMeta map[string]any
		require.NoError(t, json.Unmarshal(taskMetaBytes, &taskMeta))
		planMeta, ok := taskMeta["Plan"].(map[string]any)
		require.True(t, ok)
		delete(planMeta, "LocationID")
		legacyTaskMetaBytes, err := json.Marshal(taskMeta)
		require.NoError(t, err)

		var legacyTaskMeta struct {
			Plan Plan
		}
		require.NoError(t, json.Unmarshal(legacyTaskMetaBytes, &legacyTaskMeta))
		require.Empty(t, legacyTaskMeta.Plan.LocationID)

		legacyController, err := NewLoadDataController(&legacyTaskMeta.Plan, controller.Table, &ASTArgs{})
		require.NoError(t, err)
		require.Same(t, time.UTC, legacyController.ParquetLocation())

		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/importer/skipEstimateCompressionForParquet", "return(true)")
		require.NoError(t, legacyController.InitDataFiles(ctx))
		require.Len(t, legacyController.dataFiles, 1)
		require.Same(t, time.UTC, legacyController.dataFiles[0].ParquetMeta.Loc)
	})
}

func TestEstimateFormatSizeExpansionRatio(t *testing.T) {
	ctx := context.Background()

	t.Run("row oriented format uses identity ratio", func(t *testing.T) {
		ratio, err := estimateFormatSizeExpansionRatio(ctx, "data.csv", 1024, mydump.SourceTypeCSV, nil)
		require.NoError(t, err)
		require.Equal(t, 1.0, ratio)
	})

	t.Run("parquet ratio is clamped to physical file size", func(t *testing.T) {
		dir := t.TempDir()
		const fileName = "tiny.parquet"
		columns := []testutils.ParquetColumn{
			{
				Name:      "id",
				Type:      parquet.Types.Int64,
				Converted: schema.ConvertedTypes.None,
				Gen: func(numRows int) (any, []int16) {
					data := make([]int64, numRows)
					defLevels := make([]int16, numRows)
					for i := range numRows {
						data[i] = int64(i)
						defLevels[i] = 1
					}
					return data, defLevels
				},
			},
		}
		require.NoError(t, testutils.WriteParquetFile(dir, fileName, columns, 1))

		store, err := objstore.NewLocalStorage(dir)
		require.NoError(t, err)
		stat, err := os.Stat(filepath.Join(dir, fileName))
		require.NoError(t, err)
		rows, rowSize, err := parquetfile.SampleStatisticsFromParquet(ctx, fileName, store)
		require.NoError(t, err)
		require.Less(t, rowSize*float64(rows), float64(stat.Size()))

		ratio, err := estimateFormatSizeExpansionRatio(ctx, fileName, stat.Size(), mydump.SourceTypeParquet, store)
		require.NoError(t, err)
		require.Equal(t, 1.0, ratio)
	})
}

func TestInitCompressedFiles(t *testing.T) {
	username, err := user.Current()
	require.NoError(t, err)
	if username.Name == "root" {
		t.Skip("it cannot run as root")
	}
	ctx := context.Background()

	t.Run("real size is at least compressed file size", func(t *testing.T) {
		tempDir := t.TempDir()
		content := []byte("small file whose sampled compression ratio is below one")
		fileName := filepath.Join(tempDir, "small.csv.gz")
		require.NoError(t, os.WriteFile(fileName, content, 0o644))

		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/lightning/mydump/SampleFileCompressPercentage", `return(50)`)
		c := LoadDataController{
			Plan: &Plan{
				Format:         DataFormatCSV,
				InImportInto:   true,
				Charset:        &defaultCharacterSet,
				LineFieldsInfo: importinto.DefaultLineFieldsInfo(),
				FieldNullDef:   defaultFieldNullDef,
				Parameters:     &ImportParameters{},
			},
			logger: zap.NewExample(),
		}

		c.Path = filepath.Join(tempDir, "*.gz")
		require.NoError(t, c.InitDataFiles(ctx))
		require.Len(t, c.dataFiles, 1)
		require.Equal(t, int64(len(content)), c.dataFiles[0].FileSize)
		require.Equal(t, c.dataFiles[0].FileSize, c.dataFiles[0].RealSize)
		require.Equal(t, c.TotalFileSize, c.TotalRealSize)
	})

	tempDir := t.TempDir()

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
			LineFieldsInfo: importinto.DefaultLineFieldsInfo(),
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
			LineFieldsInfo: importinto.DefaultLineFieldsInfo(),
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

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/importer/skipEstimateCompressionForParquet", "return(true)")
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

	t.Run("unsupported parser format returns unsupported-format error", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "data.txt")
		require.NoError(t, os.WriteFile(filePath, []byte("1\n"), 0o644))

		_, err := newLoadDataParser(
			context.Background(),
			zap.NewNop(),
			"unsupported",
			0,
			nil,
			&config.CSVConfig{},
			nil,
			LoadDataReaderInfo{
				Opener: func(context.Context) (io.ReadSeekCloser, error) {
					return os.Open(filePath)
				},
			},
		)
		require.True(t, exeerrors.ErrLoadDataUnsupportedFormat.Equal(err))
		require.False(t, exeerrors.ErrLoadDataWrongFormatConfig.Equal(err))
	})
}

func TestImportPlanFromInput(t *testing.T) {
	ctx := context.Background()
	sctx := mock.NewContext()
	defer sctx.Close()
	sctx.Store = keyspaceOnlyStore{}
	node, err := parser.New().ParseOneStmt("create table t(a int)", "", "")
	require.NoError(t, err)
	tblInfo, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), 42)
	require.NoError(t, err)
	tblInfo.State = model.StatePublic
	tbl := tables.MockTableFromMeta(tblInfo)
	format := DataFormatCSV
	for _, sourceType := range []DataSourceType{DataSourceTypeFile, DataSourceTypeQuery} {
		t.Run(sourceType.String(), func(t *testing.T) {
			value := &expression.Constant{Value: types.NewIntDatum(2), RetType: types.NewFieldType(mysql.TypeLonglong)}
			path := filepath.Join(t.TempDir(), "data.csv")
			input := &importinto.Input{
				DBName: "test", DBID: 7, TableInfo: tblInfo,
				Path: path, Format: &format, DataSourceType: sourceType,
				Options: []*importinto.Option{{Name: "thread", Value: value}},
			}
			physical := &plannercore.ImportInto{
				Table: &resolve.TableNameW{
					TableName: &ast.TableName{Schema: ast.NewCIStr("test"), Name: ast.NewCIStr("t")},
					DBInfo:    &model.DBInfo{ID: 7, Name: ast.NewCIStr("test")},
				},
				Path: path, Format: &format,
				Options: []*plannercore.LoadDataOpt{{Name: "thread", Value: value}},
			}
			if sourceType == DataSourceTypeQuery {
				physical.SelectPlan = &physicalop.PhysicalSelection{}
			}
			fromSQL, err := NewImportPlan(ctx, sctx, physical, tbl)
			require.NoError(t, err)
			direct, err := NewImportPlanFromInput(ctx, sctx, input)
			require.NoError(t, err)
			require.Equal(t, fromSQL, direct)
			require.Same(t, tblInfo, direct.TableInfo)
			require.JSONEq(t, fromSQL.Parameters.String(), direct.Parameters.String())
			encoded, err := json.Marshal(fromSQL)
			require.NoError(t, err)
			directEncoded, err := json.Marshal(direct)
			require.NoError(t, err)
			require.JSONEq(t, string(encoded), string(directEncoded))

			// Both entrypoints must reject the same invalid numeric option.
			bad := &expression.Constant{Value: types.NewStringDatum("bad"), RetType: types.NewFieldType(mysql.TypeVarString)}
			input.Options[0].Value = bad
			physical.Options[0].Value = bad
			_, sqlErr := NewImportPlan(ctx, sctx, physical, tbl)
			_, directErr := NewImportPlanFromInput(ctx, sctx, input)
			require.Error(t, sqlErr)
			require.EqualError(t, directErr, sqlErr.Error())
		})
	}
	_, err = NewImportPlanFromInput(ctx, sctx, nil)
	require.ErrorContains(t, err, "target table metadata")
}
