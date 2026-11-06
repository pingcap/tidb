// Copyright 2026 PingCAP, Inc.
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

package importinto

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"runtime"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/expression"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

type keyspaceOnlyStore struct{ tidbkv.Storage }

func (keyspaceOnlyStore) GetKeyspace() string { return "" }

func resolveTestOptions(t *testing.T, _ sessionctx.Context, options []*ast.LoadDataOpt) []*Option {
	t.Helper()
	result := make([]*Option, len(options))
	for i, option := range options {
		item := &Option{Name: option.Name}
		if option.Value != nil {
			literal, ok := option.Value.(ast.ValueExpr)
			require.True(t, ok, "IMPORT options are literal values")
			item.Value = &expression.Constant{Value: types.NewDatum(literal.GetValue()), RetType: literal.GetType().Clone()}
		}
		result[i] = item
	}
	return result
}

func TestInitDefaultOptions(t *testing.T) {
	plan := &Plan{
		DataSourceType: DataSourceTypeQuery,
	}
	initDefaultImportOptions(context.Background(), plan, 10, nil)
	require.Equal(t, 2, plan.ThreadCnt)

	plan = &Plan{
		DataSourceType: DataSourceTypeFile,
	}
	vardef.CloudStorageURI.Store("s3://bucket")
	t.Cleanup(func() {
		vardef.CloudStorageURI.Store("")
	})
	initDefaultImportOptions(context.Background(), plan, 1, nil)
	require.Equal(t, config.ByteSize(0), plan.DiskQuota)
	require.Equal(t, config.OpLevelRequired, plan.Checksum)
	require.Equal(t, 1, plan.ThreadCnt)
	require.Equal(t, unlimitedWriteSpeed, plan.MaxWriteSpeed)
	require.Equal(t, false, plan.SplitFile)
	require.Equal(t, int64(100), plan.MaxRecordedErrors)
	require.Equal(t, OnDupKeyModeError, plan.OnDupKey)
	require.Equal(t, false, plan.Detached)
	require.Equal(t, "utf8mb4", *plan.Charset)
	require.Equal(t, false, plan.DisableTiKVImportMode)
	if kerneltype.IsNextGen() {
		require.Equal(t, config.DefaultBatchSize, plan.MaxEngineSize)
	} else {
		require.Equal(t, config.ByteSize(defaultMaxEngineSize), plan.MaxEngineSize)
	}

	require.Equal(t, "s3://bucket/dxf/", plan.CloudStorageURI)

	initDefaultImportOptions(context.Background(), plan, 10, nil)
	require.Equal(t, 5, plan.ThreadCnt)
}

func TestPlanUseNewCollate(t *testing.T) {
	plan := &Plan{}
	require.True(t, plan.GetUseNewCollateOrDefault(true))
	require.False(t, plan.GetUseNewCollateOrDefault(false))

	setPlanUseNewCollate(plan, false)
	require.False(t, plan.GetUseNewCollateOrDefault(true))

	data, err := json.Marshal(plan)
	require.NoError(t, err)
	require.Contains(t, string(data), `"use_new_collate":false`)

	var decoded Plan
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.False(t, decoded.GetUseNewCollateOrDefault(true))

	setPlanUseNewCollate(&decoded, true)
	require.True(t, decoded.GetUseNewCollateOrDefault(false))
}

func TestInitOptionsPositiveCase(t *testing.T) {
	sctx := mock.NewContext()
	defer sctx.Close()
	ctx := tikvutil.WithInternalSourceType(context.Background(), tidbkv.InternalImportInto)

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
	err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
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
	require.Equal(t, OnDupKeyModeError, plan.OnDupKey, sql)
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
	sqlCapture := sql + ", " + onDupKeyOption + "='capture'"
	stmt, err = p.ParseOneStmt(sqlCapture, "", "")
	require.NoError(t, err, sqlCapture)
	plan = &Plan{Format: DataFormatCSV}
	err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sqlCapture)
	require.Equal(t, "s3://bucket/path/dxf/", plan.CloudStorageURI, sqlCapture)
	require.Equal(t, OnDupKeyModeCapture, plan.OnDupKey, sqlCapture)

	// override cloud storage uri using option
	sql2 := sql + ", " + cloudStorageURIOption + "='s3://bucket/path2'"
	stmt, err = p.ParseOneStmt(sql2, "", "")
	require.NoError(t, err, sql2)
	plan = &Plan{Format: DataFormatCSV}
	err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql2)
	require.Equal(t, "s3://bucket/path2", plan.CloudStorageURI, sql2)
	// override with gs
	sql3 := sql + ", " + cloudStorageURIOption + "='gs://bucket/path2'"
	stmt, err = p.ParseOneStmt(sql3, "", "")
	require.NoError(t, err, sql3)
	plan = &Plan{Format: DataFormatCSV}
	err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql3)
	require.Equal(t, "gs://bucket/path2", plan.CloudStorageURI, sql3)
	// override with azure
	sql3a := sql + ", " + cloudStorageURIOption + "='azure://container/path2'"
	stmt, err = p.ParseOneStmt(sql3a, "", "")
	require.NoError(t, err, sql3a)
	plan = &Plan{Format: DataFormatCSV}
	err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql3a)
	require.Equal(t, "azure://container/path2", plan.CloudStorageURI, sql3a)
	sql3b := sql + ", " + cloudStorageURIOption + "='azblob://container/path3'"
	stmt, err = p.ParseOneStmt(sql3b, "", "")
	require.NoError(t, err, sql3b)
	plan = &Plan{Format: DataFormatCSV}
	err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql3b)
	require.Equal(t, "azblob://container/path3", plan.CloudStorageURI, sql3b)
	// override with empty string, force use local sort
	sql4 := sql + ", " + cloudStorageURIOption + "=''"
	stmt, err = p.ParseOneStmt(sql4, "", "")
	require.NoError(t, err, sql4)
	plan = &Plan{Format: DataFormatCSV}
	err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql4)
	require.Equal(t, "", plan.CloudStorageURI, sql4)
}

func TestInitOptionsDisallowOnDuplicateKeyWithLocalSort(t *testing.T) {
	sctx := mock.NewContext()
	defer sctx.Close()
	ctx := tikvutil.WithInternalSourceType(context.Background(), tidbkv.InternalImportInto)

	sql := "import into t from '/file.csv' with on_duplicate_key='capture'"
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	vardef.CloudStorageURI.Store("")
	plan := &Plan{Format: DataFormatCSV}
	err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
	require.ErrorIs(t, err, exeerrors.ErrLoadDataUnsupportedOption)
	require.ErrorContains(t, err, onDupKeyOption)
	require.ErrorContains(t, err, "local sort")
}

func TestInitOptionsFromQueryPositiveCase(t *testing.T) {
	sctx := mock.NewContext()
	defer sctx.Close()
	ctx := tikvutil.WithInternalSourceType(context.Background(), tidbkv.InternalImportInto)

	p := parser.New()

	// all allowed options for import from query: thread, disable_precheck, disk_quota
	sql := "import into t from select * from t2 with " +
		threadOption + "=4, " +
		disablePrecheckOption + ", " +
		diskQuotaOption + "='100gib'"
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err, sql)
	plan := &Plan{DataSourceType: DataSourceTypeQuery}
	err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql)
	require.Equal(t, 4, plan.ThreadCnt, sql)
	require.True(t, plan.DisablePrecheck, sql)
	require.Equal(t, config.ByteSize(100<<30), plan.DiskQuota, sql)

	// disk_quota alone should work
	sql = "import into t from select * from t2 with " + diskQuotaOption + "='50gib'"
	stmt, err = p.ParseOneStmt(sql, "", "")
	require.NoError(t, err, sql)
	plan = &Plan{DataSourceType: DataSourceTypeQuery}
	err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err, sql)
	require.Equal(t, config.ByteSize(50<<30), plan.DiskQuota, sql)
}

func TestInitOptionsFromQueryNegativeCase(t *testing.T) {
	sctx := mock.NewContext()
	defer sctx.Close()
	ctx := tikvutil.WithInternalSourceType(context.Background(), tidbkv.InternalImportInto)

	p := parser.New()

	// options not allowed for import from query
	disallowedOptions := []string{
		characterSetOption + "='utf8'",
		fieldsTerminatedByOption + "='aaa'",
		fieldsEnclosedByOption + "='|'",
		fieldsEscapedByOption + "='\\\\'",
		fieldsDefinedNullByOption + "='N'",
		linesTerminatedByOption + "='\\n'",
		skipRowsOption + "=1",
		splitFileOption,
		checksumTableOption + "='optional'",
		maxWriteSpeedOption + "='200mib'",
		recordErrorsOption + "=123",
		detachedOption,
		disableTiKVImportModeOption,
		maxEngineSizeOption + "='100gib'",
		cloudStorageURIOption + "='s3://bucket/path'",
	}
	for _, opt := range disallowedOptions {
		sql := "import into t from select * from t2 with " + opt
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err, sql)
		plan := &Plan{DataSourceType: DataSourceTypeQuery}
		err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
		require.ErrorIs(t, err, exeerrors.ErrLoadDataUnsupportedOption, sql)
	}

	// invalid disk_quota values for query source
	invalidDiskQuotaCases := []string{
		diskQuotaOption + "='aa'",
		diskQuotaOption + "='220MiBxxx'",
		diskQuotaOption + "=1",
		diskQuotaOption + "=false",
	}
	for _, opt := range invalidDiskQuotaCases {
		sql := "import into t from select * from t2 with " + opt
		stmt, err := p.ParseOneStmt(sql, "", "")
		require.NoError(t, err, sql)
		plan := &Plan{DataSourceType: DataSourceTypeQuery}
		err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
		require.Error(t, err, sql)
	}

	// duplicate options
	sql := "import into t from select * from t2 with " + diskQuotaOption + "='100gib', " + diskQuotaOption + "='200gib'"
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err, sql)
	plan := &Plan{DataSourceType: DataSourceTypeQuery}
	err = initImportOptions(ctx, plan, sctx, resolveTestOptions(t, sctx, stmt.(*ast.ImportIntoStmt).Options))
	require.ErrorIs(t, err, exeerrors.ErrDuplicateOption, sql)
}

func TestAdjustOptions(t *testing.T) {
	plan := &Plan{
		DiskQuota:      1,
		ThreadCnt:      100000000,
		MaxWriteSpeed:  10,
		DataSourceType: DataSourceTypeFile,
	}
	adjustImportOptions(plan, 16)
	require.Equal(t, 16, plan.ThreadCnt)
	require.Equal(t, config.ByteSize(10), plan.MaxWriteSpeed) // not adjusted
	require.False(t, plan.DisableTiKVImportMode)

	plan.ThreadCnt = 100000000
	plan.DataSourceType = DataSourceTypeQuery
	adjustImportOptions(plan, 16)
	require.Equal(t, 32, plan.ThreadCnt)
	require.False(t, plan.DisableTiKVImportMode)

	plan.CloudStorageURI = "s3://bucket/path"
	adjustImportOptions(plan, 16)
	require.True(t, plan.DisableTiKVImportMode)
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
		Path:   "azure://bucket/path?account-name=test-account&sas-token=111111",
	}
	require.NoError(t, initImportParameters(p, &Input{
		Options: []*Option{
			{
				Name: cloudStorageURIOption,
				Value: &expression.Constant{
					Value: types.NewStringDatum("azblob://this-is-for-storage/path?account-name=test-account&sas_token=bbbbbb"),
				},
			},
		},
	}))
	urlEqual(t, "azure://bucket/path?account-name=test-account&sas-token=xxxxxx", p.Parameters.FileLocation)
	require.Len(t, p.Parameters.Options, 1)
	urlEqual(t, "azblob://this-is-for-storage/path?account-name=test-account&sas_token=xxxxxx",
		p.Parameters.Options[cloudStorageURIOption].(string))

	// test other options
	require.NoError(t, initImportParameters(p, &Input{
		Options: []*Option{
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

func TestGetDefMaxEngineSize(t *testing.T) {
	if kerneltype.IsClassic() {
		require.Equal(t, config.ByteSize(500*units.GiB), getDefMaxEngineSize())
	} else {
		require.Equal(t, config.ByteSize(100*units.GiB), getDefMaxEngineSize())
	}
}
