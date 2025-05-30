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

package executor_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/stretchr/testify/require"
)

func TestSecurityEnhancedMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	sem.Enable()
	defer sem.Disable()
	tk.MustExec("create table test.t (id int);")

	// When SEM is enabled these features are restricted to all users
	// regardless of what privileges they have available.
	tk.MustGetErrMsg("IMPORT INTO test.t FROM '/file.csv'", "[planner:8132]Feature 'IMPORT INTO from server disk' is not supported when security enhanced mode is enabled")
}

func TestClassicS3ExternalID(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("only for classic")
	}
	store := testkit.CreateMockStore(t)
	outerTK := testkit.NewTestKit(t, store)
	outerTK.MustExec("create table test.t (id int);")

	bak := config.GetGlobalKeyspaceName()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceName = "aaa"
	})
	t.Cleanup(func() {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = bak
		})
	})

	t.Run("SEM enabled, explicit external ID is allowed, and we don't change it", func(t *testing.T) {
		sem.Enable()
		t.Cleanup(func() {
			sem.Disable()
		})
		tk := testkit.NewTestKit(t, store)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/importer/NewImportPlan", func(plan *plannercore.ImportInto) {
			u, err := url.Parse(plan.Path)
			require.NoError(t, err)
			require.Contains(t, u.Query(), storage.S3ExternalID)
			require.Equal(t, "allowed", u.Query().Get(storage.S3ExternalID))
			panic("FAIL IT, AS WE CANNOT RUN IT HERE")
		})
		tk.MustExec("IMPORT INTO test.t FROM 's3://bucket?EXTERNAL-ID=allowed'")
		tk.MustQuery("select * from test.t").Check(testkit.Rows())
	})

	t.Run("SEM disabled, explicit external ID is also allowed, and we don't change it", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/importer/NewImportPlan", func(plan *plannercore.ImportInto) {
			u, err := url.Parse(plan.Path)
			require.NoError(t, err)
			require.Contains(t, u.Query(), storage.S3ExternalID)
			require.Equal(t, "allowed", u.Query().Get(storage.S3ExternalID))
			panic("FAIL IT, AS WE CANNOT RUN IT HERE")
		})
		tk.MustExec("IMPORT INTO test.t FROM 's3://bucket?EXTERNAL-ID=allowed'")
		tk.MustQuery("select * from test.t").Check(testkit.Rows())
	})
}

func TestNextGenS3ExternalID(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only for nextgen")
	}
	store := testkit.CreateMockStore(t)
	outerTK := testkit.NewTestKit(t, store)
	outerTK.MustExec("create table test.t (id int);")

	t.Run("SEM enabled, forbid set S3 external ID", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		sem.Enable()
		t.Cleanup(func() {
			sem.Disable()
		})
		tk.MustGetErrMsg("IMPORT INTO test.t FROM 's3://bucket?EXTERNAL-ID=abc'", "[planner:8132]Feature 'IMPORT INTO with S3 external ID' is not supported when security enhanced mode is enabled")
	})

	t.Run("SEM enabled, set S3 external ID to keyspace name", func(t *testing.T) {
		sem.Enable()
		bak := config.GetGlobalKeyspaceName()
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = "aaa"
		})
		t.Cleanup(func() {
			config.UpdateGlobal(func(conf *config.Config) {
				conf.KeyspaceName = bak
			})
			sem.Disable()
		})
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/importer/NewImportPlan", func(plan *plannercore.ImportInto) {
			u, err := url.Parse(plan.Path)
			require.NoError(t, err)
			require.Contains(t, u.Query(), storage.S3ExternalID)
			require.Equal(t, "aaa", u.Query().Get(storage.S3ExternalID))
			panic("FAIL IT, AS WE CANNOT RUN IT HERE")
		})
		tk := testkit.NewTestKit(t, store)
		err := tk.QueryToErr("IMPORT INTO test.t FROM 's3://bucket'")
		require.ErrorContains(t, err, "FAIL IT, AS WE CANNOT RUN IT HERE")
	})

	t.Run("SEM disabled, allow explicit S3 external id, should not change it", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)

		bak := config.GetGlobalKeyspaceName()
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = "aaa"
		})
		t.Cleanup(func() {
			config.UpdateGlobal(func(conf *config.Config) {
				conf.KeyspaceName = bak
			})
		})
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/importer/NewImportPlan", func(plan *plannercore.ImportInto) {
			u, err := url.Parse(plan.Path)
			require.NoError(t, err)
			require.Contains(t, u.Query(), storage.S3ExternalID)
			require.Equal(t, "allowed", u.Query().Get(storage.S3ExternalID))
			panic("FAIL IT, AS WE CANNOT RUN IT HERE")
		})
		err := tk.QueryToErr("IMPORT INTO test.t FROM 's3://bucket?external-id=allowed'")
		require.ErrorContains(t, err, "FAIL IT, AS WE CANNOT RUN IT HERE")
	})
}

func TestNextGenUnsupportedLocalSortAndOptions(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("only for nextgen")
	}
	store := testkit.CreateMockStore(t)
	outerTK := testkit.NewTestKit(t, store)
	outerTK.MustExec("create table test.t (id int);")
	sem.Enable()
	t.Cleanup(func() {
		sem.Disable()
	})

	t.Run("import from select", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		err := tk.ExecToErr("IMPORT INTO test.t FROM select 1")
		require.ErrorIs(t, err, plannererrors.ErrNotSupportedWithSem)
		require.ErrorContains(t, err, "IMPORT INTO from select")
	})

	t.Run("local sort", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		err := tk.QueryToErr("IMPORT INTO test.t FROM 's3://bucket/*.csv'")
		require.ErrorIs(t, err, plannererrors.ErrNotSupportedWithSem)
		require.ErrorContains(t, err, "IMPORT INTO with local sort")
	})

	t.Run("unsupported options", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		bak := variable.ValidateCloudStorageURI
		variable.ValidateCloudStorageURI = func(ctx context.Context, uri string) error {
			return nil
		}
		t.Cleanup(func() {
			variable.ValidateCloudStorageURI = bak
		})
		tk.MustExec("set global tidb_cloud_storage_uri='s3://bucket/tmp'")
		for _, option := range []string{
			"disk_quota",
			"max_write_speed",
			"cloud_storage_uri",
		} {
			err := tk.QueryToErr(fmt.Sprintf("IMPORT INTO test.t FROM 's3://bucket/*.csv' with %s='1'", option))
			require.ErrorIs(t, err, exeerrors.ErrLoadDataUnsupportedOption)
			require.ErrorContains(t, err, option)
		}
	})
}

func TestImportIntoValidateColAssignmentsWithEncodeCtx(t *testing.T) {
	cases := []struct {
		exprs []string
		error string
	}{
		{
			exprs: []string{"'x'", "1+@1", "concat('hello', 'world')", "getvar('var1')"},
		},
		{
			exprs: []string{"setvar('a', 'b')"},
			error: "FUNCTION setvar is not supported in IMPORT INTO column assignment, index 0",
		},
		{
			exprs: []string{"current_user()"},
			error: "FUNCTION current_user is not supported in IMPORT INTO column assignment, index 0",
		},
		{
			exprs: []string{"current_role()"},
			error: "FUNCTION current_role is not supported in IMPORT INTO column assignment, index 0",
		},
		{
			exprs: []string{"connection_id()"},
			error: "FUNCTION connection_id is not supported in IMPORT INTO column assignment, index 0",
		},
		{
			exprs: []string{"1", "tidb_is_ddl_owner()"},
			error: "FUNCTION tidb_is_ddl_owner is not supported in IMPORT INTO column assignment, index 1",
		},
		{
			exprs: []string{"sleep(1)"},
			error: "FUNCTION sleep is not supported in IMPORT INTO column assignment, index 0",
		},
		{
			exprs: []string{"LAST_INSERT_ID()"},
			error: "FUNCTION last_insert_id is not supported in IMPORT INTO column assignment, index 0",
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d-%s", i, strings.Join(c.exprs, ",")), func(t *testing.T) {
			assigns := make([]*ast.Assignment, 0, len(c.exprs))
			for _, exprStr := range c.exprs {
				stmt, err := parser.New().ParseOneStmt("select "+exprStr, "", "")
				require.NoError(t, err)
				expr := stmt.(*ast.SelectStmt).Fields.Fields[0].Expr
				assigns = append(assigns, &ast.Assignment{
					Expr: expr,
				})
			}

			err := executor.ValidateImportIntoColAssignmentsWithEncodeCtx(&importer.Plan{}, assigns)
			if c.error == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, c.error)
			}
		})
	}
}
