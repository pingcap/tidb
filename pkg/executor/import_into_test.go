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
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto/taskkey"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/mock"
	semv1 "github.com/pingcap/tidb/pkg/util/sem"
	semv2 "github.com/pingcap/tidb/pkg/util/sem/v2"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	tikvutil "github.com/tikv/client-go/v2/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/atomic"
)

var (
	semTestPatternFnsFac = func(sqlRules ...string) map[string][2]func(t *testing.T, tk *testkit.TestKit) {
		return map[string][2]func(t *testing.T, tk *testkit.TestKit){
			"v1": {
				func(t *testing.T, tk *testkit.TestKit) { semv1.Enable() },
				func(t *testing.T, tk *testkit.TestKit) { semv1.Disable() },
			},
			"v2": {
				func(t *testing.T, tk *testkit.TestKit) {
					t.Helper()
					// in SEM V1, "import_with_external_id", "import_from_local" are forbidden
					// completely, but in SEM V2, it's taken as restricted SQL, which
					// requires RESTRICTED_SQL_ADMIN privilege, when SEM enabled, root
					// doesn't have this privilege by default.
					// we must add this Auth as default testkit session doesn't have user
					// info.
					require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))

					require.NoError(t, semv2.EnableBy(&semv2.Config{TiDBVersion: "v0.0.0",
						RestrictedSQL: semv2.SQLRestriction{Rule: sqlRules}}))
				},
				func(t *testing.T, tk *testkit.TestKit) { semv2.Disable() },
			},
		}
	}
	semTestPatternFns = semTestPatternFnsFac("import_from_local", "import_with_external_id")
)

func TestSecurityEnhancedMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t (id int);")
	for i, fns := range semTestPatternFns {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			fns[0](t, tk)
			t.Cleanup(func() {
				fns[1](t, tk)
			})

			// When SEM is enabled these features are restricted to all users
			// regardless of what privileges they have available.
			tk.MustMatchErrMsg("IMPORT INTO test.t FROM '/file.csv'", `(?i).*Feature 'IMPORT INTO .*from.*' is not supported when security enhanced mode is enabled`)
		})
	}
}

func TestClassicS3ExternalID(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("only for classic")
	}
	store := testkit.CreateMockStore(t)
	outerTK := testkit.NewTestKit(t, store)
	outerTK.MustExec("create table test.t (id int);")

	t.Run("mixed versions reject owned import submission", func(t *testing.T) {
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/serverinfo/mockGetAllServerInfo", "return(`{\"new\":{\"schema_read_only\":true},\"old\":{}}`)")
		_, _, err := executor.SubmitImportTask(context.Background(), &importer.Plan{}, "import into t from '/path/to/file'")
		require.ErrorContains(t, err, "finish upgrading all TiDB nodes")
	})

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
		for i, fns := range semTestPatternFnsFac() {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				tk := testkit.NewTestKit(t, store)
				fns[0](t, tk)
				t.Cleanup(func() {
					fns[1](t, tk)
				})
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/NewImportPlan", func(plan *plannercore.ImportInto) {
					u, err := url.Parse(plan.Path)
					require.NoError(t, err)
					require.Contains(t, u.Query(), s3like.S3ExternalID)
					require.Equal(t, "allowed", u.Query().Get(s3like.S3ExternalID))
					panic("FAIL IT, AS WE CANNOT RUN IT HERE")
				})
				tk.MustExec("IMPORT INTO test.t FROM 's3://bucket?EXTERNAL-ID=allowed'")
				tk.MustQuery("select * from test.t").Check(testkit.Rows())
			})
		}
	})

	t.Run("SEM disabled, explicit external ID is also allowed, and we don't change it", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/NewImportPlan", func(plan *plannercore.ImportInto) {
			u, err := url.Parse(plan.Path)
			require.NoError(t, err)
			require.Contains(t, u.Query(), s3like.S3ExternalID)
			require.Equal(t, "allowed", u.Query().Get(s3like.S3ExternalID))
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
	originalMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Premium))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})
	store := testkit.CreateMockStore(t)
	outerTK := testkit.NewTestKit(t, store)
	outerTK.MustExec("create table test.t (id int);")

	t.Run("SEM enabled, forbid set external ID for S3 like store", func(t *testing.T) {
		for i, fns := range semTestPatternFns {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				tk := testkit.NewTestKit(t, store)
				fns[0](t, tk)
				t.Cleanup(func() {
					fns[1](t, tk)
				})
				for _, schema := range []string{"s3", "oss"} {
					tk.MustMatchErrMsg(fmt.Sprintf("IMPORT INTO test.t FROM '%s://bucket?EXTERNAL-ID=abc'", schema), `(?i).*Feature 'IMPORT INTO .*external.*' is not supported when security enhanced mode is enabled`)
				}
			})
		}
	})

	t.Run("SEM enabled, require explicit auth for S3 like store", func(t *testing.T) {
		for i, fns := range semTestPatternFns {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				tk := testkit.NewTestKit(t, store)
				fns[0](t, tk)
				t.Cleanup(func() {
					fns[1](t, tk)
				})
				for _, schema := range []string{"s3", "oss"} {
					tk.MustMatchErrMsg(fmt.Sprintf("IMPORT INTO test.t FROM '%s://bucket'", schema), `(?i).*Feature 'IMPORT INTO .*without access key/secret access key or role ARN' is not supported when security enhanced mode is enabled`)
				}
			})
		}
	})

	t.Run("SEM enabled, set external ID to keyspace name", func(t *testing.T) {
		bak := config.GetGlobalKeyspaceName()
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = "aaa"
		})
		t.Cleanup(func() {
			config.UpdateGlobal(func(conf *config.Config) {
				conf.KeyspaceName = bak
			})
		})
		for i, fns := range semTestPatternFns {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				tk := testkit.NewTestKit(t, store)
				fns[0](t, tk)
				t.Cleanup(func() {
					fns[1](t, tk)
				})
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/NewImportPlan", func(plan *plannercore.ImportInto) {
					u, err := url.Parse(plan.Path)
					require.NoError(t, err)
					require.Contains(t, u.Query(), s3like.S3ExternalID)
					require.Equal(t, "aaa", u.Query().Get(s3like.S3ExternalID))
					panic("FAIL IT, AS WE CANNOT RUN IT HERE")
				})
				for _, schema := range []string{"s3", "oss"} {
					err := tk.QueryToErr(fmt.Sprintf("IMPORT INTO test.t FROM '%s://bucket?access-key=ak&secret-access-key=sk'", schema))
					require.ErrorContains(t, err, "FAIL IT, AS WE CANNOT RUN IT HERE")
				}
			})
		}
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
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/NewImportPlan", func(plan *plannercore.ImportInto) {
			u, err := url.Parse(plan.Path)
			require.NoError(t, err)
			require.Contains(t, u.Query(), s3like.S3ExternalID)
			require.Equal(t, "allowed", u.Query().Get(s3like.S3ExternalID))
			panic("FAIL IT, AS WE CANNOT RUN IT HERE")
		})
		err := tk.QueryToErr("IMPORT INTO test.t FROM 's3://bucket?external-id=allowed'")
		require.ErrorContains(t, err, "FAIL IT, AS WE CANNOT RUN IT HERE")
	})

	t.Run("Starter SEM enabled, require and preserve explicit external ID", func(t *testing.T) {
		require.NoError(t, deploymode.Set(deploymode.Starter))
		bak := config.GetGlobalKeyspaceName()
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = "aaa"
		})
		t.Cleanup(func() {
			config.UpdateGlobal(func(conf *config.Config) {
				conf.KeyspaceName = bak
			})
			require.NoError(t, deploymode.Set(deploymode.Premium))
		})
		for i, fns := range semTestPatternFns {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				tk := testkit.NewTestKit(t, store)
				fns[0](t, tk)
				t.Cleanup(func() {
					fns[1](t, tk)
				})
				for _, schema := range []string{"s3", "oss"} {
					for _, query := range []string{
						"access-key=ak&secret-access-key=sk",
						"EXTERNAL_ID=&access-key=ak&secret-access-key=sk",
						"external-id=allowed&EXTERNAL_ID=&access-key=ak&secret-access-key=sk",
					} {
						tk.MustMatchErrMsg(
							fmt.Sprintf("IMPORT INTO test.t FROM '%s://bucket?%s'", schema, query),
							`(?i).*The URI of data source is invalid.*external ID is required for Starter deployments.*`)
					}
					tk.MustMatchErrMsg(
						fmt.Sprintf("IMPORT INTO test.t FROM '%s://bucket?external-id=allowed'", schema),
						`(?i).*Feature 'IMPORT INTO .*without access key/secret access key or role ARN' is not supported when security enhanced mode is enabled`)
				}
				for _, tc := range []struct {
					name  string
					param string
					value string
				}{
					{name: "canonical", param: "external-id", value: "allowed"},
					{name: "alias", param: "EXTERNAL_ID", value: "caller-provided"},
				} {
					t.Run(tc.name, func(t *testing.T) {
						testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/NewImportPlan", func(plan *plannercore.ImportInto) {
							u, err := url.Parse(plan.Path)
							require.NoError(t, err)
							require.Equal(t, []string{tc.value}, u.Query()[tc.param])
							panic("FAIL IT, AS WE CANNOT RUN IT HERE")
						})
						for _, schema := range []string{"s3", "oss"} {
							err := tk.QueryToErr(fmt.Sprintf("IMPORT INTO test.t FROM '%s://bucket?%s=%s&access-key=ak&secret-access-key=sk'", schema, tc.param, tc.value))
							require.ErrorContains(t, err, "FAIL IT, AS WE CANNOT RUN IT HERE")
						}
					})
				}
			})
		}
	})

	t.Run("Starter SEM disabled, external ID remains optional", func(t *testing.T) {
		require.NoError(t, deploymode.Set(deploymode.Starter))
		t.Cleanup(func() {
			require.NoError(t, deploymode.Set(deploymode.Premium))
		})
		tk := testkit.NewTestKit(t, store)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/NewImportPlan", func(plan *plannercore.ImportInto) {
			u, err := url.Parse(plan.Path)
			require.NoError(t, err)
			require.NotContains(t, u.Query(), s3like.S3ExternalID)
			panic("FAIL IT, AS WE CANNOT RUN IT HERE")
		})
		err := tk.QueryToErr("IMPORT INTO test.t FROM 's3://bucket'")
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
	for i, fns := range semTestPatternFns {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			testNextGenUnsupportedLocalSortAndOptions(t, store, func(t *testing.T, tk *testkit.TestKit) {
				fns[0](t, tk)
				t.Cleanup(func() {
					fns[1](t, tk)
				})
			})
		})
	}
}

func TestCancelImportJobWithoutDXFTask(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("import job and dxf task is submitted together in classic, no such case")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	ctx := tikvutil.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)
	require.NoError(t, manager.InitMeta(ctx, ":4000", ""))

	assertNoDXFTask := func(jobID int64) {
		taskKey := taskkey.ForJob(jobID)
		tk.MustQuery("select count(1) from mysql.tidb_global_task where task_key = ?", taskKey).
			Check(testkit.Rows("0"))
		tk.MustQuery("select count(1) from mysql.tidb_global_task_history where task_key = ?", taskKey).
			Check(testkit.Rows("0"))
	}

	jobID, err := importer.CreateJob(ctx, tk.Session().GetSQLExecutor(), "test", "t", 1,
		tk.Session().GetSessionVars().User.String(), "", &importer.ImportParameters{
			Format: importer.DataFormatCSV,
		}, 0)
	require.NoError(t, err)
	assertNoDXFTask(jobID)

	tk.MustExec(fmt.Sprintf("cancel import job %d", jobID))
	tk.MustQuery("select status, error_message from mysql.tidb_import_jobs where id = ?", jobID).
		Check(testkit.Rows("cancelled cancelled by user"))
	assertNoDXFTask(jobID)

	backgroundJobID, err := importer.CreateJob(ctx, tk.Session().GetSQLExecutor(), "test", "t", 1,
		tk.Session().GetSessionVars().User.String(), "", &importer.ImportParameters{
			Format: importer.DataFormatCSV,
		}, 0)
	require.NoError(t, err)
	assertNoDXFTask(backgroundJobID)

	// The KILL path enters the helper with a fresh background context.
	require.NoError(t, executor.CancelAndWaitImportJobForTest(context.Background(), backgroundJobID))
	tk.MustQuery("select status, error_message from mysql.tidb_import_jobs where id = ?", backgroundJobID).
		Check(testkit.Rows("cancelled cancelled by user"))
	assertNoDXFTask(backgroundJobID)

	tk.MustExec(fmt.Sprintf("cancel import job %d", jobID))
	tk.MustQuery("select status, error_message from mysql.tidb_import_jobs where id = ?", jobID).
		Check(testkit.Rows("cancelled cancelled by user"))
	assertNoDXFTask(jobID)

	runningJobID, err := importer.CreateJob(ctx, tk.Session().GetSQLExecutor(), "test", "t", 1,
		tk.Session().GetSessionVars().User.String(), "", &importer.ImportParameters{
			Format: importer.DataFormatCSV,
		}, 0)
	require.NoError(t, err)
	require.NoError(t, importer.StartJob(ctx, tk.Session().GetSQLExecutor(), runningJobID, importer.JobStepImporting))
	assertNoDXFTask(runningJobID)

	err = tk.ExecToErr(fmt.Sprintf("cancel import job %d", runningJobID))
	require.ErrorContains(t, err, "job state changed during cancel, please try again later")
	tk.MustQuery("select status, step from mysql.tidb_import_jobs where id = ?", runningJobID).
		Check(testkit.Rows("running importing"))
	assertNoDXFTask(runningJobID)
}

func testNextGenUnsupportedLocalSortAndOptions(t *testing.T, store kv.Storage, initFn func(t *testing.T, tk *testkit.TestKit)) {
	t.Run("import from select requires global sort", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		initFn(t, tk)
		err := tk.ExecToErr("IMPORT INTO test.t FROM select 1")
		require.ErrorIs(t, err, plannererrors.ErrNotSupportedWithSem)
		require.ErrorContains(t, err, "IMPORT INTO with local sort")
	})

	t.Run("local sort", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		initFn(t, tk)
		err := tk.QueryToErr("IMPORT INTO test.t FROM 's3://bucket/*.csv?access-key=ak&secret-access-key=sk'")
		require.ErrorIs(t, err, plannererrors.ErrNotSupportedWithSem)
		require.ErrorContains(t, err, "IMPORT INTO with local sort")
	})

	t.Run("unsupported options", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		initFn(t, tk)
		bak := variable.ValidateCloudStorageURI
		variable.ValidateCloudStorageURI = func(ctx context.Context, uri string) error {
			return nil
		}
		t.Cleanup(func() {
			variable.ValidateCloudStorageURI = bak
		})
		tk.MustExec("set global tidb_cloud_storage_uri='s3://bucket/tmp'")
		t.Cleanup(func() {
			tk.MustExec("set global tidb_cloud_storage_uri=''")
		})
		for _, option := range []string{
			"disk_quota",
			"max_write_speed",
			"cloud_storage_uri",
			"thread",
			"__max_engine_size",
			"checksum_table",
			"record_errors",
		} {
			err := tk.QueryToErr(fmt.Sprintf("IMPORT INTO test.t FROM 's3://bucket/*.csv?access-key=ak&secret-access-key=sk' with %s='1'", option))
			require.ErrorIs(t, err, exeerrors.ErrLoadDataUnsupportedOption)
			require.ErrorContains(t, err, option)
		}
		for _, option := range []string{
			"__force_merge_step",
			"__manual_recovery",
		} {
			err := tk.QueryToErr(fmt.Sprintf("IMPORT INTO test.t FROM 's3://bucket/*.csv?access-key=ak&secret-access-key=sk' with %s", option))
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
			exprs: []string{"'x'", "1+@1", "concat('hello', 'world')", "getvar('var1')", "uncompress(compress(@raw))"},
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

func TestImportIntoChildSessionInheritsMaintenanceFlag(t *testing.T) {
	t.Run("ClassicReadOnlyDraining", func(t *testing.T) {
		if kerneltype.IsNextGen() {
			t.Skip("Classic FROM SELECT uses the submitting transaction")
		}
		store, dom := testkit.CreateMockStoreAndDomain(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create table test.guarded_import(a int)")
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("guarded_import"))
		require.NoError(t, err)
		called := false
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportFromSelectPrecheck", func(sctx sessionctx.Context) {
			called = true
			_, registered := sctx.GetSessionVars().GetRelatedTableForReadOnly().Load(tbl.Meta().ID)
			require.True(t, registered, "the ingestion lifetime must remain visible to read-only draining")
			txn, err := sctx.Txn(false)
			require.NoError(t, err)
			require.True(t, txn.Valid())
		})
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockImportFromSelectSetupErr", "return(true)")
		require.ErrorContains(t, tk.ExecToErr("import into test.guarded_import from select 1 with disable_precheck"), "mock import from select setup error")
		require.True(t, called)
		tk.MustQuery("select @@tidb_current_ts = 0").Check(testkit.Rows("1"))
	})
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table src (a int)")
	tk.MustExec("insert into src values (1), (2)")
	tk.MustExec("create table dst (a int)")
	previousURI := vardef.CloudStorageURI.Load()
	t.Cleanup(func() { vardef.CloudStorageURI.Store(previousURI) })
	vardef.CloudStorageURI.Store("")
	expectedError := "mock import from select setup error"
	if kerneltype.IsNextGen() {
		vardef.CloudStorageURI.Store("s3://maintenance-flag-test")
		tk.MustExec("insert into dst values (0)")
		expectedError = "target table is not empty"
	}

	sessionVars := tk.Session().GetSessionVars()
	origRestricted := sessionVars.InRestrictedSQL
	origMaintenance := sessionVars.InMViewMaintenance
	sessionVars.InRestrictedSQL = true
	sessionVars.InMViewMaintenance = true
	defer func() {
		sessionVars.InRestrictedSQL = origRestricted
		sessionVars.InMViewMaintenance = origMaintenance
	}()

	var invoked, childMaintenance bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/inheritMViewMaintenanceFlagApplied", func(maintenance bool) {
		invoked = true
		childMaintenance = maintenance
	})
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockImportFromSelectSetupErr", `return(true)`)

	err := tk.ExecToErr("import into dst from select * from src with disable_precheck")
	require.ErrorContains(t, err, expectedError)
	require.True(t, invoked)
	require.True(t, childMaintenance)
}

func TestImportIntoRejectsMaterializedViewLogBaseTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mview_enable = on")
	tk.MustExec("create table src (a int)")
	tk.MustExec("create table dst (a int)")
	tk.MustExec("create materialized view log on dst (a)")

	err := tk.ExecToErr("import into dst from select * from src with disable_precheck")
	require.ErrorContains(t, err, "IMPORT INTO on tables with materialized view log")
}

func TestImportQueryRouting(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table query_route_src(id int)")
	tk.MustExec("create table query_route_dst(id int)")
	previousURI := vardef.CloudStorageURI.Load()
	t.Cleanup(func() { vardef.CloudStorageURI.Store(previousURI) })
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/mockImportFromSelectSetupErr", `return(true)`)
	sql := "import into query_route_dst from select id from query_route_src with disable_precheck"
	vars := tk.Session().GetSessionVars()
	if kerneltype.IsNextGen() {
		vardef.CloudStorageURI.Store("s3://query-sort")
		require.ErrorContains(t, tk.ExecToErr(sql), "IMPORT INTO FROM SELECT outside materialized view maintenance")
	}
	vars.InMViewMaintenance = true
	t.Cleanup(func() { vars.InMViewMaintenance = false })
	if kerneltype.IsClassic() {
		// Classic FROM SELECT runs locally.
		require.ErrorContains(t, tk.ExecToErr(sql), "mock import from select setup error")
		return
	}
	vardef.CloudStorageURI.Store("")
	require.ErrorContains(t, tk.ExecToErr(sql), "IMPORT INTO FROM SELECT without global sort storage")
	// With a sort store, the query is captured and the import reaches the prechecks.
	vardef.CloudStorageURI.Store("s3://query-sort")
	tk.MustExec("insert into query_route_dst values (1)")
	require.ErrorContains(t, tk.ExecToErr(sql), "target table is not empty")
}

type importPlanKeyspaceStore struct {
	kv.Storage
}

func (importPlanKeyspaceStore) GetKeyspace() string {
	return ""
}

func TestNewImportPlanMatchesResolvedInput(t *testing.T) {
	ctx := context.Background()
	sctx := mock.NewContext()
	defer sctx.Close()
	sctx.Store = importPlanKeyspaceStore{}
	tblInfo := &model.TableInfo{ID: 42, Name: ast.NewCIStr("t"), State: model.StatePublic}
	tbl := tables.MockTableFromMeta(tblInfo)
	format := importer.DataFormatCSV
	value := &expression.Constant{Value: types.NewIntDatum(2), RetType: types.NewFieldType(mysql.TypeLonglong)}
	for _, sourceType := range []importer.DataSourceType{importer.DataSourceTypeFile, importer.DataSourceTypeQuery} {
		t.Run(sourceType.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.csv")
			input := &importer.Input{
				DBName: "test", DBID: 7, TableInfo: tblInfo,
				Path: path, Format: &format, DataSourceType: sourceType,
				Options: []*importer.ImportOption{{Name: "thread", Value: value}},
			}
			physical := &plannercore.ImportInto{
				Table: &resolve.TableNameW{
					TableName: &ast.TableName{Schema: ast.NewCIStr("test"), Name: ast.NewCIStr("t")},
					DBInfo:    &model.DBInfo{ID: 7, Name: ast.NewCIStr("test")},
				},
				Path: path, Format: &format,
				Options: []*plannercore.LoadDataOpt{{Name: "thread", Value: value}},
			}
			if sourceType == importer.DataSourceTypeQuery {
				physical.SelectPlan = &physicalop.PhysicalSelection{}
			}
			fromSQL, err := executor.NewImportPlan(ctx, sctx, physical, tbl)
			require.NoError(t, err)
			require.Equal(t, sourceType, fromSQL.DataSourceType)
			direct, err := importer.NewImportPlanFromInput(ctx, sctx, input)
			require.NoError(t, err)
			require.Equal(t, fromSQL, direct)
			encoded, err := json.Marshal(fromSQL)
			require.NoError(t, err)
			directEncoded, err := json.Marshal(direct)
			require.NoError(t, err)
			require.JSONEq(t, string(encoded), string(directEncoded))

			// Both entrypoints reject the same invalid numeric option.
			bad := &expression.Constant{Value: types.NewStringDatum("bad"), RetType: types.NewFieldType(mysql.TypeVarString)}
			input.Options[0].Value = bad
			physical.Options[0].Value = bad
			_, sqlErr := executor.NewImportPlan(ctx, sctx, physical, tbl)
			_, directErr := importer.NewImportPlanFromInput(ctx, sctx, input)
			require.Error(t, sqlErr)
			require.EqualError(t, directErr, sqlErr.Error())
		})
	}
}

func TestSubmitImportTaskRecovery(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.recoverable(a int primary key)")
	db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	tbl, err := dom.InfoSchema().TableByName(context.Background(), db.Name, ast.NewCIStr("recoverable"))
	require.NoError(t, err)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return testkit.NewTestKit(t, store).Session(), nil
	}, 3, 3, time.Second)
	defer pool.Close()
	manager := storage.NewTaskManager(pool)
	storage.SetTaskManager(manager)
	ctx := tikvutil.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	require.NoError(t, manager.InitMeta(ctx, "tidb", "dxf_service"))
	var claimedJob int64
	var durable bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportTableModeClaimed", func(id int64, claimErr *error) {
		require.NoError(t, *claimErr)
		claimedJob = id
		durable = len(tk.MustQuery("select id from mysql.tidb_import_jobs where id = ?", id).Rows()) == 1
		*claimErr = context.Canceled
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/beforeImportCancelCleanup", func(cleanupErr *error) {
		*cleanupErr = context.Canceled
	})
	_, _, err = executor.SubmitImportTask(ctx, &importer.Plan{
		DBID: db.ID, DBName: db.Name.O, TableInfo: tbl.Meta(), Keyspace: store.GetKeyspace(), Parameters: &importer.ImportParameters{},
	}, "import into test.recoverable from '/path/to/file'")
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, durable, "a committed mode claim must have a durable recovery job")
	tbl, err = dom.InfoSchema().TableByName(ctx, db.Name, tbl.Meta().Name)
	require.NoError(t, err)
	require.Equal(t, model.TableModeImport, tbl.Meta().Mode)
	require.Equal(t, claimedJob, tbl.Meta().ImportJobID)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/executor/beforeImportCancelCleanup")
	recovery := testkit.NewTestKit(t, store)
	recovery.MustExec(fmt.Sprintf("cancel import job %d", claimedJob))
	tbl, err = dom.InfoSchema().TableByName(ctx, db.Name, tbl.Meta().Name)
	require.NoError(t, err)
	require.Equal(t, model.TableModeNormal, tbl.Meta().Mode)
}

func TestSubmitImportTaskNextgen(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("This test is only for nextgen")
	}
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/MockDisableDistTask", "return(true)")

	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 10})
	defer cluster.Terminate(t)
	keyspaceIDs := map[string]uint32{
		keyspace.System: 1,
		"ks":            2,
	}
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/injectETCDCli",
		func(cliP **clientv3.Client, ks string) {
			id, ok := keyspaceIDs[ks]
			require.True(t, ok)
			// one client per ks
			*cliP = cluster.Client(int(id - 1))
			// we will close the client.
			cluster.TakeClient(int(id - 1))
			codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: id}, Name: ks})
			require.NoError(t, err)
			etcd.SetEtcdCliByNamespace(*cliP, keyspace.MakeKeyspaceEtcdNamespace(codec))
		},
	)
	sysKSStore, _ := testkit.CreateMockStoreAndDomainForKS(t, keyspace.System)
	sysKSTK := testkit.NewTestKit(t, sysKSStore)
	// in uni-store, Store instances are completely isolated, even they have the
	// same keyspace name, so we store them here and mock the GetStore
	// TODO use a shared storage for all Store instances.
	storeMap := make(map[string]kv.Storage, 4)
	storeMap[keyspace.System] = sysKSStore
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore",
		func(fnP *func(string) (store kv.Storage, err error)) {
			*fnP = func(ks string) (store kv.Storage, err error) {
				return storeMap[ks], nil
			}
		},
	)
	userKSStore, _ := testkit.CreateMockStoreAndDomainForKS(t, "ks")
	storeMap["ks"] = userKSStore
	userKSTK := testkit.NewTestKit(t, userKSStore)

	// The synchronous Starter case below requires four task slots. Keep the
	// fixture independent of resource initialization in preceding tests.
	originalResource := storage.GetNodeResource()
	require.NotNil(t, originalResource)
	resource := *originalResource
	resource.TotalCPU = max(4, resource.TotalCPU)
	storage.SetNodeResource(&resource)
	t.Cleanup(func() { storage.SetNodeResource(originalResource) })

	ctx := tikvutil.WithInternalSourceType(context.Background(), kv.InternalDistTask)

	manuallyInitFn := func(t *testing.T, currKSStore, sysKSStore kv.Storage) *storage.TaskManager {
		t.Helper()
		// as we have disabled the dist task in domain, we need init the task manager
		// and framework meta manually.
		getPoolFn := func(store kv.Storage) tidbutil.SessionPool {
			pool := pools.NewResourcePool(func() (pools.Resource, error) {
				return testkit.NewTestKit(t, store).Session(), nil
			}, 2, 2, time.Second)
			t.Cleanup(func() {
				pool.Close()
			})
			return pool
		}
		taskMgr := storage.NewTaskManager(getPoolFn(currKSStore))
		storage.SetTaskManager(taskMgr)
		sysKSTaskMgr := taskMgr
		if kv.IsUserKS(currKSStore) {
			sysKSTaskMgr = storage.NewTaskManager(getPoolFn(sysKSStore))
			storage.SetDXFSvcTaskMgr(sysKSTaskMgr)
		}
		require.NoError(t, sysKSTaskMgr.InitMeta(ctx, "tidb", "dxf_service"))
		return sysKSTaskMgr
	}
	setupUserKeyspaceImportJob := func(t *testing.T) (int64, *storage.TaskManager) {
		t.Helper()
		sysKSTK.MustExec("delete from mysql.tidb_import_jobs")
		sysKSTK.MustExec("delete from mysql.tidb_global_task")
		sysKSTK.MustExec("delete from mysql.tidb_global_task_history")
		userKSTK.MustExec("delete from mysql.tidb_import_jobs")
		userKSTK.MustExec("delete from mysql.tidb_global_task")
		userKSTK.MustExec("delete from mysql.tidb_global_task_history")
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = "ks"
		})
		sysKSTaskMgr := manuallyInitFn(t, userKSStore, sysKSStore)

		conn := userKSTK.Session().GetSQLExecutor()
		jobID, err := importer.CreateJob(ctx, conn, "test", "t", 1,
			userKSTK.Session().GetSessionVars().User.String(), "", &importer.ImportParameters{}, 123)
		require.NoError(t, err)
		return jobID, sysKSTaskMgr
	}

	submissionTableID := 0
	newSubmissionPlan := func(tk *testkit.TestKit) *importer.Plan {
		submissionTableID++
		tableName := fmt.Sprintf("submit_target_%d", submissionTableID)
		tk.MustExec("create table test." + tableName + "(a int primary key)")
		is := domain.GetDomain(tk.Session()).InfoSchema()
		db, ok := is.SchemaByName(ast.NewCIStr("test"))
		require.True(t, ok)
		tbl, err := is.TableByName(ctx, db.Name, ast.NewCIStr(tableName))
		require.NoError(t, err)
		return &importer.Plan{DBID: db.ID, DBName: db.Name.O, TableInfo: tbl.Meta(), Keyspace: tk.Session().GetStore().GetKeyspace(), Parameters: &importer.ImportParameters{}}
	}
	t.Run("old SYSTEM lifecycle rejects submission before changing mode", func(t *testing.T) {
		config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = "ks" })
		manuallyInitFn(t, userKSStore, sysKSStore)
		plan := newSubmissionPlan(userKSTK)
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/serverinfo/mockGetAllServerInfo", "return(`{\"new\":{\"schema_read_only\":true},\"old\":{}}`)")
		_, _, err := executor.SubmitImportTask(ctx, plan, "import into t from '/path/to/file'")
		require.ErrorContains(t, err, "SYSTEM/DXF")
		tbl, err := domain.GetDomain(userKSTK.Session()).InfoSchema().TableByName(ctx, ast.NewCIStr("test"), plan.TableInfo.Name)
		require.NoError(t, err)
		require.Equal(t, model.TableModeNormal, tbl.Meta().Mode)
		userKSTK.MustQuery("select count(*) from mysql.tidb_import_jobs where table_id = ?", plan.TableInfo.ID).Check(testkit.Rows("0"))
	})
	t.Run("assumed SYSTEM entries cannot establish lifecycle support", func(t *testing.T) {
		config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = "ks" })
		manuallyInitFn(t, userKSStore, sysKSStore)
		plan := newSubmissionPlan(userKSTK)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterGetImportLifecycleServers", func(component string, servers map[string]*serverinfo.ServerInfo) {
			if component == "SYSTEM/DXF" {
				clear(servers)
				servers["assumed"] = &serverinfo.ServerInfo{StaticInfo: serverinfo.StaticInfo{AssumedKeyspace: keyspace.System, SchemaReadOnly: true}}
			}
		})
		_, _, err := executor.SubmitImportTask(ctx, plan, "import into t from '/path/to/file'")
		require.ErrorContains(t, err, "cannot determine SYSTEM/DXF")
		tbl, err := domain.GetDomain(userKSTK.Session()).InfoSchema().TableByName(ctx, ast.NewCIStr("test"), plan.TableInfo.Name)
		require.NoError(t, err)
		require.Equal(t, model.TableModeNormal, tbl.Meta().Mode)
		userKSTK.MustQuery("select count(*) from mysql.tidb_import_jobs where table_id = ?", plan.TableInfo.ID).Check(testkit.Rows("0"))
	})
	t.Run("mode claim has a durable recoverable job", func(t *testing.T) {
		config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = "ks" })
		manuallyInitFn(t, userKSStore, sysKSStore)
		plan := newSubmissionPlan(userKSTK)
		var claimedJob int64
		var durable bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportTableModeClaimed", func(id int64, err *error) {
			claimedJob = id
			durable = len(userKSTK.MustQuery("select id from mysql.tidb_import_jobs where id = ?", id).Rows()) == 1
			*err = context.Canceled
		})
		// Model a lost submitter whose in-process compensation cannot finish.
		cleanupFails := true
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/beforeImportCancelCleanup", func(err *error) {
			if cleanupFails {
				*err = context.Canceled
			}
		})
		_, _, err := executor.SubmitImportTask(ctx, plan, "import into t from '/path/to/file'")
		require.Error(t, err)
		require.True(t, durable, "mode ownership must never outlive its durable import job")
		claimedTable, lookupErr := domain.GetDomain(userKSTK.Session()).InfoSchema().TableByName(ctx, ast.NewCIStr(plan.DBName), plan.TableInfo.Name)
		require.NoError(t, lookupErr)
		require.Equal(t, model.TableModeImport, claimedTable.Meta().Mode)
		require.Equal(t, claimedJob, claimedTable.Meta().ImportJobID)
		cleanupFails = false
		// A fresh session uses the ordinary recovery entrypoint, with no submitter state.
		recovery := testkit.NewTestKit(t, userKSStore)
		recovery.MustExec(fmt.Sprintf("cancel import job %d", claimedJob))
		tbl, err := domain.GetDomain(recovery.Session()).InfoSchema().TableByName(ctx, ast.NewCIStr(plan.DBName), plan.TableInfo.Name)
		require.NoError(t, err)
		require.Equal(t, model.TableModeNormal, tbl.Meta().Mode)
	})
	t.Run("cancel waits for a pending mode claim", func(t *testing.T) {
		config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = "ks" })
		manuallyInitFn(t, userKSStore, sysKSStore)
		plan := newSubmissionPlan(userKSTK)
		claimed := make(chan int64, 1)
		resume := make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		submitDone := make(chan error, 1)
		cancelDone := make(chan error, 1)
		cancelStarted := make(chan struct{}, 1)
		var wg sync.WaitGroup
		defer func() { release(); wg.Wait() }()
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportTableModeClaimed", func(id int64, err *error) {
			claimed <- id
			<-resume
			*err = context.Canceled
		})
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/beforeCancelDanglingImportJob", func(int64) {
			select {
			case cancelStarted <- struct{}{}:
			default:
			}
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := executor.SubmitImportTask(ctx, plan, "import into t from '/path/to/file'")
			submitDone <- err
		}()
		var id int64
		select {
		case id = <-claimed:
		case <-time.After(5 * time.Second):
			t.Fatal("import did not claim table mode")
		}
		cancelTK := testkit.NewTestKit(t, userKSStore)
		wg.Add(1)
		go func() { defer wg.Done(); cancelDone <- cancelTK.ExecToErr(fmt.Sprintf("cancel import job %d", id)) }()
		select {
		case <-cancelStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("CANCEL did not reach the pending job")
		}
		select {
		case err := <-cancelDone:
			t.Fatalf("CANCEL finished while the mode claim was locked: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		release()
		require.Error(t, <-submitDone)
		require.NoError(t, <-cancelDone)
		tbl, err := domain.GetDomain(userKSTK.Session()).InfoSchema().TableByName(ctx, ast.NewCIStr(plan.DBName), plan.TableInfo.Name)
		require.NoError(t, err)
		require.Equal(t, model.TableModeNormal, tbl.Meta().Mode)
	})
	t.Run("cancel before mode claim prevents a late claim", func(t *testing.T) {
		config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = "ks" })
		manuallyInitFn(t, userKSStore, sysKSStore)
		plan := newSubmissionPlan(userKSTK)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/beforeImportTableModeClaim", func(id int64) {
			userKSTK.MustExec(fmt.Sprintf("cancel import job %d", id))
		})
		_, _, err := executor.SubmitImportTask(ctx, plan, "import into t from '/path/to/file'")
		require.ErrorContains(t, err, "is no longer pending")
		tbl, err := domain.GetDomain(userKSTK.Session()).InfoSchema().TableByName(ctx, ast.NewCIStr(plan.DBName), plan.TableInfo.Name)
		require.NoError(t, err)
		require.Equal(t, model.TableModeNormal, tbl.Meta().Mode)
		userKSTK.MustQuery("select status from mysql.tidb_import_jobs where table_id = ?", plan.TableInfo.ID).Check(testkit.Rows("cancelled"))
	})
	t.Run("old user keyspace owner rejects submission after SYSTEM upgrade", func(t *testing.T) {
		config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = "ks" })
		manuallyInitFn(t, userKSStore, sysKSStore)
		plan := newSubmissionPlan(userKSTK)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterGetImportLifecycleServers",
			func(component string, servers map[string]*serverinfo.ServerInfo) {
				if component == "TiDB" {
					servers["old"] = &serverinfo.ServerInfo{}
				}
			})
		_, _, err := executor.SubmitImportTask(ctx, plan, "import into t from '/path/to/file'")
		require.ErrorContains(t, err, "finish upgrading all TiDB nodes")
		tbl, err := domain.GetDomain(userKSTK.Session()).InfoSchema().TableByName(ctx, ast.NewCIStr("test"), plan.TableInfo.Name)
		require.NoError(t, err)
		require.Equal(t, model.TableModeNormal, tbl.Meta().Mode)
		userKSTK.MustQuery("select count(*) from mysql.tidb_import_jobs where table_id = ?", plan.TableInfo.ID).Check(testkit.Rows("0"))
	})
	t.Run("failed submission preserves another import owner", func(t *testing.T) {
		config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = "ks" })
		manuallyInitFn(t, userKSStore, sysKSStore)
		plan := newSubmissionPlan(userKSTK)
		require.Equal(t, model.TableModeNormal, plan.TableInfo.Mode)
		dom := domain.GetDomain(userKSTK.Session())
		require.NoError(t, dom.DDLExecutor().AlterTableMode(userKSTK.Session(), &model.AlterTableModeArgs{
			SchemaID: plan.DBID, TableID: plan.TableInfo.ID, TableMode: model.TableModeImport, ImportJobID: 999999,
		}))
		_, _, err := executor.SubmitImportTask(ctx, plan, "import into t from '/path/to/file'")
		require.Error(t, err)
		tbl, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), plan.TableInfo.Name)
		require.NoError(t, err)
		require.Equal(t, model.TableModeImport, tbl.Meta().Mode)
		require.EqualValues(t, 999999, tbl.Meta().ImportJobID)
		require.NoError(t, dom.DDLExecutor().AlterTableMode(userKSTK.Session(), &model.AlterTableModeArgs{
			SchemaID: plan.DBID, TableID: plan.TableInfo.ID, TableMode: model.TableModeNormal, ImportJobID: 999999,
		}))
	})
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprintf("allocated task rollback uncertain=%t", uncertain), func(t *testing.T) {
			config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = "ks" })
			manuallyInitFn(t, userKSStore, sysKSStore)
			plan := newSubmissionPlan(userKSTK)
			defer func() {
				require.NoError(t, domain.GetDomain(userKSTK.Session()).DDLExecutor().AlterTableMode(userKSTK.Session(), &model.AlterTableModeArgs{
					SchemaID: plan.DBID, TableID: plan.TableInfo.ID, TableMode: model.TableModeNormal,
				}))
			}()
			var allocated int64
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterImportTaskIDAllocated", func(id int64, err *error) {
				allocated = id
				*err = context.Canceled
				if uncertain {
					*err = terror.ErrResultUndetermined
				}
			})
			_, _, err := executor.SubmitImportTask(ctx, plan, "import into t from '/path/to/file'")
			require.Error(t, err)
			require.Positive(t, allocated)
			sysKSTK.MustQuery("select count(*) from mysql.tidb_global_task where id = ?", allocated).Check(testkit.Rows("0"))
			dom := domain.GetDomain(userKSTK.Session())
			tbl, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), plan.TableInfo.Name)
			require.NoError(t, err)
			if uncertain {
				require.Equal(t, model.TableModeImport, tbl.Meta().Mode)
				jobs := userKSTK.MustQuery("select id from mysql.tidb_import_jobs where table_id = ?", plan.TableInfo.ID).Rows()
				userKSTK.MustExec(fmt.Sprintf("cancel import job %s", jobs[0][0]))
				tbl, err = dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), plan.TableInfo.Name)
				require.NoError(t, err)
			}
			require.Equal(t, model.TableModeNormal, tbl.Meta().Mode)
		})
	}

	t.Run("submit task in system keyspace", func(t *testing.T) {
		userKSTK.MustExec("delete from mysql.tidb_import_jobs")
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = keyspace.System
		})
		manuallyInitFn(t, sysKSStore, sysKSStore)
		jobID, task, err := executor.SubmitImportTask(ctx, newSubmissionPlan(sysKSTK), "import into t from '/path/to/file'")
		require.NoError(t, err)
		// both inside system keyspace
		sysKSTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("1"))
		sysKSTK.MustQuery("select count(1) from mysql.tidb_global_task where id = ?", task.ID).Check(testkit.Rows("1"))
		// user keyspace should not have the job
		userKSTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("0"))
		userKSTK.MustQuery("select count(1) from mysql.tidb_global_task where id = ?", task.ID).Check(testkit.Rows("0"))
	})

	t.Run("submit task in user keyspace", func(t *testing.T) {
		sysKSTK.MustExec("delete from mysql.tidb_import_jobs")
		userKSTK.MustExec("delete from mysql.tidb_global_task")
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = "ks"
		})
		manuallyInitFn(t, userKSStore, sysKSStore)
		jobID, task, err := executor.SubmitImportTask(ctx, newSubmissionPlan(userKSTK), "import into t from '/path/to/file'")
		require.NoError(t, err)
		// job created in user keyspace, task created in system keyspace
		userKSTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("1"))
		sysKSTK.MustQuery("select count(1) from mysql.tidb_global_task where id = ?", task.ID).Check(testkit.Rows("1"))
		// the reverse is not true.
		sysKSTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("0"))
		userKSTK.MustQuery("select count(1) from mysql.tidb_global_task where id = ?", task.ID).Check(testkit.Rows("0"))
	})

	for _, failurePoint := range []string{"beforeImportCancelCleanup", "beforeImportCancelModeRelease"} {
		t.Run("cancelled import retries failed mode cleanup/"+failurePoint, func(t *testing.T) {
			_, _ = setupUserKeyspaceImportJob(t)
			plan := newSubmissionPlan(userKSTK)
			var jobID int64
			jobID, createErr := importer.CreateJob(ctx, userKSTK.Session().GetSQLExecutor(), "test", plan.TableInfo.Name.L,
				plan.TableInfo.ID, userKSTK.Session().GetSessionVars().User.String(), "", &importer.ImportParameters{}, 1)
			require.NoError(t, createErr)
			args := &model.AlterTableModeArgs{SchemaID: plan.DBID, TableID: plan.TableInfo.ID, TableMode: model.TableModeImport, ImportJobID: jobID}
			require.NoError(t, domain.GetDomain(userKSTK.Session()).DDLExecutor().AlterTableMode(userKSTK.Session(), args))
			failCleanup := true
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/"+failurePoint, func(err *error) {
				if failCleanup {
					*err = fmt.Errorf("transient import cleanup failure")
				}
			})
			require.ErrorContains(t, userKSTK.ExecToErr(fmt.Sprintf("cancel import job %d", jobID)), "transient import cleanup failure")
			userKSTK.MustQuery("select status from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("cancelled"))
			failCleanup = false
			userKSTK.MustExec(fmt.Sprintf("cancel import job %d", jobID))
			tbl, lookupErr := domain.GetDomain(userKSTK.Session()).InfoSchema().TableByName(ctx, ast.NewCIStr("test"), plan.TableInfo.Name)
			require.NoError(t, lookupErr)
			require.Equal(t, model.TableModeNormal, tbl.Meta().Mode)
		})
	}

	t.Run("cancel dangling user keyspace job without DXF task", func(t *testing.T) {
		jobID, _ := setupUserKeyspaceImportJob(t)
		sysKSTK.MustQuery("select count(1) from mysql.tidb_global_task where task_key = ?", taskkey.ForJob(jobID)).
			Check(testkit.Rows("0"))

		userKSTK.MustExec(fmt.Sprintf("cancel import job %d", jobID))

		userKSTK.MustQuery("select status, error_message from mysql.tidb_import_jobs where id = ?", jobID).
			Check(testkit.Rows("cancelled cancelled by user"))
		sysKSTK.MustQuery("select count(1) from mysql.tidb_global_task where task_key = ?", taskkey.ForJob(jobID)).
			Check(testkit.Rows("0"))
	})

	t.Run("cancel user keyspace job with an archived reverted DXF task", func(t *testing.T) {
		jobID, sysKSTaskMgr := setupUserKeyspaceImportJob(t)
		taskID, err := sysKSTaskMgr.CreateTask(ctx, taskkey.ForJob(jobID), proto.ImportInto, "", 1, "", 0, proto.ExtraParams{}, nil)
		require.NoError(t, err)
		// This is a synthetic state, not a normal real-world scheduler outcome:
		// onReverting calls importScheduler.OnDone to fail or cancel the job before
		// RevertedTask marks the DXF task reverted. Bypassing OnDone here isolates
		// the check that an existing terminal task is not treated as a missing task.
		require.NoError(t, sysKSTaskMgr.RevertTask(ctx, taskID, proto.TaskStatePending, fmt.Errorf("already reverted")))
		require.NoError(t, sysKSTaskMgr.RevertedTask(ctx, taskID))
		task, err := sysKSTaskMgr.GetTaskByID(ctx, taskID)
		require.NoError(t, err)
		require.NoError(t, sysKSTaskMgr.TransferTasks2History(ctx, []*proto.Task{task}))
		sysKSTK.MustQuery("select count(1) from mysql.tidb_global_task where id = ?", taskID).
			Check(testkit.Rows("0"))
		sysKSTK.MustQuery("select state from mysql.tidb_global_task_history where id = ?", taskID).
			Check(testkit.Rows(string(proto.TaskStateReverted)))

		userKSTK.MustExec(fmt.Sprintf("cancel import job %d", jobID))

		userKSTK.MustQuery("select status from mysql.tidb_import_jobs where id = ?", jobID).
			Check(testkit.Rows("pending"))
		sysKSTK.MustQuery("select state from mysql.tidb_global_task_history where id = ?", taskID).
			Check(testkit.Rows(string(proto.TaskStateReverted)))
	})

	t.Run("cancel pending user keyspace job when task is created after lookup miss", func(t *testing.T) {
		jobID, sysKSTaskMgr := setupUserKeyspaceImportJob(t)
		sysKSTK.MustQuery("select count(1) from mysql.tidb_global_task where task_key = ?", taskkey.ForJob(jobID)).
			Check(testkit.Rows("0"))

		var lateTaskID atomic.Int64
		callbackErrCh := make(chan error, 1)
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/afterCancelImportTaskProbeMiss",
			func(probeMissJobID int64) {
				if probeMissJobID != jobID {
					callbackErrCh <- fmt.Errorf("unexpected job ID %d", probeMissJobID)
					return
				}
				taskID, err := sysKSTaskMgr.CreateTask(ctx, taskkey.ForJob(jobID), proto.ImportInto, "", 1, "", 0, proto.ExtraParams{}, nil)
				if err != nil {
					callbackErrCh <- err
					return
				}
				lateTaskID.Store(taskID)
			},
		)

		cancelErrCh := make(chan error, 1)
		go func() {
			_, err := userKSTK.Exec(fmt.Sprintf("cancel import job %d", jobID))
			cancelErrCh <- err
		}()

		select {
		case err := <-cancelErrCh:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			if taskID := lateTaskID.Load(); taskID != 0 {
				require.NoError(t, sysKSTaskMgr.FailTask(ctx, taskID, proto.TaskStatePending, fmt.Errorf("unblock timed out cancel")))
				<-cancelErrCh
			}
			require.FailNow(t, "cancel import waited for a task created after the cancel-by-key miss")
		}
		select {
		case err := <-callbackErrCh:
			require.NoError(t, err)
		default:
		}

		userKSTK.MustQuery("select status, error_message from mysql.tidb_import_jobs where id = ?", jobID).
			Check(testkit.Rows("cancelled cancelled by user"))
		sysKSTK.MustQuery("select state from mysql.tidb_global_task where task_key = ?", taskkey.ForJob(jobID)).
			Check(testkit.Rows(string(proto.TaskStatePending)))
	})

	t.Run("submit global-sort task uses async prepare mode", func(t *testing.T) {
		originalMode := deploymode.Get()
		t.Cleanup(func() {
			require.NoError(t, deploymode.Set(originalMode))
		})
		require.NoError(t, deploymode.Set(deploymode.Premium))
		sysKSTK.MustExec("delete from mysql.tidb_import_jobs")
		sysKSTK.MustExec("delete from mysql.tidb_global_task")
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = keyspace.System
		})
		manuallyInitFn(t, sysKSStore, sysKSStore)

		plan := newSubmissionPlan(sysKSTK)
		plan.ThreadCnt = 16
		plan.MaxNodeCnt = 8
		plan.CloudStorageURI = "local:///tmp/prepare-mode-sort"
		jobID, task, err := executor.SubmitImportTask(ctx, plan, "import into t from 's3://bucket/test.csv'")
		require.NoError(t, err)
		sysKSTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("1"))
		sysKSTK.MustQuery(
			"select concurrency, max_node_count, json_extract(extra_params, '$.prepare_mode') "+
				"from mysql.tidb_global_task where id = ?",
			task.ID,
		).Check(testkit.Rows("1 1 1"))
	})

	t.Run("submit global-sort task in Starter uses synchronous prepare", func(t *testing.T) {
		originalMode := deploymode.Get()
		t.Cleanup(func() {
			require.NoError(t, deploymode.Set(originalMode))
		})
		require.NoError(t, deploymode.Set(deploymode.Starter))
		sysKSTK.MustExec("delete from mysql.tidb_import_jobs")
		sysKSTK.MustExec("delete from mysql.tidb_global_task")
		config.UpdateGlobal(func(conf *config.Config) {
			conf.KeyspaceName = keyspace.System
		})
		manuallyInitFn(t, sysKSStore, sysKSStore)

		plan := newSubmissionPlan(sysKSTK)
		plan.ThreadCnt = 4
		plan.MaxNodeCnt = 2
		plan.CloudStorageURI = "local:///tmp/prepare-mode-sort-starter"
		jobID, task, err := executor.SubmitImportTask(ctx, plan, "import into t from 's3://bucket/test.csv'")
		require.NoError(t, err)
		sysKSTK.MustQuery("select count(1) from mysql.tidb_import_jobs where id = ?", jobID).Check(testkit.Rows("1"))
		sysKSTK.MustQuery(
			"select concurrency, max_node_count, json_extract(extra_params, '$.prepare_mode') is null "+
				"from mysql.tidb_global_task where id = ?",
			task.ID,
		).Check(testkit.Rows("4 2 1"))
	})
}
