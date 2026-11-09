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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/pkdbsummary"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
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

// TestImportJobStatisticsVisibility gives two users different import jobs and
// queries the new statistics table without mysql-table SELECT privileges. A user
// sees only their job until explicitly granted IMPORT_JOB_VIEW.
func TestImportJobStatisticsVisibility(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	root.MustExec("create user 'statistics_owner'@'localhost'")
	root.MustExec("create user 'statistics_viewer'@'localhost'")
	owner := testkit.NewTestKit(t, store)
	viewer := testkit.NewTestKit(t, store)
	require.NoError(t, owner.Session().Auth(&auth.UserIdentity{Username: "statistics_owner", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, viewer.Session().Auth(&auth.UserIdentity{Username: "statistics_viewer", Hostname: "localhost"}, nil, nil, nil))
	ctx := tikvutil.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	manager, err := storage.GetTaskManager()
	require.NoError(t, err)
	require.NoError(t, manager.InitMeta(ctx, ":4000", ""))
	for _, user := range []*testkit.TestKit{owner, viewer} {
		id, err := importer.CreateJob(ctx, root.Session().GetSQLExecutor(), "test", "table", 1, user.Session().GetSessionVars().User.String(), &importer.ImportParameters{Format: importer.DataFormatCSV}, 0)
		require.NoError(t, err)
		require.NoError(t, importer.StartJob(ctx, root.Session().GetSQLExecutor(), id, importer.JobStepImporting))
		result := pkdbsummary.NewResult(2, true, pkdbsummary.Outcomes{Imported: 1, Skipped: 1})
		require.NoError(t, importer.FinishJob(ctx, root.Session().GetSQLExecutor(), id, &importer.JobSummary{ImportStatistics: &result}))
	}
	query := "select json_extract(statistics,'$.total'),json_extract(statistics,'$.imported'),json_extract(statistics,'$.skipped') from information_schema.PKDB_IMPORT_JOB_STATISTICS"
	owner.MustQuery(query).Check(testkit.Rows("2 1 1"))
	viewer.MustQuery(query).Check(testkit.Rows("2 1 1"))
	root.MustExec("grant IMPORT_JOB_VIEW on *.* to 'statistics_viewer'@'localhost'")
	viewer.MustQuery(query).Check(testkit.Rows("2 1 1", "2 1 1"))
}
