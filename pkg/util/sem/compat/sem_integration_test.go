// Copyright 2025 PingCAP, Inc.
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

package compat_test

import (
	"net/url"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/parser/auth"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/sem/compat"
	"github.com/stretchr/testify/require"
)

func TestRestrictedSQL(t *testing.T) {
	cleanup := compat.SwitchToSEMForTest(t, compat.V2)
	t.Cleanup(cleanup)

	// Test the behavior of the RESTRICTED_SQL_ADMIN privilege
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t (id int);")

	tk.MustExec("CREATE USER nobodyuser, semuser")
	tk.MustExec("GRANT ALL PRIVILEGES ON *.* TO nobodyuser")
	tk.MustExec("GRANT ALL PRIVILEGES ON *.* TO semuser")
	tk.MustExec("GRANT RESTRICTED_SQL_ADMIN ON *.* TO semuser")

	t.Run("IMPORT INTO with external-id is not allowed", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)

		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "nobodyuser", Hostname: "localhost"}, nil, nil, nil))
		t.Cleanup(func() {
			require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		})
		tk.MustContainErrMsg("IMPORT INTO test.t FROM 's3://bucket?EXTERNAL-ID=abc'", "is not supported when security enhanced mode is enabled")

		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "semuser", Hostname: "localhost"}, nil, nil, nil))
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

	t.Run("ALTER RESOURCE GROUP is not allowed", func(t *testing.T) {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("CREATE RESOURCE GROUP rg RU_PER_SEC=1000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' ACTION=KILL)")

		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "nobodyuser", Hostname: "localhost"}, nil, nil, nil))
		t.Cleanup(func() {
			require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		})
		tk.MustContainErrMsg("ALTER RESOURCE GROUP rg RU_PER_SEC=500", "is not supported when security enhanced mode is enabled")

		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "semuser", Hostname: "localhost"}, nil, nil, nil))
		tk.MustExec("ALTER RESOURCE GROUP rg RU_PER_SEC=500")
	})
}
