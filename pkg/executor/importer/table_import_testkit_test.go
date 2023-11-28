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

package importer_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
)

func TestChecksumTable(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()

	plan := &importer.Plan{
		DBName: "db",
		TableInfo: &model.TableInfo{
			Name: model.NewCIStr("tb"),
		},
	}
	// fake result
	localChecksum := verify.MakeKVChecksum(1, 1, 1)
	tk.MustExec("create database db")
	tk.MustExec("create table db.tb(id int)")
	tk.MustExec("insert into db.tb values(1)")
	remoteChecksum, err := importer.TestChecksumTable(ctx, tk.Session(), plan, logutil.BgLogger())
	require.NoError(t, err)
	require.True(t, remoteChecksum.IsEqual(&localChecksum))
	// again
	remoteChecksum, err = importer.TestChecksumTable(ctx, tk.Session(), plan, logutil.BgLogger())
	require.NoError(t, err)
	require.True(t, remoteChecksum.IsEqual(&localChecksum))

	_ = failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum", `return(true)`)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum")
	}()
	remoteChecksum, err = importer.TestChecksumTable(ctx, tk.Session(), plan, logutil.BgLogger())
	require.NoError(t, err)
	require.True(t, remoteChecksum.IsEqual(&localChecksum))
}

func TestImportFromSelectCleanup(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	checkImportDirEmpty := func() {
		tidbCfg := tidb.GetGlobalConfig()
		sortPathSuffix := "import-" + strconv.Itoa(int(tidbCfg.Port))
		importDir := filepath.Join(tidbCfg.TempDir, sortPathSuffix)
		if _, err := os.Stat(importDir); err != nil {
			require.True(t, os.IsNotExist(err), importDir)
		} else {
			entries, err := os.ReadDir(importDir)
			require.NoError(t, err)
			if len(entries) != 0 {
				// we didn't close backend here, so there should be an empty dir left.
				for _, en := range entries {
					require.True(t, en.IsDir())
					require.True(t, common.IsEmptyDir(filepath.Join(importDir, en.Name())))
				}
				require.NoError(t, os.RemoveAll(importDir))
			}
		}
	}
	checkImportDirEmpty()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/mockImportFromSelectErr", `return(true)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/skipAllocateTS", `return(true)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/mockImportFromSelectErr"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/lightning/backend/local/skipAllocateTS"))
	})

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	dbInfo, ok := do.InfoSchema().SchemaByName(model.NewCIStr("test"))
	require.True(t, ok)
	table, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	plan, err := importer.NewImportPlan(tk.Session(), &plannercore.ImportInto{
		Table: &ast.TableName{
			Name: model.NewCIStr("t"),
			DBInfo: &model.DBInfo{
				Name: model.NewCIStr("test"),
				ID:   dbInfo.ID,
			},
		},
		SelectPlan: &plannercore.PhysicalSelection{},
	}, table)
	require.NoError(t, err)
	controller, err := importer.NewLoadDataController(plan, table, &importer.ASTArgs{})
	require.NoError(t, err)
	ti, err := importer.NewTableImporterForTest(
		&importer.JobImportParam{
			GroupCtx: ctx,
		},
		controller,
		"11",
		store,
	)
	require.NoError(t, err)
	ch := make(chan importer.QueryRow)
	ti.SetSelectedRowCh(ch)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ch)
		for i := 1; i <= 3; i++ {
			ch <- importer.QueryRow{
				ID: int64(i),
				Data: []types.Datum{
					types.NewIntDatum(int64(i)),
				},
			}
		}
	}()
	_, err = ti.ImportSelectedRows(ctx, tk.Session())
	require.ErrorContains(t, err, "mock import from select error")
	wg.Wait()
	// we don't close TableImporter here, as there are too many dependencies to
	// mock for backend.Close to work.
	checkImportDirEmpty()
}
