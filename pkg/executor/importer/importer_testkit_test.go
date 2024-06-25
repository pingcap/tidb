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
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/mock"
	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestVerifyChecksum(t *testing.T) {
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
		Checksum:               config.OpLevelRequired,
		DistSQLScanConcurrency: 50,
	}
	tk.MustExec("create database db")
	tk.MustExec("create table db.tb(id int)")
	tk.MustExec("insert into db.tb values(1)")

	// admin checksum table always return 1, 1, 1 for memory store
	// Checksum = required
	backupDistScanCon := tk.Session().GetSessionVars().DistSQLScanConcurrency()
	require.Equal(t, variable.DefDistSQLScanConcurrency, backupDistScanCon)
	localChecksum := verify.MakeKVChecksum(1, 1, 1)
	err := importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.NoError(t, err)
	require.Equal(t, backupDistScanCon, tk.Session().GetSessionVars().DistSQLScanConcurrency())
	localChecksum = verify.MakeKVChecksum(1, 2, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.ErrorIs(t, err, common.ErrChecksumMismatch)

	// check a slow checksum can be canceled
	plan2 := &importer.Plan{
		DBName: "db",
		TableInfo: &model.TableInfo{
			Name: model.NewCIStr("tb2"),
		},
		Checksum: config.OpLevelRequired,
	}
	tk.MustExec(`
		create table db.tb2(
			id int,
			index idx1(id),
			index idx2(id),
			index idx3(id),
			index idx4(id),
			index idx5(id),
			index idx6(id),
			index idx7(id),
			index idx8(id),
			index idx9(id),
			index idx10(id)
		)`)
	tk.MustExec("insert into db.tb2 values(1)")
	backup, err := tk.Session().GetSessionVars().GetSessionOrGlobalSystemVar(ctx, variable.TiDBChecksumTableConcurrency)
	require.NoError(t, err)
	err = tk.Session().GetSessionVars().SetSystemVar(variable.TiDBChecksumTableConcurrency, "1")
	require.NoError(t, err)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/afterHandleChecksumRequest", `sleep(1000)`))

	ctx2, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err = importer.VerifyChecksum(ctx2, plan2, localChecksum, tk.Session(), logutil.BgLogger())
	require.ErrorContains(t, err, "Query execution was interrupted")

	err = tk.Session().GetSessionVars().SetSystemVar(variable.TiDBChecksumTableConcurrency, backup)
	require.NoError(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/afterHandleChecksumRequest"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum", `3*return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum"))
	}()
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.ErrorContains(t, err, "occur an error when checksum")
	// remote checksum success after retry
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum", `1*return(true)`))
	localChecksum = verify.MakeKVChecksum(1, 1, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.NoError(t, err)

	// checksum = optional
	plan.Checksum = config.OpLevelOptional
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum"))
	localChecksum = verify.MakeKVChecksum(1, 1, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.NoError(t, err)
	localChecksum = verify.MakeKVChecksum(1, 2, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.NoError(t, err)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum", `3*return(true)`))
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.NoError(t, err)

	// checksum = off
	plan.Checksum = config.OpLevelOff
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum"))
	localChecksum = verify.MakeKVChecksum(1, 2, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, tk.Session(), logutil.BgLogger())
	require.NoError(t, err)
}

func TestGetTargetNodeCpuCnt(t *testing.T) {
	_, tm, ctx := testutil.InitTableTest(t)
	old := variable.EnableDistTask.Load()

	variable.EnableDistTask.Store(false)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(16)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu"))
		variable.EnableDistTask.Store(old)
	})
	require.NoError(t, tm.InitMeta(ctx, "tidb1", ""))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)"))
	targetNodeCPUCnt, err := importer.GetTargetNodeCPUCnt(ctx, importer.DataSourceTypeQuery, "")
	require.NoError(t, err)
	require.Equal(t, 8, targetNodeCPUCnt)

	// invalid path
	_, err = importer.GetTargetNodeCPUCnt(ctx, importer.DataSourceTypeFile, ":xx")
	require.ErrorIs(t, err, exeerrors.ErrLoadDataInvalidURI)
	// server disk import
	targetNodeCPUCnt, err = importer.GetTargetNodeCPUCnt(ctx, importer.DataSourceTypeFile, "/path/to/xxx.csv")
	require.NoError(t, err)
	require.Equal(t, 8, targetNodeCPUCnt)
	// disttask disabled
	targetNodeCPUCnt, err = importer.GetTargetNodeCPUCnt(ctx, importer.DataSourceTypeFile, "s3://path/to/xxx.csv")
	require.NoError(t, err)
	require.Equal(t, 8, targetNodeCPUCnt)
	// disttask enabled
	variable.EnableDistTask.Store(true)

	targetNodeCPUCnt, err = importer.GetTargetNodeCPUCnt(ctx, importer.DataSourceTypeFile, "s3://path/to/xxx.csv")
	require.NoError(t, err)
	require.Equal(t, 16, targetNodeCPUCnt)
}

func TestPostProcess(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()

	tk.MustExec("create database db")
	tk.MustExec("create table db.tb(id int primary key)")
	tk.MustExec("insert into db.tb values(1)")
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	dbInfo, ok := do.InfoSchema().SchemaByName(model.NewCIStr("db"))
	require.True(t, ok)
	table, err := do.InfoSchema().TableByName(model.NewCIStr("db"), model.NewCIStr("tb"))
	require.NoError(t, err)
	plan := &importer.Plan{
		DBID:             dbInfo.ID,
		DBName:           "db",
		TableInfo:        table.Meta(),
		DesiredTableInfo: table.Meta(),
		Checksum:         config.OpLevelRequired,
	}
	logger := zap.NewExample()

	// verify checksum failed
	localChecksum := verify.NewKVGroupChecksumForAdd()
	localChecksum.AddRawGroup(verify.DataKVGroupID, 1, 2, 1)
	require.ErrorIs(t, importer.PostProcess(ctx, tk.Session(), nil, plan, localChecksum, logger), common.ErrChecksumMismatch)
	// success
	localChecksum = verify.NewKVGroupChecksumForAdd()
	localChecksum.AddRawGroup(verify.DataKVGroupID, 1, 1, 1)
	require.NoError(t, importer.PostProcess(ctx, tk.Session(), nil, plan, localChecksum, logger))
	// rebase success
	tk.MustExec("create table db.tb2(id int auto_increment primary key)")
	table, err = do.InfoSchema().TableByName(model.NewCIStr("db"), model.NewCIStr("tb2"))
	require.NoError(t, err)
	plan.TableInfo, plan.DesiredTableInfo = table.Meta(), table.Meta()
	integration.BeforeTestExternal(t)
	testEtcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() {
		testEtcdCluster.Terminate(t)
	})
	tidbCfg := tidb.GetGlobalConfig()
	pathBak := tidbCfg.Path
	defer func() {
		tidbCfg.Path = pathBak
	}()
	tidbCfg.Path = testEtcdCluster.Members[0].ClientURLs[0].String()
	require.NoError(t, importer.PostProcess(ctx, tk.Session(), map[autoid.AllocatorType]int64{
		autoid.RowIDAllocType: 123,
	}, plan, localChecksum, logger))
	allocators := table.Allocators(tk.Session().GetTableCtx())
	nextGlobalAutoID, err := allocators.Get(autoid.RowIDAllocType).NextGlobalAutoID()
	require.NoError(t, err)
	require.Equal(t, int64(124), nextGlobalAutoID)
	tk.MustExec("insert into db.tb2 values(default)")
	tk.MustQuery("select * from db.tb2").Check(testkit.Rows("124"))
}

func getTableImporter(ctx context.Context, t *testing.T, store kv.Storage, tableName, path string, opts []*plannercore.LoadDataOpt) *importer.TableImporter {
	tk := testkit.NewTestKit(t, store)
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	dbInfo, ok := do.InfoSchema().SchemaByName(model.NewCIStr("test"))
	require.True(t, ok)
	table, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr(tableName))
	require.NoError(t, err)
	var selectPlan base.PhysicalPlan
	if path == "" {
		selectPlan = &plannercore.PhysicalSelection{}
	}
	plan, err := importer.NewImportPlan(ctx, tk.Session(), &plannercore.ImportInto{
		Path:       path,
		Table:      &ast.TableName{Name: table.Meta().Name, DBInfo: dbInfo},
		Options:    opts,
		SelectPlan: selectPlan,
	}, table)
	require.NoError(t, err)
	controller, err := importer.NewLoadDataController(plan, table, &importer.ASTArgs{})
	require.NoError(t, err)
	if path != "" {
		require.NoError(t, controller.InitDataStore(ctx))
	}
	ti, err := importer.NewTableImporterForTest(ctx, controller, "11", &storeHelper{kvStore: store})
	require.NoError(t, err)
	return ti
}

func TestProcessChunkWith(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tidbCfg := tidb.GetGlobalConfig()
	tidbCfg.TempDir = t.TempDir()

	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int)")
	fileName := path.Join(tidbCfg.TempDir, "test.csv")
	sourceData := []byte("1,2,3\n4,5,6\n7,8,9\n")
	require.NoError(t, os.WriteFile(fileName, sourceData, 0o644))

	keyspace := store.GetCodec().GetKeyspace()
	t.Run("file chunk", func(t *testing.T) {
		chunkInfo := &checkpoints.ChunkCheckpoint{
			FileMeta: mydump.SourceFileMeta{Type: mydump.SourceTypeCSV, Path: "test.csv"},
			Chunk:    mydump.Chunk{EndOffset: int64(len(sourceData)), RowIDMax: 10000},
		}
		ti := getTableImporter(ctx, t, store, "t", fileName, []*plannercore.LoadDataOpt{
			{Name: "skip_rows", Value: expression.NewInt64Const(1)}})
		defer ti.Backend().CloseEngineMgr()
		kvWriter := mock.NewMockEngineWriter(ctrl)
		kvWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		progress := importer.NewProgress()
		checksum := verify.NewKVGroupChecksumWithKeyspace(keyspace)
		err := importer.ProcessChunkWithWriter(ctx, chunkInfo, ti, kvWriter, kvWriter, progress, zap.NewExample(), checksum)
		require.NoError(t, err)
		require.Len(t, progress.GetColSize(), 3)
		checksumMap := checksum.GetInnerChecksums()
		require.Len(t, checksumMap, 1)
		require.Equal(t, verify.MakeKVChecksum(74, 2, 15625182175392723123), *checksumMap[verify.DataKVGroupID])
	})

	t.Run("query chunk", func(t *testing.T) {
		chunkInfo := &checkpoints.ChunkCheckpoint{
			FileMeta: mydump.SourceFileMeta{Type: mydump.SourceTypeCSV, Path: "test.csv"},
			Chunk:    mydump.Chunk{EndOffset: int64(len(sourceData)), RowIDMax: 10000},
		}
		ti := getTableImporter(ctx, t, store, "t", "", nil)
		defer ti.Backend().CloseEngineMgr()
		rowsCh := make(chan importer.QueryRow, 3)
		for i := 1; i <= 3; i++ {
			rowsCh <- importer.QueryRow{
				ID: int64(i),
				Data: []types.Datum{
					types.NewIntDatum(int64((i-1)*3 + 1)),
					types.NewIntDatum(int64((i-1)*3 + 2)),
					types.NewIntDatum(int64((i-1)*3 + 3)),
				},
			}
		}
		close(rowsCh)
		ti.SetSelectedRowCh(rowsCh)
		kvWriter := mock.NewMockEngineWriter(ctrl)
		kvWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		progress := importer.NewProgress()
		checksum := verify.NewKVGroupChecksumWithKeyspace(keyspace)
		err := importer.ProcessChunkWithWriter(ctx, chunkInfo, ti, kvWriter, kvWriter, progress, zap.NewExample(), checksum)
		require.NoError(t, err)
		require.Len(t, progress.GetColSize(), 3)
		checksumMap := checksum.GetInnerChecksums()
		require.Len(t, checksumMap, 1)
		require.Equal(t, verify.MakeKVChecksum(111, 3, 14231358899564314836), *checksumMap[verify.DataKVGroupID])
	})
}

func TestPopulateChunks(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tidbCfg := tidb.GetGlobalConfig()
	tidbCfg.TempDir = t.TempDir()

	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int)")
	require.NoError(t, os.WriteFile(path.Join(tidbCfg.TempDir, "test-01.csv"),
		[]byte("1,2,3\n4,5,6\n7,8,9\n"), 0o644))
	require.NoError(t, os.WriteFile(path.Join(tidbCfg.TempDir, "test-02.csv"),
		[]byte("8,8,8\n"), 0o644))
	require.NoError(t, os.WriteFile(path.Join(tidbCfg.TempDir, "test-03.csv"),
		[]byte("9,9,9\n10,10,10\n"), 0o644))
	ti := getTableImporter(ctx, t, store, "t", fmt.Sprintf("%s/test-*.csv", tidbCfg.TempDir), []*plannercore.LoadDataOpt{{Name: "__max_engine_size", Value: expression.NewStrConst("20")}})
	defer ti.Backend().CloseEngineMgr()
	require.NoError(t, ti.InitDataFiles(ctx))
	engines, err := ti.PopulateChunks(ctx)
	require.NoError(t, err)
	require.Len(t, engines, 3)
	require.Len(t, engines[0].Chunks, 2)
	require.Len(t, engines[1].Chunks, 1)
}
