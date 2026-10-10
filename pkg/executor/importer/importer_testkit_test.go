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
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/docker/go-units"
	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/mock"
	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/testutil"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/ingestor/ingestctrl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	backendkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/importdef"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
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
			Name: ast.NewCIStr("tb"),
		},
		Checksum:               config.OpLevelRequired,
		DistSQLScanConcurrency: 50,
	}
	tk.MustExec("create database db")
	tk.MustExec("create table db.tb(id int)")
	tk.MustExec("insert into db.tb values(1)")

	getRemoteChecksumFn := func() (*ingestctrl.RemoteChecksum, error) {
		return importer.RemoteChecksumTableBySQL(ctx, tk.Session(), plan, logutil.BgLogger())
	}

	// admin checksum table always return 1, 1, 1 for memory store
	// Checksum = required
	backupDistScanCon := tk.Session().GetSessionVars().DistSQLScanConcurrency()
	require.Equal(t, vardef.DefDistSQLScanConcurrency, backupDistScanCon)
	localChecksum := verify.MakeKVChecksum(1, 1, 1)
	err := importer.VerifyChecksum(ctx, plan, localChecksum, logutil.BgLogger(), getRemoteChecksumFn)
	require.NoError(t, err)
	require.Equal(t, backupDistScanCon, tk.Session().GetSessionVars().DistSQLScanConcurrency())
	localChecksum = verify.MakeKVChecksum(1, 2, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, logutil.BgLogger(), getRemoteChecksumFn)
	require.ErrorIs(t, err, common.ErrChecksumMismatch)

	// check a slow checksum can be canceled
	plan2 := &importer.Plan{
		DBName: "db",
		TableInfo: &model.TableInfo{
			Name: ast.NewCIStr("tb2"),
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
	backup, err := tk.Session().GetSessionVars().GetSessionOrGlobalSystemVar(ctx, vardef.TiDBChecksumTableConcurrency)
	require.NoError(t, err)
	err = tk.Session().GetSessionVars().SetSystemVar(vardef.TiDBChecksumTableConcurrency, "1")
	require.NoError(t, err)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/afterHandleChecksumRequest", `sleep(1000)`))

	ctx2, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err = importer.VerifyChecksum(ctx2, plan2, localChecksum, logutil.BgLogger(), func() (*ingestctrl.RemoteChecksum, error) {
		return importer.RemoteChecksumTableBySQL(ctx2, tk.Session(), plan2, logutil.BgLogger())
	})
	require.ErrorContains(t, err, "Query execution was interrupted")

	err = tk.Session().GetSessionVars().SetSystemVar(vardef.TiDBChecksumTableConcurrency, backup)
	require.NoError(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/afterHandleChecksumRequest"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum", `3*return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum"))
	}()
	err = importer.VerifyChecksum(ctx, plan, localChecksum, logutil.BgLogger(), getRemoteChecksumFn)
	require.ErrorContains(t, err, "occur an error when checksum")
	// remote checksum success after retry
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum", `1*return(true)`))
	localChecksum = verify.MakeKVChecksum(1, 1, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, logutil.BgLogger(), getRemoteChecksumFn)
	require.NoError(t, err)

	// checksum = optional
	plan.Checksum = config.OpLevelOptional
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum"))
	localChecksum = verify.MakeKVChecksum(1, 1, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, logutil.BgLogger(), getRemoteChecksumFn)
	require.NoError(t, err)
	localChecksum = verify.MakeKVChecksum(1, 2, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, logutil.BgLogger(), getRemoteChecksumFn)
	require.NoError(t, err)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum", `3*return(true)`))
	err = importer.VerifyChecksum(ctx, plan, localChecksum, logutil.BgLogger(), getRemoteChecksumFn)
	require.NoError(t, err)

	// checksum = off
	plan.Checksum = config.OpLevelOff
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/importer/errWhenChecksum"))
	localChecksum = verify.MakeKVChecksum(1, 2, 1)
	err = importer.VerifyChecksum(ctx, plan, localChecksum, logutil.BgLogger(), getRemoteChecksumFn)
	require.NoError(t, err)
}

type testKitStoreHelper struct {
	store kv.Storage
}

func (h testKitStoreHelper) GetTS(context.Context) (physical, logical int64, err error) {
	return 12345, 67890, nil
}

func (h testKitStoreHelper) GetTiKVCodec() tikv.Codec {
	return h.store.GetCodec()
}

func TestLocalBackendSortedWriterWithTestKitTable(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Without a primary key, the table uses the hidden, non-clustered row ID.
	tk.MustExec("create table t (v int)")

	tableInfo, err := tk.Session().GetInfoSchema().TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	lightningConfig := config.NewConfig()
	lightningConfig.TikvImporter.Backend = config.BackendLocal
	lightningConfig.TikvImporter.SortedKVDir = filepath.Join(t.TempDir(), "sorted-kv")
	lightningConfig.TikvImporter.RangeConcurrency = 1
	lightningConfig.TikvImporter.EngineMemCacheSize = config.DefaultEngineMemCacheSize
	lightningConfig.TikvImporter.LocalWriterMemCacheSize = config.DefaultLocalWriterMemCacheSize
	lightningConfig.Conflict.Strategy = config.ReplaceOnDup
	backendConfig := ingestctrl.NewBackendConfig(lightningConfig, 1000, "", "", "", 0)
	localBackend, err := ingestctrl.NewBackendForTest(ctx, backendConfig, testKitStoreHelper{store: store})
	require.NoError(t, err)
	backendClosed := false
	t.Cleanup(func() {
		if !backendClosed {
			localBackend.Close()
		}
	})

	engineConfig := &backend.EngineConfig{
		TableInfo: &importdef.TableInfo{
			ID:   tableInfo.ID,
			DB:   "test",
			Name: "t",
			Core: tableInfo,
		},
	}
	engineManager := backend.MakeEngineManager(localBackend)
	openedEngine, err := engineManager.OpenEngine(ctx, engineConfig, "`test`.`t`", 1)
	require.NoError(t, err)

	writerConfig := &backend.LocalWriterConfig{}
	writerConfig.Local.IsKVSorted = true
	writer, err := openedEngine.LocalWriter(ctx, writerConfig)
	require.NoError(t, err)

	makeRow := func(id int64) []common.KvPair {
		return []common.KvPair{{
			Key:   tablecodec.EncodeRowKeyWithHandle(tableInfo.ID, kv.IntHandle(id)),
			Val:   make([]byte, 96*1024),
			RowID: common.EncodeIntRowID(id),
		}}
	}

	// A large row can make the importer deliver one KV per batch. Keep the
	// encoded keys the same length so the next batch overwrites the previous
	// last key at the same offset in Writer.sortedKeyBuf.
	for id := int64(1); id <= 3; id++ {
		require.NoError(t, writer.AppendRows(ctx, nil, backendkv.MakeRowsFromKvPairs(makeRow(id))))
	}
	flushStatus, err := writer.Close(ctx)
	require.NoError(t, err)
	require.Eventually(t, flushStatus.Flushed, time.Second, time.Millisecond)

	_, err = openedEngine.Close(ctx)
	require.NoError(t, err)
	localBackend.Close()
	backendClosed = true

	// Close the engine before opening its Pebble database read-only. The engine
	// metadata key is outside the normal KV key range, so the iterator count is
	// the number of data KVs written by the local backend.
	dbPath := filepath.Join(backendConfig.LocalStoreDir, openedEngine.GetEngineUUID().String())
	db, err := pebble.Open(dbPath, &pebble.Options{ReadOnly: true})
	require.NoError(t, err)
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte{1}})
	require.NoError(t, err)
	kvCount := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		kvCount++
	}
	require.NoError(t, iter.Error())
	require.NoError(t, iter.Close())
	require.NoError(t, db.Close())
	require.Equal(t, 3, kvCount)
}

func TestGetTargetNodeCpuCnt(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("DXF is always enabled in nextgen")
	}
	store, tm, ctx := testutil.InitTableTest(t)
	tk := testkit.NewTestKit(t, store)
	originNodeResource := storage.GetNodeResource()
	t.Cleanup(func() {
		storage.SetNodeResource(originNodeResource)
	})

	tk.MustExec("set @@global.tidb_enable_dist_task = off;")
	storage.SetNodeResource(proto.NewNodeResource(16, 16*units.GiB, 100*units.GiB))
	require.NoError(t, tm.InitMeta(ctx, "tidb1", ""))

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)")
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
	tk.MustExec("set @@global.tidb_enable_dist_task = on;")

	targetNodeCPUCnt, err = importer.GetTargetNodeCPUCnt(ctx, importer.DataSourceTypeFile, "s3://path/to/xxx.csv")
	require.NoError(t, err)
	require.Equal(t, 16, targetNodeCPUCnt)
}

func TestPostProcess(t *testing.T) {
	ctx := context.Background()
	integration.BeforeTestExternal(t)
	testEtcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() {
		testEtcdCluster.Terminate(t)
	})
	store := testkit.CreateMockStore(t, mockstore.WithPDAddr([]string{
		testEtcdCluster.Members[0].ClientURLs[0].String(),
	}))
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
	dbInfo, ok := do.InfoSchema().SchemaByName(ast.NewCIStr("db"))
	require.True(t, ok)
	table, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("db"), ast.NewCIStr("tb"))
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
	table, err = do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("db"), ast.NewCIStr("tb2"))
	require.NoError(t, err)
	plan.TableInfo, plan.DesiredTableInfo = table.Meta(), table.Meta()
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

func getTableImporter(ctx context.Context, t *testing.T, store kv.Storage, tableName, path, format string, opts []*plannercore.LoadDataOpt) *importer.TableImporter {
	tk := testkit.NewTestKit(t, store)
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	dbInfo, ok := do.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	table, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tableName))
	require.NoError(t, err)
	var selectPlan base.PhysicalPlan
	if path == "" {
		selectPlan = &physicalop.PhysicalSelection{}
	}
	plan, err := importer.NewImportPlan(ctx, tk.Session(), &plannercore.ImportInto{
		Path:   path,
		Format: &format,
		Table: &resolve.TableNameW{
			TableName: &ast.TableName{Name: table.Meta().Name},
			DBInfo:    dbInfo,
		},
		Options:    opts,
		SelectPlan: selectPlan,
	}, table)
	require.NoError(t, err)
	controller, err := importer.NewLoadDataController(plan, table, &importer.ASTArgs{})
	require.NoError(t, err)
	if path != "" {
		require.NoError(t, controller.InitDataStore(ctx))
	}
	ti, err := importer.NewTableImporterForTest(ctx, controller, "11", store)
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
	prefixLenForOneRow := uint64(len(keyspace))
	tk.MustExec("create table t_close(a int, b int, c int, key(b))")
	for _, tc := range []struct {
		name                  string
		dataClose, indexClose bool
		writeFailure          bool
	}{
		{"data close", true, false, false},
		{"index close", false, true, false},
		{"both close", true, true, false},
		{"write and close", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ti := getTableImporter(ctx, t, store, "t_close", fileName, importer.DataFormatCSV, nil)
			defer ti.LoadDataController.Close()
			defer ti.Backend().CloseEngineMgr()
			ctrl := gomock.NewController(t)
			be := mock.NewMockBackend(ctrl)
			be.EXPECT().OpenEngine(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)
			mgr := backend.MakeEngineManager(be)
			dataEngine, err := mgr.OpenEngine(ctx, &backend.EngineConfig{}, "test.t", 1)
			require.NoError(t, err)
			indexEngine, err := mgr.OpenEngine(ctx, &backend.EngineConfig{}, "test.t", 2)
			require.NoError(t, err)
			dataWriter, indexWriter := mock.NewMockEngineWriter(ctrl), mock.NewMockEngineWriter(ctrl)
			gomock.InOrder(
				be.EXPECT().LocalWriter(gomock.Any(), gomock.Any(), gomock.Any()).Return(dataWriter, nil),
				be.EXPECT().LocalWriter(gomock.Any(), gomock.Any(), gomock.Any()).Return(indexWriter, nil),
			)
			var dataErr, indexErr, writeErr error
			if tc.dataClose {
				dataErr = errors.New("injected data close error")
			}
			if tc.indexClose {
				indexErr = errors.New("injected index close error")
			}
			if tc.writeFailure {
				writeErr = errors.New("injected append error")
			}
			dataWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(writeErr).AnyTimes()
			indexWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			dataWriter.EXPECT().Close(gomock.Any()).Return(nil, dataErr)
			indexWriter.EXPECT().Close(gomock.Any()).Return(nil, indexErr)
			chunkInfo := &importer.Chunk{Path: "test.csv", Type: mydump.SourceTypeCSV, EndOffset: int64(len(sourceData)), RowIDMax: 10000}
			err = importer.ProcessChunk(ctx, chunkInfo, ti, dataEngine, indexEngine, zap.NewNop(), verify.NewKVGroupChecksumWithKeyspace(keyspace), nil)
			want := dataErr
			if indexErr != nil {
				want = indexErr
			}
			if writeErr != nil {
				want = writeErr
			}
			require.ErrorIs(t, err, want)
		})
	}
	t.Run("file chunk", func(t *testing.T) {
		chunkInfo := &importer.Chunk{
			Path:      "test.csv",
			Type:      mydump.SourceTypeCSV,
			EndOffset: int64(len(sourceData)),
			RowIDMax:  10000,
		}
		var scanedRows uint64 = 2
		ti := getTableImporter(ctx, t, store, "t", fileName, importer.DataFormatCSV, []*plannercore.LoadDataOpt{
			{Name: "skip_rows", Value: expression.NewInt64Const(1)}})
		defer func() {
			ti.LoadDataController.Close()
			ti.Backend().CloseEngineMgr()
		}()
		kvWriter := mock.NewMockEngineWriter(ctrl)
		kvWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		checksum := verify.NewKVGroupChecksumWithKeyspace(keyspace)
		err := importer.ProcessChunkWithWriter(ctx, chunkInfo, ti, kvWriter, kvWriter, zap.NewExample(), checksum, nil)
		require.NoError(t, err)
		checksumMap := checksum.GetInnerChecksums()
		require.Len(t, checksumMap, 1)
		require.Equal(t, verify.MakeKVChecksumWithKeyspace(keyspace, 74+scanedRows*prefixLenForOneRow, 2, 15625182175392723123),
			*checksumMap[verify.DataKVGroupID])
	})

	t.Run("query chunk", func(t *testing.T) {
		chunkInfo := &importer.Chunk{
			Path:      "test.csv",
			Type:      mydump.SourceTypeCSV,
			EndOffset: int64(len(sourceData)),
			RowIDMax:  10000,
		}
		ti := getTableImporter(ctx, t, store, "t", "", importer.DataFormatCSV, nil)
		defer func() {
			ti.LoadDataController.Close()
			ti.Backend().CloseEngineMgr()
		}()
		chkCh := make(chan importer.QueryChunk, 3)
		fields := make([]*types.FieldType, 0, 3)
		for range 3 {
			fields = append(fields, types.NewFieldType(mysql.TypeLong))
		}
		chk := chunk.New(fields, 2, 2)
		for i := 1; i <= 2; i++ {
			chk.AppendInt64(0, int64((i-1)*3+1))
			chk.AppendInt64(1, int64((i-1)*3+2))
			chk.AppendInt64(2, int64((i-1)*3+3))
		}
		chkCh <- importer.QueryChunk{
			Fields:      fields,
			Chk:         chk,
			RowIDOffset: 0,
		}
		chk = chunk.New(fields, 1, 1)
		for i := 3; i <= 3; i++ {
			chk.AppendInt64(0, int64((i-1)*3+1))
			chk.AppendInt64(1, int64((i-1)*3+2))
			chk.AppendInt64(2, int64((i-1)*3+3))
		}
		chkCh <- importer.QueryChunk{
			Fields:      fields,
			Chk:         chk,
			RowIDOffset: 2,
		}
		close(chkCh)
		ti.SetSelectedChunkCh(chkCh)
		writtenDataKVs := make([]common.KvPair, 0, 3)
		kvWriter := mock.NewMockEngineWriter(ctrl)
		kvWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ []string, rows encode.Rows) error {
				writtenDataKVs = append(writtenDataKVs, backendkv.Rows2KvPairs(rows)...)
				return nil
			},
		).AnyTimes()
		checksum := verify.NewKVGroupChecksumWithKeyspace(keyspace)
		err := importer.ProcessChunkWithWriter(ctx, chunkInfo, ti, kvWriter, kvWriter, zap.NewExample(), checksum, nil)
		require.NoError(t, err)
		checksumMap := checksum.GetInnerChecksums()
		require.Len(t, checksumMap, 1)
		require.Len(t, writtenDataKVs, 3)
		rowIDs := make([]int64, 0, len(writtenDataKVs))
		for _, pair := range writtenDataKVs {
			handle, err := tablecodec.DecodeRowKey(pair.Key)
			require.NoError(t, err)
			rowIDs = append(rowIDs, handle.IntValue())
		}
		require.ElementsMatch(t, []int64{1, 2, 3}, rowIDs)

		expectedDataChecksum := verify.NewKVChecksumWithKeyspace(keyspace)
		expectedDataChecksum.Update(writtenDataKVs)
		actualDataChecksum := checksumMap[verify.DataKVGroupID]
		require.Equal(t, expectedDataChecksum.SumSize(), actualDataChecksum.SumSize())
		require.Equal(t, expectedDataChecksum.SumKVS(), actualDataChecksum.SumKVS())
		require.Equal(t, expectedDataChecksum.Sum(), actualDataChecksum.Sum())
	})
}

func TestPopulateChunks(t *testing.T) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalImportInto)
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
	ti := getTableImporter(ctx, t, store, "t", fmt.Sprintf("%s/test-*.csv", tidbCfg.TempDir), importer.DataFormatCSV, []*plannercore.LoadDataOpt{{Name: "__max_engine_size", Value: expression.NewStrConst("20")}})
	defer func() {
		ti.LoadDataController.Close()
		ti.Backend().CloseEngineMgr()
	}()
	require.NoError(t, ti.InitDataFiles(ctx))
	engines, err := ti.PopulateChunks(ctx)
	require.NoError(t, err)
	require.Len(t, engines, 3)
	require.Len(t, engines[0], 2)
	require.Len(t, engines[1], 1)
	require.Len(t, engines[common.IndexEngineID], 0)
}

func TestCalResourceParams(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("we only cal resource related params in nextgen")
	}
	_, tm, ctx := testutil.InitTableTest(t)
	testutil.MockNodeResource(t, 8)
	require.NoError(t, tm.InitMeta(ctx, "tidb1", handle.GetTargetScope()))
	c := &importer.LoadDataController{TotalRealSize: 200 * units.TiB, Plan: &importer.Plan{TableInfo: &model.TableInfo{}}}
	importer.WithLogger(zap.NewNop())(c)
	require.NoError(t, c.CalResourceParams(ctx, nil))
	require.Equal(t, 8, c.ThreadCnt)
	require.Equal(t, 32, c.MaxNodeCnt)
	require.Equal(t, 256, c.DistSQLScanConcurrency)

	c = &importer.LoadDataController{TotalRealSize: 300 * units.GiB, Plan: &importer.Plan{TableInfo: &model.TableInfo{}}}
	importer.WithLogger(zap.NewNop())(c)
	require.NoError(t, c.CalResourceParams(ctx, nil))
	require.Equal(t, 8, c.ThreadCnt)
	require.Equal(t, 2, c.MaxNodeCnt)
	require.Equal(t, 124, c.DistSQLScanConcurrency)
}
