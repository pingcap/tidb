// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"database/sql/driver"
	"sort"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/mockstore"
	tmock "github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type metaMgrSuite struct {
	mockDB      sqlmock.Sqlmock
	mgr         *dbTableMetaMgr
	checksumMgr *testChecksumMgr
}

func newTableRestore(t *testing.T,
	db, table string,
	dbID, tableID int64,
	createTableSQL string, kvStore kv.Storage,
) *TableRestore {
	p := parser.New()
	se := tmock.NewContext()

	node, err := p.ParseOneStmt(createTableSQL, "utf8mb4", "utf8mb4_bin")
	require.NoError(t, err)
	tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), tableID)
	require.NoError(t, err)
	tableInfo.State = model.StatePublic

	ti := &checkpoints.TidbTableInfo{
		ID:   tableInfo.ID,
		DB:   db,
		Name: table,
		Core: tableInfo,
	}
	dbInfo := &checkpoints.TidbDBInfo{
		ID:   dbID,
		Name: db,
		Tables: map[string]*checkpoints.TidbTableInfo{
			table: ti,
		},
	}

	ctx := kv.WithInternalSourceType(context.Background(), "test")
	err = kv.RunInNewTxn(ctx, kvStore, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		if err := m.CreateDatabase(&model.DBInfo{ID: dbInfo.ID}); err != nil && !errors.ErrorEqual(err, meta.ErrDBExists) {
			return err
		}
		return m.CreateTableOrView(dbInfo.ID, ti.Core)
	})
	require.NoError(t, err)

	tableName := common.UniqueTable(db, table)
	logger := log.With(zap.String("table", tableName))

	return &TableRestore{
		dbInfo:    dbInfo,
		tableName: tableName,
		tableInfo: ti,
		kvStore:   kvStore,
		logger:    logger,
	}
}

func newMetaMgrSuite(t *testing.T) *metaMgrSuite {
	db, m, err := sqlmock.New()
	require.NoError(t, err)

	storePath := t.TempDir()
	kvStore, err := mockstore.NewMockStore(mockstore.WithPath(storePath))
	require.NoError(t, err)

	var s metaMgrSuite
	s.mgr = &dbTableMetaMgr{
		session: db,
		taskID:  1,
		tr: newTableRestore(t, "test", "t1", 1, 1,
			"CREATE TABLE `t1` (`c1` varchar(5) NOT NULL)", kvStore),
		tableName:    common.UniqueTable("test", TableMetaTableName),
		needChecksum: true,
	}
	s.mockDB = m
	s.checksumMgr = &testChecksumMgr{}

	t.Cleanup(func() {
		require.NoError(t, s.mockDB.ExpectationsWereMet())
		require.NoError(t, kvStore.Close())
	})
	return &s
}

func TestAllocTableRowIDsSingleTable(t *testing.T) {
	s := newMetaMgrSuite(t)

	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(1)
	updateArgs := []driver.Value{int64(0), int64(10), "restore", int64(1), int64(1)}
	s.prepareMock(rows, &nextID, updateArgs, nil, nil, false)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(0), rowIDBase)
	require.Nil(t, ck)

	require.Equal(t, 0, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsSingleTableAutoIDNot0(t *testing.T) {
	s := newMetaMgrSuite(t)
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(999)
	updateArgs := []driver.Value{int64(998), int64(1008), "allocated", int64(1), int64(1)}
	newStatus := "restore"
	s.prepareMock(rows, &nextID, updateArgs, nil, &newStatus, false)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(998), rowIDBase)
	require.Nil(t, ck)

	require.Equal(t, 1, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsSingleTableContainsData(t *testing.T) {
	s := newMetaMgrSuite(t)

	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(999)
	checksum := verification.MakeKVChecksum(1, 2, 3)
	updateArgs := []driver.Value{int64(998), int64(1008), "allocated", int64(1), int64(1)}
	s.prepareMock(rows, &nextID, updateArgs, &checksum, nil, false)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(998), rowIDBase)
	require.Equal(t, &checksum, ck)
	require.Equal(t, 1, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsSingleTableSkipChecksum(t *testing.T) {
	s := newMetaMgrSuite(t)

	s.mgr.needChecksum = false
	defer func() {
		s.mgr.needChecksum = true
	}()
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(999)
	newStatus := "restore"
	updateArgs := []driver.Value{int64(998), int64(1008), "allocated", int64(1), int64(1)}
	s.prepareMock(rows, &nextID, updateArgs, nil, &newStatus, false)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(998), rowIDBase)
	require.Nil(t, ck)

	require.Equal(t, 0, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsAllocated(t *testing.T) {
	s := newMetaMgrSuite(t)

	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(998), int64(1008), uint64(0), uint64(0), uint64(0), metaStatusRowIDAllocated.String()},
	}
	checksum := verification.MakeKVChecksum(2, 1, 3)
	s.prepareMock(rows, nil, nil, &checksum, nil, false)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(998), rowIDBase)
	require.Equal(t, &checksum, ck)
	require.Equal(t, 1, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsFinished(t *testing.T) {
	s := newMetaMgrSuite(t)

	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(998), int64(1008), uint64(1), uint64(2), uint64(3), metaStatusRestoreStarted.String()},
	}
	checksum := verification.MakeKVChecksum(2, 1, 3)
	s.prepareMock(rows, nil, nil, nil, nil, false)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(998), rowIDBase)
	require.Equal(t, &checksum, ck)
	require.Equal(t, 0, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsMultiTasksInit(t *testing.T) {
	s := newMetaMgrSuite(t)
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
		{int64(2), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(1)
	updateArgs := []driver.Value{int64(0), int64(10), "restore", int64(1), int64(1)}
	s.prepareMock(rows, &nextID, updateArgs, nil, nil, false)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(0), rowIDBase)
	require.Nil(t, ck)

	require.Equal(t, 0, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsMultiTasksAllocated(t *testing.T) {
	s := newMetaMgrSuite(t)
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), metaStatusInitial.String()},
		{int64(2), int64(0), int64(100), uint64(0), uint64(0), uint64(0), metaStatusRowIDAllocated.String()},
	}
	updateArgs := []driver.Value{int64(100), int64(110), "restore", int64(1), int64(1)}
	s.prepareMock(rows, nil, updateArgs, nil, nil, false)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(100), rowIDBase)
	require.Nil(t, ck)

	require.Equal(t, 0, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsRetryOnTableInChecksum(t *testing.T) {
	s := newMetaMgrSuite(t)

	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)
	s.mockDB.ExpectExec("SET SESSION tidb_txn_mode = 'pessimistic';").
		WillReturnResult(sqlmock.NewResult(int64(0), int64(0)))
	s.mockDB.ExpectBegin()
	s.mockDB.ExpectQuery("\\QSELECT task_id, row_id_base, row_id_max, total_kvs_base, total_bytes_base, checksum_base, status from `test`.`table_meta` WHERE table_id = ? FOR UPDATE\\E").
		WithArgs(int64(1)).
		WillReturnError(errors.New("mock err"))
	s.mockDB.ExpectRollback()
	// should not retry
	_, _, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock err")

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), metaStatusChecksuming.String()},
	}
	s.prepareMock(rows, nil, nil, nil, nil, true)
	rows = [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), metaStatusInitial.String()},
		{int64(2), int64(0), int64(100), uint64(0), uint64(0), uint64(0), metaStatusRowIDAllocated.String()},
	}
	updateArgs := []driver.Value{int64(100), int64(110), "restore", int64(1), int64(1)}
	s.prepareMockInner(rows, nil, updateArgs, nil, nil, false)

	// fail, retry and success
	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(100), rowIDBase)
	require.Nil(t, ck)

	require.Equal(t, 0, s.checksumMgr.callCnt)
}

func (s *metaMgrSuite) prepareMock(rowsVal [][]driver.Value, nextRowID *int64, updateArgs []driver.Value, checksum *verification.KVChecksum, updateStatus *string, rollback bool) {
	s.mockDB.ExpectExec("SET SESSION tidb_txn_mode = 'pessimistic';").
		WillReturnResult(sqlmock.NewResult(int64(0), int64(0)))
	s.prepareMockInner(rowsVal, nextRowID, updateArgs, checksum, updateStatus, rollback)
}
func (s *metaMgrSuite) prepareMockInner(rowsVal [][]driver.Value, nextRowID *int64, updateArgs []driver.Value, checksum *verification.KVChecksum, updateStatus *string, rollback bool) {
	s.mockDB.ExpectBegin()

	rows := sqlmock.NewRows([]string{"task_id", "row_id_base", "row_id_max", "total_kvs_base", "total_bytes_base", "checksum_base", "status"})
	for _, r := range rowsVal {
		rows = rows.AddRow(r...)
	}
	s.mockDB.ExpectQuery("\\QSELECT task_id, row_id_base, row_id_max, total_kvs_base, total_bytes_base, checksum_base, status from `test`.`table_meta` WHERE table_id = ? FOR UPDATE\\E").
		WithArgs(int64(1)).
		WillReturnRows(rows)

	if nextRowID != nil {
		allocs := autoid.NewAllocatorsFromTblInfo(s.mgr.tr, s.mgr.tr.dbInfo.ID, s.mgr.tr.tableInfo.Core)
		alloc := allocs.Get(autoid.RowIDAllocType)
		alloc.ForceRebase(*nextRowID - 1)
	}

	if len(updateArgs) > 0 {
		s.mockDB.ExpectExec("\\Qupdate `test`.`table_meta` set row_id_base = ?, row_id_max = ?, status = ? where table_id = ? and task_id = ?\\E").
			WithArgs(updateArgs...).
			WillReturnResult(sqlmock.NewResult(int64(0), int64(1)))
	}

	if rollback {
		s.mockDB.ExpectRollback()
		return
	}

	s.mockDB.ExpectCommit()

	if checksum != nil {
		s.mockDB.ExpectExec("\\Qupdate `test`.`table_meta` set total_kvs_base = ?, total_bytes_base = ?, checksum_base = ?, status = ? where table_id = ? and task_id = ?\\E").
			WithArgs(checksum.SumKVS(), checksum.SumSize(), checksum.Sum(), metaStatusRestoreStarted.String(), int64(1), int64(1)).
			WillReturnResult(sqlmock.NewResult(int64(0), int64(1)))
		s.checksumMgr.checksum = RemoteChecksum{
			TotalBytes: checksum.SumSize(),
			TotalKVs:   checksum.SumKVS(),
			Checksum:   checksum.Sum(),
		}
	}

	if updateStatus != nil {
		s.mockDB.ExpectExec("\\Qupdate `test`.`table_meta` set status = ? where table_id = ? and task_id = ?\\E").
			WithArgs(*updateStatus, int64(1), int64(1)).
			WillReturnResult(sqlmock.NewResult(int64(0), int64(1)))
	}
}

type taskMetaMgrSuite struct {
	mgr    *dbTaskMetaMgr
	mockDB sqlmock.Sqlmock
}

func newTaskMetaMgrSuite(t *testing.T) *taskMetaMgrSuite {
	db, m, err := sqlmock.New()
	require.NoError(t, err)

	var s taskMetaMgrSuite
	s.mgr = &dbTaskMetaMgr{
		session:   db,
		taskID:    1,
		tableName: common.UniqueTable("test", "t1"),
	}
	s.mockDB = m
	return &s
}

func TestCheckTasksExclusively(t *testing.T) {
	s := newTaskMetaMgrSuite(t)
	s.mockDB.ExpectExec("SET SESSION tidb_txn_mode = 'pessimistic';").
		WillReturnResult(sqlmock.NewResult(int64(0), int64(0)))
	s.mockDB.ExpectBegin()
	s.mockDB.ExpectQuery("SELECT task_id, pd_cfgs, status, state, source_bytes, cluster_avail from `test`.`t1` FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "pd_cfgs", "status", "state", "source_bytes", "cluster_avail"}).
			AddRow("0", "", taskMetaStatusInitial.String(), "0", "0", "0").
			AddRow("1", "", taskMetaStatusInitial.String(), "0", "0", "0").
			AddRow("2", "", taskMetaStatusInitial.String(), "0", "0", "0").
			AddRow("3", "", taskMetaStatusInitial.String(), "0", "0", "0").
			AddRow("4", "", taskMetaStatusInitial.String(), "0", "0", "0"))

	s.mockDB.ExpectExec("\\QREPLACE INTO `test`.`t1` (task_id, pd_cfgs, status, state, source_bytes, cluster_avail) VALUES(?, ?, ?, ?, ?, ?)\\E").
		WithArgs(int64(2), "", taskMetaStatusInitial.String(), int(0), uint64(2048), uint64(0)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	s.mockDB.ExpectExec("\\QREPLACE INTO `test`.`t1` (task_id, pd_cfgs, status, state, source_bytes, cluster_avail) VALUES(?, ?, ?, ?, ?, ?)\\E").
		WithArgs(int64(3), "", taskMetaStatusInitial.String(), int(0), uint64(3072), uint64(0)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	s.mockDB.ExpectCommit()

	err := s.mgr.CheckTasksExclusively(context.Background(), func(tasks []taskMeta) ([]taskMeta, error) {
		require.Equal(t, 5, len(tasks))
		sort.Slice(tasks, func(i, j int) bool {
			return tasks[i].taskID < tasks[j].taskID
		})
		for j := 0; j < 5; j++ {
			require.Equal(t, int64(j), tasks[j].taskID)
		}

		var newTasks []taskMeta
		for j := 2; j < 4; j++ {
			task := tasks[j]
			task.sourceBytes = uint64(j * 1024)
			newTasks = append(newTasks, task)
		}
		return newTasks, nil
	})
	require.NoError(t, err)
}

type testChecksumMgr struct {
	checksum RemoteChecksum
	callCnt  int
}

func (t *testChecksumMgr) Checksum(ctx context.Context, tableInfo *checkpoints.TidbTableInfo) (*RemoteChecksum, error) {
	t.callCnt++
	return &t.checksum, nil
}

func TestSingleTaskMetaMgr(t *testing.T) {
	metaBuilder := singleMgrBuilder{
		taskID: time.Now().UnixNano(),
	}
	metaMgr := metaBuilder.TaskMetaMgr(nil)

	ok, err := metaMgr.CheckTaskExist(context.Background())
	require.NoError(t, err)
	require.False(t, ok)

	err = metaMgr.InitTask(context.Background(), 1<<30)
	require.NoError(t, err)

	ok, err = metaMgr.CheckTaskExist(context.Background())
	require.NoError(t, err)
	require.True(t, ok)

	err = metaMgr.CheckTasksExclusively(context.Background(), func(tasks []taskMeta) ([]taskMeta, error) {
		require.Len(t, tasks, 1)
		require.Equal(t, uint64(1<<30), tasks[0].sourceBytes)
		return nil, nil
	})
	require.NoError(t, err)
}

func newTableInfo2(t *testing.T,
	dbID, tableID int64,
	createTableSql string, kvStore kv.Storage,
) *model.TableInfo {
	p := parser.New()
	se := tmock.NewContext()

	node, err := p.ParseOneStmt(createTableSql, "utf8mb4", "utf8mb4_bin")
	require.NoError(t, err)
	tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), tableID)
	require.NoError(t, err)
	tableInfo.State = model.StatePublic

	ctx := kv.WithInternalSourceType(context.Background(), "test")
	err = kv.RunInNewTxn(ctx, kvStore, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		if err := m.CreateDatabase(&model.DBInfo{ID: dbID}); err != nil && !errors.ErrorEqual(err, meta.ErrDBExists) {
			return err
		}
		return m.CreateTableOrView(dbID, tableInfo)
	})
	require.NoError(t, err)
	return tableInfo
}

type mockRequirement struct {
	kv.Storage
}

func (r mockRequirement) Store() kv.Storage {
	return r.Storage
}

func (r mockRequirement) AutoIDClient() *autoid.ClientDiscover {
	return nil
}

func TestAllocGlobalAutoID(t *testing.T) {
	storePath := t.TempDir()
	kvStore, err := mockstore.NewMockStore(mockstore.WithPath(storePath))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, kvStore.Close())
	})

	cases := []struct {
		tableID              int64
		createTableSQL       string
		expectErrStr         string
		expectAllocatorTypes []autoid.AllocatorType
	}{
		// autoID, autoIncrID = false, false
		{
			tableID:              11,
			createTableSQL:       "create table t11 (a int primary key clustered)",
			expectErrStr:         "has no auto ID",
			expectAllocatorTypes: nil,
		},
		{
			tableID:              12,
			createTableSQL:       "create table t12 (a int primary key clustered) AUTO_ID_CACHE 1",
			expectErrStr:         "has no auto ID",
			expectAllocatorTypes: nil,
		},
		// autoID, autoIncrID = true, false
		{
			tableID:              21,
			createTableSQL:       "create table t21 (a int)",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.RowIDAllocType},
		},
		{
			tableID:              22,
			createTableSQL:       "create table t22 (a int) AUTO_ID_CACHE 1",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.RowIDAllocType},
		},
		// autoID, autoIncrID = false, true
		{
			tableID:              31,
			createTableSQL:       "create table t31 (a int primary key clustered auto_increment)",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.RowIDAllocType},
		},
		{
			tableID:              32,
			createTableSQL:       "create table t32 (a int primary key clustered auto_increment) AUTO_ID_CACHE 1",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.AutoIncrementType, autoid.RowIDAllocType},
		},
		// autoID, autoIncrID = true, true
		{
			tableID:              41,
			createTableSQL:       "create table t41 (a int primary key nonclustered auto_increment)",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.RowIDAllocType},
		},
		{
			tableID:              42,
			createTableSQL:       "create table t42 (a int primary key nonclustered auto_increment) AUTO_ID_CACHE 1",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.AutoIncrementType, autoid.RowIDAllocType},
		},
		// autoRandomID
		{
			tableID:              51,
			createTableSQL:       "create table t51 (a bigint primary key auto_random)",
			expectErrStr:         "",
			expectAllocatorTypes: []autoid.AllocatorType{autoid.AutoRandomType},
		},
	}
	ctx := context.Background()
	for _, c := range cases {
		ti := newTableInfo2(t, 1, c.tableID, c.createTableSQL, kvStore)
		allocators, err := getGlobalAutoIDAlloc(mockRequirement{kvStore}, 1, ti)
		if c.expectErrStr == "" {
			require.NoError(t, err, c.tableID)
			require.NoError(t, rebaseGlobalAutoID(ctx, 123, mockRequirement{kvStore}, 1, ti))
			base, idMax, err := allocGlobalAutoID(ctx, 100, mockRequirement{kvStore}, 1, ti)
			require.NoError(t, err, c.tableID)
			require.Equal(t, int64(123), base, c.tableID)
			require.Equal(t, int64(223), idMax, c.tableID)
			// all allocators are rebased and allocated
			for _, alloc := range allocators {
				base2, max2, err := alloc.Alloc(ctx, 100, 1, 1)
				require.NoError(t, err, c.tableID)
				require.Equal(t, int64(223), base2, c.tableID)
				require.Equal(t, int64(323), max2, c.tableID)
			}
		} else {
			require.ErrorContains(t, err, c.expectErrStr, c.tableID)
		}
		var allocatorTypes []autoid.AllocatorType
		for _, alloc := range allocators {
			allocatorTypes = append(allocatorTypes, alloc.GetType())
		}
		require.Equal(t, c.expectAllocatorTypes, allocatorTypes, c.tableID)
	}
}
