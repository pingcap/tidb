// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"database/sql/driver"
	"sort"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	tmock "github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type metaMgrSuite struct {
	mockDB      sqlmock.Sqlmock
	mgr         *dbTableMetaMgr
	checksumMgr *testChecksumMgr
}

func newTableRestore(t *testing.T) *TableRestore {
	p := parser.New()
	se := tmock.NewContext()

	node, err := p.ParseOneStmt("CREATE TABLE `t1` (`c1` varchar(5) NOT NULL)", "utf8mb4", "utf8mb4_bin")
	require.NoError(t, err)
	tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), int64(1))
	require.NoError(t, err)
	tableInfo.State = model.StatePublic

	schema := "test"
	tb := "t1"
	ti := &checkpoints.TidbTableInfo{
		ID:   tableInfo.ID,
		DB:   schema,
		Name: tb,
		Core: tableInfo,
	}

	tableName := common.UniqueTable(schema, tb)
	logger := log.With(zap.String("table", tableName))
	return &TableRestore{
		tableName: tableName,
		tableInfo: ti,
		logger:    logger,
	}
}

func newMetaMgrSuite(t *testing.T) (*metaMgrSuite, func()) {
	db, m, err := sqlmock.New()
	require.NoError(t, err)

	var s metaMgrSuite
	s.mgr = &dbTableMetaMgr{
		session:      db,
		taskID:       1,
		tr:           newTableRestore(t),
		tableName:    common.UniqueTable("test", TableMetaTableName),
		needChecksum: true,
	}
	s.mockDB = m
	s.checksumMgr = &testChecksumMgr{}
	return &s, func() {
		require.NoError(t, s.mockDB.ExpectationsWereMet())
	}
}

func TestAllocTableRowIDsSingleTable(t *testing.T) {
	s, clean := newMetaMgrSuite(t)
	defer clean()

	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(1)
	updateArgs := []driver.Value{int64(0), int64(10), "restore", int64(1), int64(1)}
	s.prepareMock(rows, &nextID, updateArgs, nil, nil)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(0), rowIDBase)
	require.Nil(t, ck)

	require.Equal(t, 0, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsSingleTableAutoIDNot0(t *testing.T) {
	s, clean := newMetaMgrSuite(t)
	defer clean()
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(999)
	updateArgs := []driver.Value{int64(998), int64(1008), "allocated", int64(1), int64(1)}
	newStatus := "restore"
	s.prepareMock(rows, &nextID, updateArgs, nil, &newStatus)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(998), rowIDBase)
	require.Nil(t, ck)

	require.Equal(t, 1, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsSingleTableContainsData(t *testing.T) {
	s, clean := newMetaMgrSuite(t)
	defer clean()

	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(999)
	checksum := verification.MakeKVChecksum(1, 2, 3)
	updateArgs := []driver.Value{int64(998), int64(1008), "allocated", int64(1), int64(1)}
	s.prepareMock(rows, &nextID, updateArgs, &checksum, nil)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(998), rowIDBase)
	require.Equal(t, &checksum, ck)
	require.Equal(t, 1, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsSingleTableSkipChecksum(t *testing.T) {
	s, clean := newMetaMgrSuite(t)
	defer clean()

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
	s.prepareMock(rows, &nextID, updateArgs, nil, &newStatus)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(998), rowIDBase)
	require.Nil(t, ck)

	require.Equal(t, 0, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsAllocated(t *testing.T) {
	s, clean := newMetaMgrSuite(t)
	defer clean()

	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(998), int64(1008), uint64(0), uint64(0), uint64(0), metaStatusRowIDAllocated.String()},
	}
	checksum := verification.MakeKVChecksum(2, 1, 3)
	s.prepareMock(rows, nil, nil, &checksum, nil)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(998), rowIDBase)
	require.Equal(t, &checksum, ck)
	require.Equal(t, 1, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsFinished(t *testing.T) {
	s, clean := newMetaMgrSuite(t)
	defer clean()

	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(998), int64(1008), uint64(1), uint64(2), uint64(3), metaStatusRestoreStarted.String()},
	}
	checksum := verification.MakeKVChecksum(2, 1, 3)
	s.prepareMock(rows, nil, nil, nil, nil)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(998), rowIDBase)
	require.Equal(t, &checksum, ck)
	require.Equal(t, 0, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsMultiTasksInit(t *testing.T) {
	s, clean := newMetaMgrSuite(t)
	defer clean()
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
		{int64(2), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(1)
	updateArgs := []driver.Value{int64(0), int64(10), "restore", int64(1), int64(1)}
	s.prepareMock(rows, &nextID, updateArgs, nil, nil)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(0), rowIDBase)
	require.Nil(t, ck)

	require.Equal(t, 0, s.checksumMgr.callCnt)
}

func TestAllocTableRowIDsMultiTasksAllocated(t *testing.T) {
	s, clean := newMetaMgrSuite(t)
	defer clean()
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), metaStatusInitial.String()},
		{int64(2), int64(0), int64(100), uint64(0), uint64(0), uint64(0), metaStatusRowIDAllocated.String()},
	}
	updateArgs := []driver.Value{int64(100), int64(110), "restore", int64(1), int64(1)}
	s.prepareMock(rows, nil, updateArgs, nil, nil)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, int64(100), rowIDBase)
	require.Nil(t, ck)

	require.Equal(t, 0, s.checksumMgr.callCnt)
}

func (s *metaMgrSuite) prepareMock(rowsVal [][]driver.Value, nextRowID *int64, updateArgs []driver.Value, checksum *verification.KVChecksum, updateStatus *string) {
	s.mockDB.ExpectExec("SET SESSION tidb_txn_mode = 'pessimistic';").
		WillReturnResult(sqlmock.NewResult(int64(0), int64(0)))

	s.mockDB.ExpectBegin()

	rows := sqlmock.NewRows([]string{"task_id", "row_id_base", "row_id_max", "total_kvs_base", "total_bytes_base", "checksum_base", "status"})
	for _, r := range rowsVal {
		rows = rows.AddRow(r...)
	}
	s.mockDB.ExpectQuery("\\QSELECT task_id, row_id_base, row_id_max, total_kvs_base, total_bytes_base, checksum_base, status from `test`.`table_meta` WHERE table_id = ? FOR UPDATE\\E").
		WithArgs(int64(1)).
		WillReturnRows(rows)
	if nextRowID != nil {
		s.mockDB.ExpectQuery("SHOW TABLE `test`.`t1` NEXT_ROW_ID").
			WillReturnRows(sqlmock.NewRows([]string{"DB_NAME", "TABLE_NAME", "COLUMN_NAME", "NEXT_GLOBAL_ROW_ID", "ID_TYPE"}).
				AddRow("test", "t1", "_tidb_rowid", *nextRowID, "AUTO_INCREMENT"))
	}

	if len(updateArgs) > 0 {
		s.mockDB.ExpectExec("\\Qupdate `test`.`table_meta` set row_id_base = ?, row_id_max = ?, status = ? where table_id = ? and task_id = ?\\E").
			WithArgs(updateArgs...).
			WillReturnResult(sqlmock.NewResult(int64(0), int64(1)))
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
