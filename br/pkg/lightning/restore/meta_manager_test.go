// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"database/sql/driver"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/ddl"
	tmock "github.com/pingcap/tidb/util/mock"
	"go.uber.org/zap"
)

var _ = Suite(&metaMgrSuite{})

type metaMgrSuite struct {
	mockDB      sqlmock.Sqlmock
	tr          *TableRestore
	mgr         *dbTableMetaMgr
	checksumMgr *testChecksumMgr
}

func (s *metaMgrSuite) SetUpSuite(c *C) {
	p := parser.New()
	se := tmock.NewContext()

	node, err := p.ParseOneStmt("CREATE TABLE `t1` (`c1` varchar(5) NOT NULL)", "utf8mb4", "utf8mb4_bin")
	c.Assert(err, IsNil)
	tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), int64(1))
	c.Assert(err, IsNil)
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
	s.tr = &TableRestore{
		tableName: tableName,
		tableInfo: ti,
		logger:    logger,
	}
}

func (s *metaMgrSuite) SetUpTest(c *C) {
	db, m, err := sqlmock.New()
	c.Assert(err, IsNil)

	s.mgr = &dbTableMetaMgr{
		session:      db,
		taskID:       1,
		tr:           s.tr,
		tableName:    common.UniqueTable("test", tableMetaTableName),
		needChecksum: true,
	}
	s.mockDB = m
	s.checksumMgr = &testChecksumMgr{}
}

func (s *metaMgrSuite) TearDownTest(c *C) {
	c.Assert(s.mockDB.ExpectationsWereMet(), IsNil)
}

func (s *metaMgrSuite) TestAllocTableRowIDsSingleTable(c *C) {
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(1)
	updateArgs := []driver.Value{int64(0), int64(10), "restore", int64(1), int64(1)}
	s.prepareMock(rows, &nextID, updateArgs, nil, nil)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	c.Assert(err, IsNil)
	c.Assert(rowIDBase, Equals, int64(0))
	c.Assert(ck, IsNil)
	c.Assert(s.checksumMgr.callCnt, Equals, 0)
}

func (s *metaMgrSuite) TestAllocTableRowIDsSingleTableAutoIDNot0(c *C) {
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(999)
	updateArgs := []driver.Value{int64(998), int64(1008), "allocated", int64(1), int64(1)}
	newStatus := "restore"
	s.prepareMock(rows, &nextID, updateArgs, nil, &newStatus)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	c.Assert(err, IsNil)
	c.Assert(rowIDBase, Equals, int64(998))
	c.Assert(ck, IsNil)
	c.Assert(s.checksumMgr.callCnt, Equals, 1)
}

func (s *metaMgrSuite) TestAllocTableRowIDsSingleTableContainsData(c *C) {
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(999)
	checksum := verification.MakeKVChecksum(1, 2, 3)
	updateArgs := []driver.Value{int64(998), int64(1008), "allocated", int64(1), int64(1)}
	s.prepareMock(rows, &nextID, updateArgs, &checksum, nil)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	c.Assert(err, IsNil)
	c.Assert(rowIDBase, Equals, int64(998))
	c.Assert(ck, DeepEquals, &checksum)
	c.Assert(s.checksumMgr.callCnt, Equals, 1)
}

func (s *metaMgrSuite) TestAllocTableRowIDsSingleTableSkipChecksum(c *C) {
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
	c.Assert(err, IsNil)
	c.Assert(rowIDBase, Equals, int64(998))
	c.Assert(ck, IsNil)
	c.Assert(s.checksumMgr.callCnt, Equals, 0)
}

func (s *metaMgrSuite) TestAllocTableRowIDsAllocated(c *C) {
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(998), int64(1008), uint64(0), uint64(0), uint64(0), metaStatusRowIDAllocated.String()},
	}
	checksum := verification.MakeKVChecksum(2, 1, 3)
	s.prepareMock(rows, nil, nil, &checksum, nil)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	c.Assert(err, IsNil)
	c.Assert(rowIDBase, Equals, int64(998))
	c.Assert(ck, DeepEquals, &checksum)
	c.Assert(s.checksumMgr.callCnt, Equals, 1)
}

func (s *metaMgrSuite) TestAllocTableRowIDsFinished(c *C) {
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(998), int64(1008), uint64(1), uint64(2), uint64(3), metaStatusRestoreStarted.String()},
	}
	checksum := verification.MakeKVChecksum(2, 1, 3)
	s.prepareMock(rows, nil, nil, nil, nil)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	c.Assert(err, IsNil)
	c.Assert(rowIDBase, Equals, int64(998))
	c.Assert(ck, DeepEquals, &checksum)
	c.Assert(s.checksumMgr.callCnt, Equals, 0)
}

func (s *metaMgrSuite) TestAllocTableRowIDsMultiTasksInit(c *C) {
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
		{int64(2), int64(0), int64(0), uint64(0), uint64(0), uint64(0), "initialized"},
	}
	nextID := int64(1)
	updateArgs := []driver.Value{int64(0), int64(10), "restore", int64(1), int64(1)}
	s.prepareMock(rows, &nextID, updateArgs, nil, nil)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	c.Assert(err, IsNil)
	c.Assert(rowIDBase, Equals, int64(0))
	c.Assert(ck, IsNil)
	c.Assert(s.checksumMgr.callCnt, Equals, 0)
}

func (s *metaMgrSuite) TestAllocTableRowIDsMultiTasksAllocated(c *C) {
	ctx := context.WithValue(context.Background(), &checksumManagerKey, s.checksumMgr)

	rows := [][]driver.Value{
		{int64(1), int64(0), int64(0), uint64(0), uint64(0), uint64(0), metaStatusInitial.String()},
		{int64(2), int64(0), int64(100), uint64(0), uint64(0), uint64(0), metaStatusRowIDAllocated.String()},
	}
	updateArgs := []driver.Value{int64(100), int64(110), "restore", int64(1), int64(1)}
	s.prepareMock(rows, nil, updateArgs, nil, nil)

	ck, rowIDBase, err := s.mgr.AllocTableRowIDs(ctx, 10)
	c.Assert(err, IsNil)
	c.Assert(rowIDBase, Equals, int64(100))
	c.Assert(ck, IsNil)
	c.Assert(s.checksumMgr.callCnt, Equals, 0)
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
