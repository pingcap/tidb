// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"fmt"
	"time"

	tcontext "github.com/pingcap/dumpling/v4/context"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"golang.org/x/sync/errgroup"
)

func (s *testSQLSuite) TestDumpBlock(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	mock.ExpectQuery(fmt.Sprintf("SHOW CREATE DATABASE `%s`", escapeString(database))).
		WillReturnRows(sqlmock.NewRows([]string{"Database", "Create Database"}).
			AddRow("test", "CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))

	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()
	conn, err := db.Conn(tctx)
	c.Assert(err, IsNil)

	d := &Dumper{
		tctx:      tctx,
		conf:      DefaultConfig(),
		cancelCtx: cancel,
	}
	wg, writingCtx := errgroup.WithContext(tctx)
	writerErr := errors.New("writer error")

	wg.Go(func() error {
		return errors.Trace(writerErr)
	})
	wg.Go(func() error {
		time.Sleep(time.Second)
		return context.Canceled
	})
	writerCtx := tctx.WithContext(writingCtx)
	// simulate taskChan is full
	taskChan := make(chan Task, 1)
	taskChan <- &TaskDatabaseMeta{}
	d.conf.Tables = DatabaseTables{}.AppendTable(database, nil)
	c.Assert(errors.ErrorEqual(d.dumpDatabases(writerCtx, conn, taskChan), context.Canceled), IsTrue)
	c.Assert(errors.ErrorEqual(wg.Wait(), writerErr), IsTrue)
}

func (s *testSQLSuite) TestDumpTableMeta(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()
	conn, err := db.Conn(tctx)
	c.Assert(err, IsNil)
	conf := DefaultConfig()
	conf.NoSchemas = true

	for serverType := ServerTypeUnknown; serverType < ServerTypeAll; serverType++ {
		conf.ServerInfo.ServerType = ServerType(serverType)
		hasImplicitRowID := false
		mock.ExpectQuery("SHOW COLUMNS FROM").
			WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
				AddRow("id", "int(11)", "NO", "PRI", nil, ""))
		if serverType == ServerTypeTiDB {
			mock.ExpectExec("SELECT _tidb_rowid from").
				WillReturnResult(sqlmock.NewResult(0, 0))
			hasImplicitRowID = true
		}
		mock.ExpectQuery(fmt.Sprintf("SELECT \\* FROM `%s`.`%s`", database, table)).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
		meta, err := dumpTableMeta(conf, conn, database, &TableInfo{Type: TableTypeBase, Name: table})
		c.Assert(err, IsNil)
		c.Assert(meta.DatabaseName(), Equals, database)
		c.Assert(meta.TableName(), Equals, table)
		c.Assert(meta.SelectedField(), Equals, "*")
		c.Assert(meta.SelectedLen(), Equals, 1)
		c.Assert(meta.ShowCreateTable(), Equals, "")
		c.Assert(meta.HasImplicitRowID(), Equals, hasImplicitRowID)
	}
}
