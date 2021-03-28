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

	database := "test"
	mock.ExpectQuery(fmt.Sprintf("SHOW CREATE DATABASE `%s`", escapeString(database))).
		WillReturnRows(sqlmock.NewRows([]string{"Database", "Create Database"}).
			AddRow("test", "CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))

	tctx, cancel := tcontext.Background().WithCancel()
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
	d.conf.Tables = DatabaseTables{}.AppendTable("test", nil)
	c.Assert(errors.ErrorEqual(d.dumpDatabases(writerCtx, conn, taskChan), context.Canceled), IsTrue)
	c.Assert(errors.ErrorEqual(wg.Wait(), writerErr), IsTrue)
}
