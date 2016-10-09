// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testIndexChangeSuite{})

type testIndexChangeSuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo
}

func (s *testIndexChangeSuite) SetUpSuite(c *C) {
	s.store = testCreateStore(c, "test_index_change")
	s.dbInfo = &model.DBInfo{
		Name: model.NewCIStr("test_index_change"),
		ID:   1,
	}
	err := kv.RunInNewTxn(s.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		return errors.Trace(t.CreateDatabase(s.dbInfo))
	})
	c.Check(err, IsNil)
}

func (s *testIndexChangeSuite) TestIndexChange(c *C) {
	defer testleak.AfterTest(c)()
	d := newDDL(s.store, nil, nil, testLease)
	// create table t (c1 int primary key, c2 int);
	tblInfo := testTableInfo(c, d, "t", 2)
	tblInfo.Columns[0].Flag = mysql.PriKeyFlag | mysql.NotNullFlag
	tblInfo.PKIsHandle = true
	ctx := testNewContext(c, d)
	_, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)
	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	originTable := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	// insert t values (1, 1), (2, 2), (3, 3)
	_, err = originTable.AddRecord(ctx, types.MakeDatums(1, 1))
	c.Assert(err, IsNil)
	_, err = originTable.AddRecord(ctx, types.MakeDatums(2, 2))
	c.Assert(err, IsNil)
	_, err = originTable.AddRecord(ctx, types.MakeDatums(3, 3))
	c.Assert(err, IsNil)

	err = ctx.CommitTxn()
	c.Assert(err, IsNil)

	tc := &testDDLCallback{}
	// set up hook
	prevState := model.StateNone
	var (
		deleteOnlyTable table.Table
		writeOnlyTable  table.Table
		publicTable     table.Table
	)
	var checkErr error
	tc.onJobUpdated = func(job *model.Job) {
		if job.SchemaState == prevState {
			return
		}
		prevState = job.SchemaState
		var err error
		switch job.SchemaState {
		case model.StateDeleteOnly:
			deleteOnlyTable, err = getCurrentTable(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case model.StateWriteOnly:
			writeOnlyTable, err = getCurrentTable(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			err = s.checkAddWriteOnly(d, ctx, deleteOnlyTable, writeOnlyTable)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case model.StatePublic:
			publicTable, err = getCurrentTable(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			err = s.checkAddPublic(d, ctx, writeOnlyTable, publicTable)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		}
	}
	d.setHook(tc)
	testCreateIndex(c, ctx, d, s.dbInfo, originTable.Meta(), false, "c2", "c2")
	c.Check(errors.ErrorStack(checkErr), Equals, "")

	d.Stop()
	prevState = model.StatePublic
	var noneTable table.Table
	tc.onJobUpdated = func(job *model.Job) {
		if job.SchemaState == prevState {
			return
		}
		prevState = job.SchemaState
		var err error
		switch job.SchemaState {
		case model.StateWriteOnly:
			writeOnlyTable, err = getCurrentTable(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			err = s.checkDropWriteOnly(d, ctx, publicTable, writeOnlyTable)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case model.StateDeleteOnly:
			deleteOnlyTable, err = getCurrentTable(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			err = s.checkDropDeleteOnly(d, ctx, writeOnlyTable, deleteOnlyTable)
			if err != nil {
				checkErr = errors.Trace(err)
			}
		case model.StateNone:
			noneTable, err = getCurrentTable(d, s.dbInfo.ID, tblInfo.ID)
			if err != nil {
				checkErr = errors.Trace(err)
			}
			if len(noneTable.Indices()) != 0 {
				checkErr = errors.New("index should have been dropped")
			}
		}
	}
	d.start()
	testDropIndex(c, ctx, d, s.dbInfo, publicTable.Meta(), "c2")
	c.Check(errors.ErrorStack(checkErr), Equals, "")
}

func checkIndexExists(ctx context.Context, tbl table.Table, indexValue interface{}, handle int64, exists bool) error {
	txn, err := ctx.GetTxn(true)
	if err != nil {
		return errors.Trace(err)
	}
	idx := tbl.Indices()[0]
	doesExist, _, err := idx.Exist(txn, types.MakeDatums(indexValue), handle)
	if err != nil {
		return errors.Trace(err)
	}
	if exists != doesExist {
		if exists {
			return errors.New("index should exists")
		}
		return errors.New("index should not exists")
	}
	return nil
}

func (s *testIndexChangeSuite) checkAddWriteOnly(d *ddl, ctx context.Context, delOnlyTbl, writeOnlyTbl table.Table) error {
	// DeleteOnlyTable: insert t values (4, 4);
	_, err := delOnlyTbl.AddRecord(ctx, types.MakeDatums(4, 4))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeOnlyTbl, 4, 4, false)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyTable: insert t values (5, 5);
	_, err = writeOnlyTbl.AddRecord(ctx, types.MakeDatums(5, 5))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeOnlyTbl, 5, 5, true)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyTable: update t set c2 = 1 where c1 = 4 and c2 = 4
	err = writeOnlyTbl.UpdateRecord(ctx, 4, types.MakeDatums(4, 4), types.MakeDatums(4, 1), touchedMap(writeOnlyTbl))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeOnlyTbl, 1, 4, true)
	if err != nil {
		return errors.Trace(err)
	}

	// DeleteOnlyTable: update t set c2 = 3 where c1 = 4 and c2 = 1
	err = delOnlyTbl.UpdateRecord(ctx, 4, types.MakeDatums(4, 1), types.MakeDatums(4, 3), touchedMap(writeOnlyTbl))
	if err != nil {
		return errors.Trace(err)
	}
	// old value index not exists.
	err = checkIndexExists(ctx, writeOnlyTbl, 1, 4, false)
	if err != nil {
		return errors.Trace(err)
	}
	// new value index not exists.
	err = checkIndexExists(ctx, writeOnlyTbl, 3, 4, false)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyTable: delete t where c1 = 4 and c2 = 3
	err = writeOnlyTbl.RemoveRecord(ctx, 4, types.MakeDatums(4, 3))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeOnlyTbl, 3, 4, false)
	if err != nil {
		return errors.Trace(err)
	}

	// DeleteOnlyTable: delete t where c1 = 5
	err = delOnlyTbl.RemoveRecord(ctx, 5, types.MakeDatums(5, 5))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeOnlyTbl, 5, 5, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testIndexChangeSuite) checkAddPublic(d *ddl, ctx context.Context, writeTbl, publicTbl table.Table) error {
	// WriteOnlyTable: insert t values (6, 6)
	_, err := writeTbl.AddRecord(ctx, types.MakeDatums(6, 6))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, publicTbl, 6, 6, true)
	if err != nil {
		return errors.Trace(err)
	}
	// PublicTable: insert t values (7, 7)
	_, err = publicTbl.AddRecord(ctx, types.MakeDatums(7, 7))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, publicTbl, 7, 7, true)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyTable: update t set c2 = 5 where c1 = 7 and c2 = 7
	err = writeTbl.UpdateRecord(ctx, 7, types.MakeDatums(7, 7), types.MakeDatums(7, 5), touchedMap(writeTbl))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, publicTbl, 5, 7, true)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, publicTbl, 7, 7, false)
	if err != nil {
		return errors.Trace(err)
	}
	// WriteOnlyTable: delete t where c1 = 6
	err = writeTbl.RemoveRecord(ctx, 6, types.MakeDatums(6, 6))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, publicTbl, 6, 6, false)

	var rows [][]types.Datum
	publicTbl.IterRecords(ctx, publicTbl.FirstKey(), publicTbl.Cols(),
		func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
			rows = append(rows, data)
			return true, nil
		})
	if len(rows) == 0 {
		return errors.New("table is empty")
	}
	for _, row := range rows {
		idxVal := row[1].GetInt64()
		handle := row[0].GetInt64()
		err = checkIndexExists(ctx, publicTbl, idxVal, handle, true)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *testIndexChangeSuite) checkDropWriteOnly(d *ddl, ctx context.Context, publicTbl, writeTbl table.Table) error {
	// WriteOnlyTable insert t values (8, 8)
	_, err := writeTbl.AddRecord(ctx, types.MakeDatums(8, 8))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, publicTbl, 8, 8, true)
	if err != nil {
		return errors.Trace(err)
	}
	// WriteOnlyTable update t set c2 = 7 where c1 = 8 and c2 = 8
	err = writeTbl.UpdateRecord(ctx, 8, types.MakeDatums(8, 8), types.MakeDatums(8, 7), touchedMap(writeTbl))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, publicTbl, 7, 8, true)
	// WriteOnlyTable delete t where c1 = 8
	err = writeTbl.RemoveRecord(ctx, 8, types.MakeDatums(8, 7))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, publicTbl, 7, 8, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testIndexChangeSuite) checkDropDeleteOnly(d *ddl, ctx context.Context, writeTbl, delTbl table.Table) error {
	// WriteOnlyTable insert t values (9, 9)
	_, err := writeTbl.AddRecord(ctx, types.MakeDatums(9, 9))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeTbl, 9, 9, true)
	// DeleteOnlyTable insert t values (10, 10)
	_, err = delTbl.AddRecord(ctx, types.MakeDatums(10, 10))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeTbl, 10, 10, false)
	if err != nil {
		return errors.Trace(err)
	}
	// DeleteOnlyTable update t set c2 = 10 where c1 = 9
	err = delTbl.UpdateRecord(ctx, 9, types.MakeDatums(9, 9), types.MakeDatums(9, 10), touchedMap(delTbl))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeTbl, 9, 9, false)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeTbl, 10, 9, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
