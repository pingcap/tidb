// Copyright 2020 PingCAP, Inc.
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
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

var _ = Suite(&testPartitionSuite{})

type testPartitionSuite struct {
	store kv.Storage
}

func (s *testPartitionSuite) SetUpSuite(c *C) {
	s.store = testCreateStore(c, "test_store")
}

func (s *testPartitionSuite) TearDownSuite(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testPartitionSuite) TestDropAndTruncatePartition(c *C) {
	d := testNewDDLAndStart(
		context.Background(),
		c,
		WithStore(s.store),
		WithLease(testLease),
	)
	defer d.Stop()
	dbInfo := testSchemaInfo(c, d, "test_partition")
	testCreateSchema(c, testNewContext(d), d, dbInfo)
	// generate 5 partition in tableInfo.
	tblInfo, partIDs := buildTableInfoWithPartition(c, d)
	ctx := testNewContext(d)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)

	testDropPartition(c, ctx, d, dbInfo, tblInfo, []string{"p0", "p1"})

	testTruncatePartition(c, ctx, d, dbInfo, tblInfo, []int64{partIDs[3], partIDs[4]})
}

func buildTableInfoWithPartition(c *C, d *ddl) (*model.TableInfo, []int64) {
	tbl := &model.TableInfo{
		Name: model.NewCIStr("t"),
	}
	col := &model.ColumnInfo{
		Name:      model.NewCIStr("c"),
		Offset:    1,
		State:     model.StatePublic,
		FieldType: *types.NewFieldType(mysql.TypeLong),
		ID:        allocateColumnID(tbl),
	}
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
	tbl.ID = genIDs[0]
	tbl.Columns = []*model.ColumnInfo{col}
	tbl.Charset = "utf8"
	tbl.Collate = "utf8_bin"

	partIDs, err := d.genGlobalIDs(5)
	c.Assert(err, IsNil)
	partInfo := &model.PartitionInfo{
		Type:   model.PartitionTypeRange,
		Expr:   tbl.Columns[0].Name.L,
		Enable: true,
		Definitions: []model.PartitionDefinition{
			{
				ID:       partIDs[0],
				Name:     model.NewCIStr("p0"),
				LessThan: []string{"100"},
			},
			{
				ID:       partIDs[1],
				Name:     model.NewCIStr("p1"),
				LessThan: []string{"200"},
			},
			{
				ID:       partIDs[2],
				Name:     model.NewCIStr("p2"),
				LessThan: []string{"300"},
			},
			{
				ID:       partIDs[3],
				Name:     model.NewCIStr("p3"),
				LessThan: []string{"400"},
			},
			{
				ID:       partIDs[4],
				Name:     model.NewCIStr("p4"),
				LessThan: []string{"500"},
			},
		},
	}
	tbl.Partition = partInfo
	return tbl, partIDs
}

func buildDropPartitionJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, partNames []string) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{partNames},
	}
}

func testDropPartition(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, partNames []string) *model.Job {
	job := buildDropPartitionJob(dbInfo, tblInfo, partNames)
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildTruncatePartitionJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, pids []int64) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionTruncateTablePartition,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{pids},
	}
}

func testTruncatePartition(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, pids []int64) *model.Job {
	job := buildTruncatePartitionJob(dbInfo, tblInfo, pids)
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}
