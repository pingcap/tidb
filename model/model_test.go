// Copyright 2015 PingCAP, Inc.
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

package model

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testModelSuite{})

type testModelSuite struct {
}

func (*testModelSuite) TestT(c *C) {
	abc := NewCIStr("aBC")
	c.Assert(abc.O, Equals, "aBC")
	c.Assert(abc.L, Equals, "abc")
	c.Assert(abc.String(), Equals, "aBC")
}

func (*testModelSuite) TestModelBasic(c *C) {
	column := &ColumnInfo{
		ID:           1,
		Name:         NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
	}
	column.Flag |= mysql.PriKeyFlag

	index := &IndexInfo{
		Name:  NewCIStr("key"),
		Table: NewCIStr("t"),
		Columns: []*IndexColumn{
			{
				Name:   NewCIStr("c"),
				Offset: 0,
				Length: 10,
			}},
		Unique:  true,
		Primary: true,
	}

	fk := &FKInfo{
		RefCols: []CIStr{NewCIStr("a")},
		Cols:    []CIStr{NewCIStr("a")},
	}

	table := &TableInfo{
		ID:          1,
		Name:        NewCIStr("t"),
		Charset:     "utf8",
		Collate:     "utf8",
		Columns:     []*ColumnInfo{column},
		Indices:     []*IndexInfo{index},
		ForeignKeys: []*FKInfo{fk},
		PKIsHandle:  true,
	}

	dbInfo := &DBInfo{
		ID:      1,
		Name:    NewCIStr("test"),
		Charset: "utf8",
		Collate: "utf8",
		Tables:  []*TableInfo{table},
	}

	n := dbInfo.Clone()
	c.Assert(n, DeepEquals, dbInfo)

	pkName := table.GetPkName()
	c.Assert(pkName, Equals, NewCIStr("c"))
	newColumn := table.GetPkColInfo()
	c.Assert(newColumn, DeepEquals, column)
	inIdx := table.ColumnIsInIndex(column)
	c.Assert(inIdx, Equals, true)
	tp := IndexTypeBtree
	c.Assert(tp.String(), Equals, "BTREE")
	tp = IndexTypeHash
	c.Assert(tp.String(), Equals, "HASH")
	tp = 1E5
	c.Assert(tp.String(), Equals, "")
	has := index.HasPrefixIndex()
	c.Assert(has, Equals, true)

	// Corner cases
	column.Flag ^= mysql.PriKeyFlag
	pkName = table.GetPkName()
	c.Assert(pkName, Equals, NewCIStr(""))
	newColumn = table.GetPkColInfo()
	c.Assert(newColumn, IsNil)
	anCol := &ColumnInfo{
		Name: NewCIStr("d"),
	}
	exIdx := table.ColumnIsInIndex(anCol)
	c.Assert(exIdx, Equals, false)
	anIndex := &IndexInfo{
		Columns: []*IndexColumn{},
	}
	no := anIndex.HasPrefixIndex()
	c.Assert(no, Equals, false)
}

func (*testModelSuite) TestJobCodec(c *C) {
	type A struct {
		Name string
	}
	job := &Job{
		ID:         1,
		BinlogInfo: &HistoryInfo{},
		Args:       []interface{}{NewCIStr("a"), A{Name: "abc"}},
	}
	job.BinlogInfo.AddDBInfo(123, &DBInfo{ID: 1, Name: NewCIStr("test_history_db")})
	job.BinlogInfo.AddTableInfo(123, &TableInfo{ID: 1, Name: NewCIStr("test_history_tbl")})

	c.Assert(job.IsCancelled(), Equals, false)
	b, err := job.Encode(false)
	c.Assert(err, IsNil)
	newJob := &Job{}
	err = newJob.Decode(b)
	c.Assert(err, IsNil)
	c.Assert(newJob.BinlogInfo, DeepEquals, job.BinlogInfo)
	name := CIStr{}
	a := A{}
	err = newJob.DecodeArgs(&name, &a)
	c.Assert(err, IsNil)
	c.Assert(name, DeepEquals, NewCIStr(""))
	c.Assert(a, DeepEquals, A{Name: ""})
	c.Assert(len(newJob.String()), Greater, 0)

	b1, err := job.Encode(true)
	c.Assert(err, IsNil)
	newJob = &Job{}
	err = newJob.Decode(b1)
	c.Assert(err, IsNil)
	c.Assert(newJob.BinlogInfo, DeepEquals, job.BinlogInfo)
	name = CIStr{}
	a = A{}
	err = newJob.DecodeArgs(&name, &a)
	c.Assert(err, IsNil)
	c.Assert(name, DeepEquals, NewCIStr("a"))
	c.Assert(a, DeepEquals, A{Name: "abc"})
	c.Assert(len(newJob.String()), Greater, 0)

	job.State = JobDone
	c.Assert(job.IsDone(), IsTrue)
	c.Assert(job.IsFinished(), IsTrue)
	c.Assert(job.IsRunning(), IsFalse)
	c.Assert(job.IsSynced(), IsFalse)
	job.SetRowCount(3)
	c.Assert(job.GetRowCount(), Equals, int64(3))
}

func (testModelSuite) TestState(c *C) {
	schemaTbl := []SchemaState{
		StateDeleteOnly,
		StateWriteOnly,
		StateWriteReorganization,
		StateDeleteReorganization,
		StatePublic,
	}

	for _, state := range schemaTbl {
		c.Assert(len(state.String()), Greater, 0)
	}

	jobTbl := []JobState{
		JobRunning,
		JobDone,
		JobCancelled,
		JobRollback,
		JobRollbackDone,
		JobSynced,
	}

	for _, state := range jobTbl {
		c.Assert(len(state.String()), Greater, 0)
	}
}

func (testModelSuite) TestString(c *C) {
	acts := []struct {
		act    ActionType
		result string
	}{
		{ActionNone, "none"},
		{ActionAddForeignKey, "add foreign key"},
		{ActionDropForeignKey, "drop foreign key"},
		{ActionTruncateTable, "truncate table"},
		{ActionModifyColumn, "modify column"},
		{ActionRenameTable, "rename table"},
		{ActionSetDefaultValue, "set default value"},
		{ActionCreateSchema, "create schema"},
		{ActionDropSchema, "drop schema"},
		{ActionCreateTable, "create table"},
		{ActionDropTable, "drop table"},
		{ActionAddIndex, "add index"},
		{ActionDropIndex, "drop index"},
		{ActionAddColumn, "add column"},
		{ActionDropColumn, "drop column"},
	}

	for _, v := range acts {
		str := v.act.String()
		c.Assert(str, Equals, v.result)
	}
}
