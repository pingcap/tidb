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
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (*testSuite) TestT(c *C) {
	abc := NewCIStr("aBC")
	c.Assert(abc.O, Equals, "aBC")
	c.Assert(abc.L, Equals, "abc")
	c.Assert(abc.String(), Equals, "aBC")
}

func (*testSuite) TestClone(c *C) {
	column := &ColumnInfo{
		ID:           1,
		Name:         NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
	}

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

	table := &TableInfo{
		ID:          1,
		Name:        NewCIStr("t"),
		Charset:     "utf8",
		Collate:     "utf8",
		Columns:     []*ColumnInfo{column},
		Indices:     []*IndexInfo{index},
		ForeignKeys: []*FKInfo{},
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
}

func (*testSuite) TestJobCodec(c *C) {
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

	b, err := job.Encode()
	c.Assert(err, IsNil)

	newJob := &Job{}
	err = newJob.Decode(b)
	c.Assert(err, IsNil)
	c.Assert(newJob.BinlogInfo, DeepEquals, job.BinlogInfo)

	name := CIStr{}
	a := A{}
	err = newJob.DecodeArgs(&name, &a)
	c.Assert(err, IsNil)
	c.Assert(name, DeepEquals, NewCIStr("a"))
	c.Assert(a, DeepEquals, A{Name: "abc"})

	c.Assert(len(newJob.String()), Greater, 0)

	job.State = JobDone
	c.Assert(job.IsFinished(), IsTrue)
	c.Assert(job.IsRunning(), IsFalse)
}

func (testSuite) TestState(c *C) {
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
	}

	for _, state := range jobTbl {
		c.Assert(len(state.String()), Greater, 0)
	}

	actionTbl := []ActionType{
		ActionCreateSchema,
		ActionDropSchema,
		ActionCreateTable,
		ActionDropTable,
		ActionAddColumn,
		ActionDropColumn,
		ActionAddIndex,
		ActionDropIndex,
	}

	for _, action := range actionTbl {
		c.Assert(len(action.String()), Greater, 0)
	}
}
