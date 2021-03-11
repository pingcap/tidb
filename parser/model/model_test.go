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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
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
		Hidden:       true,
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

	seq := &SequenceInfo{
		Increment: 1,
		MinValue:  1,
		MaxValue:  100,
	}

	table := &TableInfo{
		ID:          1,
		Name:        NewCIStr("t"),
		Charset:     "utf8",
		Collate:     "utf8_bin",
		Columns:     []*ColumnInfo{column},
		Indices:     []*IndexInfo{index},
		ForeignKeys: []*FKInfo{fk},
		PKIsHandle:  true,
	}

	table2 := &TableInfo{
		ID:       2,
		Name:     NewCIStr("s"),
		Sequence: seq,
	}

	dbInfo := &DBInfo{
		ID:      1,
		Name:    NewCIStr("test"),
		Charset: "utf8",
		Collate: "utf8_bin",
		Tables:  []*TableInfo{table},
	}

	n := dbInfo.Clone()
	c.Assert(n, DeepEquals, dbInfo)

	pkName := table.GetPkName()
	c.Assert(pkName, Equals, NewCIStr("c"))
	newColumn := table.GetPkColInfo()
	c.Assert(newColumn.Hidden, Equals, true)
	c.Assert(newColumn, DeepEquals, column)
	inIdx := table.ColumnIsInIndex(column)
	c.Assert(inIdx, Equals, true)
	tp := IndexTypeBtree
	c.Assert(tp.String(), Equals, "BTREE")
	tp = IndexTypeHash
	c.Assert(tp.String(), Equals, "HASH")
	tp = 1e5
	c.Assert(tp.String(), Equals, "")
	has := index.HasPrefixIndex()
	c.Assert(has, Equals, true)
	t := table.GetUpdateTime()
	c.Assert(t, Equals, TSConvert2Time(table.UpdateTS))
	c.Assert(table2.IsSequence(), IsTrue)
	c.Assert(table2.IsBaseTable(), IsFalse)

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

	extraPK := NewExtraHandleColInfo()
	c.Assert(extraPK.Flag, Equals, uint(mysql.NotNullFlag|mysql.PriKeyFlag))
}

func (*testModelSuite) TestJobStartTime(c *C) {
	job := &Job{
		ID:         123,
		BinlogInfo: &HistoryInfo{},
	}
	t := time.Unix(0, 0)
	c.Assert(t, Equals, TSConvert2Time(job.StartTS))
	ret := fmt.Sprintf("%s", job)
	c.Assert(job.String(), Equals, ret)
}

func (*testModelSuite) TestJobCodec(c *C) {
	type A struct {
		Name string
	}
	job := &Job{
		ID:         1,
		TableID:    2,
		SchemaID:   1,
		BinlogInfo: &HistoryInfo{},
		Args:       []interface{}{NewCIStr("a"), A{Name: "abc"}},
	}
	job.BinlogInfo.AddDBInfo(123, &DBInfo{ID: 1, Name: NewCIStr("test_history_db")})
	job.BinlogInfo.AddTableInfo(123, &TableInfo{ID: 1, Name: NewCIStr("test_history_tbl")})

	// Test IsDependentOn.
	// job: table ID is 2
	// job1: table ID is 2
	var err error
	job1 := &Job{
		ID:         2,
		TableID:    2,
		SchemaID:   1,
		Type:       ActionRenameTable,
		BinlogInfo: &HistoryInfo{},
		Args:       []interface{}{int64(3), NewCIStr("new_table_name")},
	}
	job1.RawArgs, err = json.Marshal(job1.Args)
	c.Assert(err, IsNil)
	isDependent, err := job.IsDependentOn(job1)
	c.Assert(err, IsNil)
	c.Assert(isDependent, IsTrue)
	// job1: rename table, old schema ID is 3
	// job2: create schema, schema ID is 3
	job2 := &Job{
		ID:         3,
		TableID:    3,
		SchemaID:   3,
		Type:       ActionCreateSchema,
		BinlogInfo: &HistoryInfo{},
	}
	isDependent, err = job2.IsDependentOn(job1)
	c.Assert(err, IsNil)
	c.Assert(isDependent, IsTrue)

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

	job.BinlogInfo.Clean()
	b1, err := job.Encode(true)
	c.Assert(err, IsNil)
	newJob = &Job{}
	err = newJob.Decode(b1)
	c.Assert(err, IsNil)
	c.Assert(newJob.BinlogInfo, DeepEquals, &HistoryInfo{})
	name = CIStr{}
	a = A{}
	err = newJob.DecodeArgs(&name, &a)
	c.Assert(err, IsNil)
	c.Assert(name, DeepEquals, NewCIStr("a"))
	c.Assert(a, DeepEquals, A{Name: "abc"})
	c.Assert(len(newJob.String()), Greater, 0)

	b2, err := job.Encode(true)
	c.Assert(err, IsNil)
	newJob = &Job{}
	err = newJob.Decode(b2)
	c.Assert(err, IsNil)
	name = CIStr{}
	// Don't decode to a here.
	err = newJob.DecodeArgs(&name)
	c.Assert(err, IsNil)
	c.Assert(name, DeepEquals, NewCIStr("a"))
	c.Assert(len(newJob.String()), Greater, 0)

	job.State = JobStateDone
	c.Assert(job.IsDone(), IsTrue)
	c.Assert(job.IsFinished(), IsTrue)
	c.Assert(job.IsRunning(), IsFalse)
	c.Assert(job.IsSynced(), IsFalse)
	c.Assert(job.IsRollbackDone(), IsFalse)
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
		StateGlobalTxnOnly,
	}

	for _, state := range schemaTbl {
		c.Assert(len(state.String()), Greater, 0)
	}

	jobTbl := []JobState{
		JobStateRunning,
		JobStateDone,
		JobStateCancelled,
		JobStateRollingback,
		JobStateRollbackDone,
		JobStateSynced,
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
		{ActionAddColumns, "add multi-columns"},
		{ActionDropColumn, "drop column"},
		{ActionDropColumns, "drop multi-columns"},
		{ActionModifySchemaCharsetAndCollate, "modify schema charset and collate"},
		{ActionDropIndexes, "drop multi-indexes"},
	}

	for _, v := range acts {
		str := v.act.String()
		c.Assert(str, Equals, v.result)
	}
}

func (testModelSuite) TestUnmarshalCIStr(c *C) {
	var ci CIStr

	// Test unmarshal CIStr from a single string.
	str := "aaBB"
	buf, err := json.Marshal(str)
	c.Assert(err, IsNil)
	ci.UnmarshalJSON(buf)
	c.Assert(ci.O, Equals, str)
	c.Assert(ci.L, Equals, "aabb")

	buf, err = json.Marshal(ci)
	c.Assert(string(buf), Equals, `{"O":"aaBB","L":"aabb"}`)
	ci.UnmarshalJSON(buf)
	c.Assert(ci.O, Equals, str)
	c.Assert(ci.L, Equals, "aabb")
}

func (testModelSuite) TestDefaultValue(c *C) {
	srcCol := &ColumnInfo{
		ID: 1,
	}
	randPlainStr := "random_plain_string"

	oldPlainCol := srcCol.Clone()
	oldPlainCol.Name = NewCIStr("oldPlainCol")
	oldPlainCol.FieldType = *types.NewFieldType(mysql.TypeLong)
	oldPlainCol.DefaultValue = randPlainStr
	oldPlainCol.OriginDefaultValue = randPlainStr

	newPlainCol := srcCol.Clone()
	newPlainCol.Name = NewCIStr("newPlainCol")
	newPlainCol.FieldType = *types.NewFieldType(mysql.TypeLong)
	err := newPlainCol.SetDefaultValue(1)
	c.Assert(err, IsNil)
	c.Assert(newPlainCol.GetDefaultValue(), Equals, 1)
	err = newPlainCol.SetDefaultValue(randPlainStr)
	c.Assert(newPlainCol.GetDefaultValue(), Equals, randPlainStr)

	randBitStr := string([]byte{25, 185})

	oldBitCol := srcCol.Clone()
	oldBitCol.Name = NewCIStr("oldBitCol")
	oldBitCol.FieldType = *types.NewFieldType(mysql.TypeBit)
	oldBitCol.DefaultValue = randBitStr
	oldBitCol.OriginDefaultValue = randBitStr

	newBitCol := srcCol.Clone()
	newBitCol.Name = NewCIStr("newBitCol")
	newBitCol.FieldType = *types.NewFieldType(mysql.TypeBit)
	err = newBitCol.SetDefaultValue(1)
	// Only string type is allowed in BIT column.
	c.Assert(err, ErrorMatches, ".*Invalid default value.*")
	c.Assert(newBitCol.GetDefaultValue(), Equals, 1)
	err = newBitCol.SetDefaultValue(randBitStr)
	c.Assert(newBitCol.GetDefaultValue(), Equals, randBitStr)

	nullBitCol := srcCol.Clone()
	nullBitCol.Name = NewCIStr("nullBitCol")
	nullBitCol.FieldType = *types.NewFieldType(mysql.TypeBit)
	err = nullBitCol.SetOriginDefaultValue(nil)
	c.Assert(err, IsNil)
	c.Assert(nullBitCol.GetOriginDefaultValue(), IsNil)

	testCases := []struct {
		col          *ColumnInfo
		isConsistent bool
	}{
		{oldPlainCol, true},
		{oldBitCol, false},
		{newPlainCol, true},
		{newBitCol, true},
		{nullBitCol, true},
	}
	for _, tc := range testCases {
		col, isConsistent := tc.col, tc.isConsistent
		cmt := Commentf("%s assertion failed", col.Name.O)
		bytes, err := json.Marshal(col)
		c.Assert(err, IsNil, cmt)
		var newCol ColumnInfo
		err = json.Unmarshal(bytes, &newCol)
		c.Assert(err, IsNil, cmt)
		if isConsistent {
			c.Assert(col.GetDefaultValue(), Equals, newCol.GetDefaultValue())
			c.Assert(col.GetOriginDefaultValue(), Equals, newCol.GetOriginDefaultValue())
		} else {
			c.Assert(col.DefaultValue == newCol.DefaultValue, IsFalse, cmt)
			c.Assert(col.OriginDefaultValue == newCol.OriginDefaultValue, IsFalse, cmt)
		}
	}
}
