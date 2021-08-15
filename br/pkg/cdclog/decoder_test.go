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

package cdclog

import (
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
)

func Test(t *testing.T) { check.TestingT(t) }

type batchSuite struct {
	ddlEvents     []*MessageDDL
	ddlDecodeItem []*SortItem

	rowEvents     []*MessageRow
	rowDecodeItem []*SortItem
}

var updateCols = map[string]Column{
	// json.Number
	"id":   {Type: 1, Value: "1"},
	"name": {Type: 2, Value: "test"},
}

var _ = check.Suite(&batchSuite{
	ddlEvents: []*MessageDDL{
		{"drop table event", 4},
		{"create table event", 3},
		{"drop table event", 5},
	},
	ddlDecodeItem: []*SortItem{
		{ItemType: DDL, Schema: "test", Table: "event", TS: 1, Data: MessageDDL{"drop table event", 4}},
		{ItemType: DDL, Schema: "test", Table: "event", TS: 1, Data: MessageDDL{"create table event", 3}},
		{ItemType: DDL, Schema: "test", Table: "event", TS: 1, Data: MessageDDL{"drop table event", 5}},
	},

	rowEvents: []*MessageRow{
		{Update: updateCols},
		{PreColumns: updateCols},
		{Delete: updateCols},
	},
	rowDecodeItem: []*SortItem{
		{ItemType: RowChanged, Schema: "test", Table: "event", TS: 1, RowID: 0, Data: MessageRow{Update: updateCols}},
		{ItemType: RowChanged, Schema: "test", Table: "event", TS: 1, RowID: 1, Data: MessageRow{PreColumns: updateCols}},
		{ItemType: RowChanged, Schema: "test", Table: "event", TS: 1, RowID: 2, Data: MessageRow{Delete: updateCols}},
	},
})

func buildEncodeRowData(events []*MessageRow) []byte {
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	data := append(make([]byte, 0), versionByte[:]...)
	key := messageKey{
		TS:     1,
		Schema: "test",
		Table:  "event",
	}
	var LenByte [8]byte
	for i := 0; i < len(events); i++ {
		key.RowID = int64(i)
		keyBytes, _ := key.Encode()
		binary.BigEndian.PutUint64(LenByte[:], uint64(len(keyBytes)))
		data = append(data, LenByte[:]...)
		data = append(data, keyBytes...)
		eventBytes, _ := events[i].Encode()
		binary.BigEndian.PutUint64(LenByte[:], uint64(len(eventBytes)))
		data = append(data, LenByte[:]...)
		data = append(data, eventBytes...)
	}
	return data
}

func buildEncodeDDLData(events []*MessageDDL) []byte {
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], BatchVersion1)
	data := append(make([]byte, 0), versionByte[:]...)
	key := messageKey{
		TS:     1,
		Schema: "test",
		Table:  "event",
	}
	var LenByte [8]byte
	for i := 0; i < len(events); i++ {
		keyBytes, _ := key.Encode()
		binary.BigEndian.PutUint64(LenByte[:], uint64(len(keyBytes)))
		data = append(data, LenByte[:]...)
		data = append(data, keyBytes...)
		eventBytes, _ := events[i].Encode()
		binary.BigEndian.PutUint64(LenByte[:], uint64(len(eventBytes)))
		data = append(data, LenByte[:]...)
		data = append(data, eventBytes...)
	}
	return data
}

func (s *batchSuite) TestDecoder(c *check.C) {
	var item *SortItem

	data := buildEncodeDDLData(s.ddlEvents)
	decoder, err := NewJSONEventBatchDecoder(data)
	c.Assert(err, check.IsNil)
	index := 0
	for {
		hasNext := decoder.HasNext()
		if !hasNext {
			break
		}
		item, err = decoder.NextEvent(DDL)
		c.Assert(err, check.IsNil)
		c.Assert(item.Data.(*MessageDDL), check.DeepEquals, s.ddlEvents[index])
		index++
	}

	data = buildEncodeRowData(s.rowEvents)
	decoder, err = NewJSONEventBatchDecoder(data)
	c.Assert(err, check.IsNil)
	index = 0
	for {
		hasNext := decoder.HasNext()
		if !hasNext {
			break
		}
		item, err = decoder.NextEvent(RowChanged)
		c.Assert(err, check.IsNil)
		c.Assert(item.Data.(*MessageRow), check.DeepEquals, s.rowEvents[index])
		c.Assert(item.RowID, check.Equals, int64(index))
		index++
	}
}

func (s *batchSuite) TestColumn(c *check.C) {
	// test varbinary columns (same type with varchar 15)
	col1 := Column{Type: mysql.TypeVarchar, Flag: BinaryFlag, Value: "\\x00\\x01"}
	col1 = formatColumnVal(col1)
	dat, err := col1.ToDatum()
	c.Assert(err, check.IsNil)
	c.Assert(dat.GetString(), check.Equals, "\x00\x01")

	// test binary columns (same type with varchar 254)
	col2 := Column{Type: mysql.TypeString, Flag: BinaryFlag, Value: "test\\ttest"}
	col2 = formatColumnVal(col2)
	dat, err = col2.ToDatum()
	c.Assert(err, check.IsNil)
	c.Assert(dat.GetString(), check.Equals, "test\ttest")

	// test year columns
	val := json.Number("2020")
	colYear := Column{Type: mysql.TypeYear, Value: val}
	dat, err = colYear.ToDatum()
	c.Assert(err, check.IsNil)
	c.Assert(dat.GetInt64(), check.Equals, int64(2020))
}
