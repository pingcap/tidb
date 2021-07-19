// Copyright 2021 PingCAP, Inc.
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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
)

var _ = Suite(&testUnionIterSuit{})

type testUnionIterSuit struct {
}

func (s *testUnionIterSuit) TestUnionIter(c *C) {
	// test iter normal cases, snap iter become invalid before dirty iter
	snapRecords := []*mockRecord{
		r("k01", "v1"),
		r("k03", "v3"),
		r("k06", "v6"),
		r("k10", "v10"),
		r("k12", "v12"),
		r("k15", "v15"),
		r("k16", "v16"),
	}

	dirtyRecords := []*mockRecord{
		r("k03", "x3"),
		r("k05", "x5"),
		r("k07", "x7"),
		r("k08", "x8"),
	}

	assertUnionIter(c, dirtyRecords, snapRecords, []*mockRecord{
		r("k01", "v1"),
		r("k03", "x3"),
		r("k05", "x5"),
		r("k06", "v6"),
		r("k07", "x7"),
		r("k08", "x8"),
		r("k10", "v10"),
		r("k12", "v12"),
		r("k15", "v15"),
		r("k16", "v16"),
	})

	// test iter normal cases, dirty iter become invalid before snap iter
	dirtyRecords = []*mockRecord{
		r("k03", "x3"),
		r("k05", "x5"),
		r("k07", "x7"),
		r("k08", "x8"),
		r("k17", "x17"),
		r("k18", "x18"),
	}

	assertUnionIter(c, dirtyRecords, snapRecords, []*mockRecord{
		r("k01", "v1"),
		r("k03", "x3"),
		r("k05", "x5"),
		r("k06", "v6"),
		r("k07", "x7"),
		r("k08", "x8"),
		r("k10", "v10"),
		r("k12", "v12"),
		r("k15", "v15"),
		r("k16", "v16"),
		r("k17", "x17"),
		r("k18", "x18"),
	})
}

func assertUnionIter(c *C, dirtyRecords, snapRecords, expected []*mockRecord) {
	iter, err := executor.NewUnionIter(newMockIter(dirtyRecords), newMockIter(snapRecords), false)
	c.Assert(err, IsNil)
	assertIter(c, iter, expected)

	// assert reverse is true
	iter, err = executor.NewUnionIter(newMockIter(reverseRecords(dirtyRecords)), newMockIter(reverseRecords(snapRecords)), true)
	c.Assert(err, IsNil)
	assertIter(c, iter, reverseRecords(expected))
}

func assertIter(c *C, iter kv.Iterator, expected []*mockRecord) {
	records := make([]*mockRecord, 0, len(expected))
	for iter.Valid() {
		records = append(records, &mockRecord{iter.Key(), iter.Value()})
		err := iter.Next()
		c.Assert(err, IsNil)
	}
	c.Assert(len(records), Equals, len(expected))
	for idx, record := range records {
		c.Assert(record.key, BytesEquals, expected[idx].key)
		c.Assert(record.value, BytesEquals, expected[idx].value)
	}
}

func reverseRecords(records []*mockRecord) []*mockRecord {
	reversed := make([]*mockRecord, 0)
	for i := range records {
		reversed = append(reversed, records[len(records)-i-1])
	}
	return reversed
}

type mockRecord struct {
	key   []byte
	value []byte
}

func r(key, value string) *mockRecord {
	bKey := []byte(key)
	bValue := []byte(value)
	if value == "nil" {
		bValue = nil
	}

	return &mockRecord{bKey, bValue}
}

type mockIter struct {
	data []*mockRecord
	cur  int
}

func newMockIter(records []*mockRecord) *mockIter {
	return &mockIter{
		records,
		0,
	}
}

func (m *mockIter) Valid() bool {
	return m.cur >= 0 && m.cur < len(m.data)
}

func (m *mockIter) Key() kv.Key {
	return m.data[m.cur].key
}

func (m *mockIter) Value() []byte {
	return m.data[m.cur].value
}

func (m *mockIter) Next() error {
	if m.Valid() {
		m.cur += 1
	}

	return nil
}

func (m *mockIter) Close() {
	m.cur = -1
}
