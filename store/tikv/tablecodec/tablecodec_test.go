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

package tablecodec

import (
	"math"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/util/codec"
	"github.com/pingcap/tidb/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = SerialSuites(&testTableCodecSuite{})

type testTableCodecSuite struct{}

func (s *testTableCodecSuite) TestDecodeBadDecical(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/util/codec/errorInDecodeDecimal", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/util/codec/errorInDecodeDecimal"), IsNil)
	}()
	dec := types.NewDecFromStringForTest("0.111")
	b, err := codec.EncodeDecimal(nil, dec, 0, 0)
	c.Assert(err, IsNil)
	// Expect no panic.
	_, _, err = codec.DecodeOne(b)
	c.Assert(err, NotNil)
}

func (s *testTableCodecSuite) TestIndexKey(c *C) {
	tableID := int64(4)
	indexID := int64(5)
	indexKey := EncodeIndexSeekKey(tableID, indexID, []byte{})
	tTableID, tIndexID, isRecordKey, err := DecodeKeyHead(indexKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)
	c.Assert(tIndexID, Equals, indexID)
	c.Assert(isRecordKey, IsFalse)
}

func (s *testTableCodecSuite) TestRecordKey(c *C) {
	tableID := int64(55)
	tableKey := EncodeRowKeyWithHandle(tableID, kv.IntHandle(math.MaxUint32))
	tTableID, _, isRecordKey, err := DecodeKeyHead(tableKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)
	c.Assert(isRecordKey, IsTrue)

	encodedHandle := codec.EncodeInt(nil, math.MaxUint32)
	rowKey := EncodeRowKey(tableID, encodedHandle)
	c.Assert([]byte(tableKey), BytesEquals, []byte(rowKey))
	tTableID, handle, err := DecodeRecordKey(rowKey)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)
	c.Assert(handle.IntValue(), Equals, int64(math.MaxUint32))

	recordPrefix := GenTableRecordPrefix(tableID)
	rowKey = EncodeRecordKey(recordPrefix, kv.IntHandle(math.MaxUint32))
	c.Assert([]byte(tableKey), BytesEquals, []byte(rowKey))

	_, _, err = DecodeRecordKey(nil)
	c.Assert(err, NotNil)
	_, _, err = DecodeRecordKey([]byte("abcdefghijklmnopqrstuvwxyz"))
	c.Assert(err, NotNil)
	c.Assert(DecodeTableID(nil), Equals, int64(0))
}

func (s *testTableCodecSuite) TestPrefix(c *C) {
	const tableID int64 = 66
	key := EncodeTablePrefix(tableID)
	tTableID := DecodeTableID(key)
	c.Assert(tTableID, Equals, tableID)

	c.Assert(TablePrefix(), BytesEquals, tablePrefix)

	tablePrefix1 := GenTablePrefix(tableID)
	c.Assert([]byte(tablePrefix1), BytesEquals, []byte(key))

	indexPrefix := EncodeTableIndexPrefix(tableID, math.MaxUint32)
	tTableID, indexID, isRecordKey, err := DecodeKeyHead(indexPrefix)
	c.Assert(err, IsNil)
	c.Assert(tTableID, Equals, tableID)
	c.Assert(indexID, Equals, int64(math.MaxUint32))
	c.Assert(isRecordKey, IsFalse)

	prefixKey := GenTableIndexPrefix(tableID)
	c.Assert(DecodeTableID(prefixKey), Equals, tableID)

	c.Assert(TruncateToRowKeyLen(append(indexPrefix, "xyz"...)), HasLen, RecordRowKeyLen)
	c.Assert(TruncateToRowKeyLen(key), HasLen, len(key))
}

func (s *testTableCodecSuite) TestCutPrefix(c *C) {
	key := EncodeTableIndexPrefix(42, 666)
	res := CutRowKeyPrefix(key)
	c.Assert(res, BytesEquals, []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x9a})
	res = CutIndexPrefix(key)
	c.Assert(res, BytesEquals, []byte{})
}

func (s *testTableCodecSuite) TestRange(c *C) {
	s1, e1 := GetTableHandleKeyRange(22)
	s2, e2 := GetTableHandleKeyRange(23)
	c.Assert(s1, Less, e1)
	c.Assert(e1, Less, s2)
	c.Assert(s2, Less, e2)

	s1, e1 = GetTableIndexKeyRange(42, 666)
	s2, e2 = GetTableIndexKeyRange(42, 667)
	c.Assert(s1, Less, e1)
	c.Assert(e1, Less, s2)
	c.Assert(s2, Less, e2)
}

func BenchmarkHasTablePrefix(b *testing.B) {
	k := kv.Key("foobar")
	for i := 0; i < b.N; i++ {
		hasTablePrefix(k)
	}
}

func BenchmarkHasTablePrefixBuiltin(b *testing.B) {
	k := kv.Key("foobar")
	for i := 0; i < b.N; i++ {
		k.HasPrefix(tablePrefix)
	}
}

func (s *testTableCodecSuite) TestError(c *C) {
	kvErrs := []*terror.Error{
		errInvalidKey,
		errInvalidRecordKey,
		errInvalidIndexKey,
	}
	for _, err := range kvErrs {
		code := terror.ToSQLError(err).Code
		c.Assert(code != mysql.ErrUnknown && code == uint16(err.Code()), IsTrue, Commentf("err: %v", err))
	}
}
