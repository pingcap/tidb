// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"bytes"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

type testMetaKVSuite struct{}

var _ = Suite(&testMetaKVSuite{})

func encodeTxnMetaKey(key []byte, field []byte, ts uint64) []byte {
	k := tablecodec.EncodeMetaKey(key, field)
	txnKey := codec.EncodeBytes(nil, k)
	return codec.EncodeUintDesc(txnKey, ts)
}

func (s *testMetaKVSuite) TestRawMetaKeyForDB(c *C) {
	var (
		dbID int64  = 1
		ts   uint64 = 400036290571534337
		mDbs        = []byte("DBs")
	)

	txnKey := encodeTxnMetaKey(mDbs, meta.DBkey(dbID), ts)

	rawMetakey, err := ParseTxnMetaKeyFrom(txnKey)
	c.Assert(err, IsNil)

	parseDbID, err := meta.ParseDBKey(rawMetakey.Field)
	c.Assert(err, IsNil)
	c.Assert(parseDbID, Equals, dbID)

	newKey := rawMetakey.EncodeMetaKey()
	c.Assert(bytes.Equal(txnKey, newKey), IsTrue)
}

func (s *testMetaKVSuite) TestRawMetaKeyForTable(c *C) {
	var (
		dbID    int64  = 1
		tableID int64  = 57
		ts      uint64 = 400036290571534337
	)
	txnKey := encodeTxnMetaKey(meta.DBkey(dbID), meta.TableKey(tableID), ts)

	rawMetakey, err := ParseTxnMetaKeyFrom(txnKey)
	c.Assert(err, IsNil)

	parseDBID, err := meta.ParseDBKey(rawMetakey.Key)
	c.Assert(err, IsNil)
	c.Assert(parseDBID, Equals, dbID)

	parseTableID, err := meta.ParseTableKey(rawMetakey.Field)
	c.Assert(err, IsNil)
	c.Assert(parseTableID, Equals, tableID)

	newKey := rawMetakey.EncodeMetaKey()
	c.Assert(bytes.Equal(txnKey, newKey), IsTrue)
}

func (s *testMetaKVSuite) TestWriteType(c *C) {
	t := 'P'
	wt, err := WriteTypeFrom(byte(t))
	c.Assert(err, IsNil)
	c.Assert(wt, Equals, WriteTypePut)
}

func (s *testMetaKVSuite) TestWriteCFValueNoShortValue(c *C) {
	buff := make([]byte, 0, 9)
	buff = append(buff, byte('P'))
	buff = codec.EncodeUvarint(buff, 400036290571534337)

	v := new(RawWriteCFValue)
	err := v.ParseFrom(buff)
	c.Assert(err, IsNil)
	c.Assert(v.HasShortValue(), IsFalse)

	encodedBuff := v.EncodeTo()
	c.Assert(bytes.Equal(buff, encodedBuff), IsTrue)
}
