// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream_test

import (
	"bytes"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/stream"
)

type testDecodeKVSuite struct{}

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDecodeKVSuite{})

func (s *testDecodeKVSuite) TestDecodeKVEntry(c *C) {
	var (
		pairs = map[string]string{
			"db": "tidb",
			"kv": "tikv",
		}
		buff = make([]byte, 0)
	)

	for k, v := range pairs {
		buff = append(buff, stream.EncodeKVEntry([]byte(k), []byte(v))...)
	}

	ei := stream.NewEventIterator(buff)
	for ei.Valid() {
		ei.Next()
		err := ei.GetError()
		c.Assert(err, IsNil)

		key := ei.Key()
		value := ei.Value()
		v, exist := pairs[string(key)]
		c.Assert(exist, IsTrue)
		c.Assert(string(value), Equals, v)
	}
}

func (s *testDecodeKVSuite) TestDecodeKVEntryError(c *C) {
	var (
		k    = []byte("db")
		v    = []byte("tidb")
		buff = make([]byte, 0)
	)

	buff = append(buff, stream.EncodeKVEntry(k, v)...)
	buff = append(buff, 'x')

	ei := stream.NewEventIterator(buff)
	ei.Next()
	c.Assert(bytes.Equal(k, ei.Key()), IsTrue)
	c.Assert(bytes.Equal(v, ei.Value()), IsTrue)
	c.Assert(ei.Valid(), IsTrue)

	ei.Next()
	c.Assert(ei.GetError(), NotNil)
}
