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

package kv

import (
	"bytes"
	"errors"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testKeySuite{})

type testKeySuite struct {
}

func (s *testKeySuite) TestPartialNext(c *C) {
	defer testleak.AfterTest(c)()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	// keyA represents a multi column index.
	keyA, err := codec.EncodeValue(sc, nil, types.NewDatum("abc"), types.NewDatum("def"))
	c.Check(err, IsNil)
	keyB, err := codec.EncodeValue(sc, nil, types.NewDatum("abca"), types.NewDatum("def"))
	c.Check(err, IsNil)

	// We only use first column value to seek.
	seekKey, err := codec.EncodeValue(sc, nil, types.NewDatum("abc"))
	c.Check(err, IsNil)

	nextKey := Key(seekKey).Next()
	cmp := bytes.Compare(nextKey, keyA)
	c.Assert(cmp, Equals, -1)

	// Use next partial key, we can skip all index keys with first column value equal to "abc".
	nextPartialKey := Key(seekKey).PrefixNext()
	cmp = bytes.Compare(nextPartialKey, keyA)
	c.Assert(cmp, Equals, 1)

	cmp = bytes.Compare(nextPartialKey, keyB)
	c.Assert(cmp, Equals, -1)
}

func (s *testKeySuite) TestIsPoint(c *C) {
	tests := []struct {
		start   []byte
		end     []byte
		isPoint bool
	}{
		{
			start:   Key("rowkey1"),
			end:     Key("rowkey2"),
			isPoint: true,
		},
		{
			start:   Key("rowkey1"),
			end:     Key("rowkey3"),
			isPoint: false,
		},
		{
			start:   Key(""),
			end:     []byte{0},
			isPoint: true,
		},
		{
			start:   []byte{123, 123, 255, 255},
			end:     []byte{123, 124, 0, 0},
			isPoint: true,
		},
		{
			start:   []byte{123, 123, 255, 255},
			end:     []byte{123, 124, 0, 1},
			isPoint: false,
		},
		{
			start:   []byte{123, 123},
			end:     []byte{123, 123, 0},
			isPoint: true,
		},
		{
			start:   []byte{255},
			end:     []byte{0},
			isPoint: false,
		},
	}
	for _, tt := range tests {
		kr := KeyRange{
			StartKey: tt.start,
			EndKey:   tt.end,
		}
		c.Check(kr.IsPoint(), Equals, tt.isPoint)
	}
}

func (s *testKeySuite) TestBasicFunc(c *C) {
	c.Assert(IsTxnRetryableError(nil), IsFalse)
	c.Assert(IsTxnRetryableError(ErrTxnRetryable), IsTrue)
	c.Assert(IsTxnRetryableError(errors.New("test")), IsFalse)
}

func BenchmarkIsPoint(b *testing.B) {
	b.ReportAllocs()
	kr := KeyRange{
		StartKey: []byte("rowkey1"),
		EndKey:   []byte("rowkey2"),
	}
	for i := 0; i < b.N; i++ {
		kr.IsPoint()
	}
}
