// Copyright 2017 PingCAP, Inc.
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

package logutil_test

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type testHexSuite struct {
	suite.Suite
}

func (s *testHexSuite) TestHex() {
	assert := assert.New(s.T())
	var region metapb.Region
	region.Id = 6662
	region.StartKey = []byte{'t', 200, '\\', 000, 000, 000, '\\', 000, 000, 000, 37, '-', 000, 000, 000, 000, 000, 000, 000, 37}
	region.EndKey = []byte("3asg3asd")

	assert.Equal(logutil.Hex(&region).String(), "{Id:6662 StartKey:74c85c0000005c000000252d0000000000000025 EndKey:3361736733617364 RegionEpoch:<nil> Peers:[] EncryptionMeta:<nil>}")
}

func (s *testHexSuite) TestPrettyPrint() {
	assert := assert.New(s.T())
	var buf bytes.Buffer

	byteSlice := []byte("asd2fsdafs中文3af")
	logutil.PrettyPrint(&buf, reflect.ValueOf(byteSlice))
	assert.Equal(buf.String(), "61736432667364616673e4b8ade69687336166")
	assert.Equal(buf.String(), hex.EncodeToString(byteSlice))
	buf.Reset()

	// Go reflect can't distinguish uint8 from byte!
	intSlice := []uint8{1, 2, 3, uint8('a'), uint8('b'), uint8('c'), uint8('\'')}
	logutil.PrettyPrint(&buf, reflect.ValueOf(intSlice))
	assert.Equal(buf.String(), "01020361626327")
	buf.Reset()

	var ran kv.KeyRange
	ran.StartKey = kv.Key("_txxey23_i263")
	ran.EndKey = nil
	logutil.PrettyPrint(&buf, reflect.ValueOf(ran))
	assert.Equal(buf.String(), "{StartKey:5f747878657932335f69323633 EndKey:}")
}

func TestHexTestSuite(t *testing.T) {
	suite.Run(t, new(testHexSuite))
}
