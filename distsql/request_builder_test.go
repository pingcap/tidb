// Copyright 2018 PingCAP, Inc.
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

package distsql

import (
	"os"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(&logutil.LogConfig{
		Level: logLevel,
	})
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

type handleRange struct {
	start int64
	end   int64
}

func (s *testSuite) getExpectedRanges(tid int64, hrs []*handleRange) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(hrs))
	for _, hr := range hrs {
		startKey := tablecodec.EncodeRowKeyWithHandle(tid, hr.start)
		endKey := tablecodec.EncodeRowKeyWithHandle(tid, hr.end)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs
}

func (s *testSuite) TestTableHandlesToKVRanges(c *C) {
	handles := []int64{0, 2, 3, 4, 5, 10, 11, 100}

	// Build expected key ranges.
	hrs := make([]*handleRange, 0, len(handles))
	hrs = append(hrs, &handleRange{start: 0, end: 1})
	hrs = append(hrs, &handleRange{start: 2, end: 6})
	hrs = append(hrs, &handleRange{start: 10, end: 12})
	hrs = append(hrs, &handleRange{start: 100, end: 101})

	// Build key ranges.
	expect := s.getExpectedRanges(1, hrs)
	actual := TableHandlesToKVRanges(1, handles)

	// Compare key ranges and expected key ranges.
	c.Assert(len(actual), Equals, len(expect))
	for i := range actual {
		c.Assert(actual[i].StartKey, DeepEquals, expect[i].StartKey)
		c.Assert(actual[i].EndKey, DeepEquals, expect[i].EndKey)
	}
}
