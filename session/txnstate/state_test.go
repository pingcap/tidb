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

package txnstate

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/tablecodec"
)

type testMainSuite struct{}

var _ = Suite(&testMainSuite{})

func (s *testMainSuite) TestKeysNeedLock(c *C) {
	rowKey := tablecodec.EncodeRowKeyWithHandle(1, 1)
	indexKey := tablecodec.EncodeIndexSeekKey(1, 1, []byte{1})
	uniqueValue := make([]byte, 8)
	uniqueUntouched := append(uniqueValue, '1')
	nonUniqueVal := []byte{'0'}
	nonUniqueUntouched := []byte{'1'}
	var deleteVal []byte
	rowVal := []byte{'a', 'b', 'c'}
	tests := []struct {
		key  []byte
		val  []byte
		need bool
	}{
		{rowKey, rowVal, true},
		{rowKey, deleteVal, true},
		{indexKey, nonUniqueVal, false},
		{indexKey, nonUniqueUntouched, false},
		{indexKey, uniqueValue, true},
		{indexKey, uniqueUntouched, false},
		{indexKey, deleteVal, false},
	}
	for _, tt := range tests {
		c.Assert(keyNeedToLock(tt.key, tt.val), Equals, tt.need)
	}
}
