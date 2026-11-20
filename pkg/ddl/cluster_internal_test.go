// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"bytes"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFlashbackMaskingPolicyKeyRanges(t *testing.T) {
	excluded := addToSlice("mysql", "user", 41, nil)
	excluded = addToSlice("mysql", "tidb_masking_policy", 42, excluded)
	ranges := getTableDataKeyRanges(excluded)
	contains := func(id int64) bool {
		key := tablecodec.EncodeTablePrefix(id)
		for _, r := range ranges {
			if bytes.Compare(key, r.StartKey) >= 0 && bytes.Compare(key, r.EndKey) < 0 {
				return true
			}
		}
		return false
	}
	require.False(t, contains(41))
	require.True(t, contains(42), "policy rows must flash back with their table/column ID bindings")
	require.True(t, contains(43))
}
