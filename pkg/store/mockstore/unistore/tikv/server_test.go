// Copyright 2026 PingCAP, Inc.
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

package tikv

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
)

func TestMockPointReadExecDetails(t *testing.T) {
	for _, tc := range []struct {
		name       string
		pairs      []*kvrpcpb.KvPair
		keys, size uint64
	}{
		{name: "empty response"},
		{name: "miss", pairs: []*kvrpcpb.KvPair{{Key: []byte("missing")}}},
		{name: "partial success", pairs: []*kvrpcpb.KvPair{
			nil,
			{Key: []byte("bad"), Value: []byte("bad"), Error: &kvrpcpb.KeyError{}},
			{Key: []byte("k1"), Value: []byte("abc")},
			{Key: []byte("k2"), Value: []byte("abcdef")},
		}, keys: 2, size: 13},
	} {
		t.Run(tc.name, func(t *testing.T) {
			details := mockPointReadExecDetails(tc.pairs)
			if !kerneltype.IsNextGen() {
				require.Nil(t, details)
				return
			}
			require.NotNil(t, details)
			require.Equal(t, &kvrpcpb.ScanDetailV2{
				TotalVersions: tc.keys, ProcessedVersions: tc.keys,
				TotalVersionsSize: tc.size, ProcessedVersionsSize: tc.size,
			}, details.ScanDetailV2)
		})
	}
}
