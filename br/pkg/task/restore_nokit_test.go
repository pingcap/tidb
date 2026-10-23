// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestPhysicalRestoreSysTables(t *testing.T) {
	usePhysicalCfg := &SnapshotRestoreConfig{RestoreConfig: &RestoreConfig{
		FastLoadSysTables: true,
		RestoreCommonConfig: RestoreCommonConfig{
			WithSysTable: true,
		},
		LoadStats: true,
	}}
	loadSysTablePhysical, loadStatsPhysical := isRestoreSysTablesPhysically(usePhysicalCfg)
	// we only test how it works on next-gen, won't test other combinations
	if kerneltype.IsNextGen() {
		require.False(t, loadSysTablePhysical)
		require.False(t, loadStatsPhysical)
	} else {
		require.True(t, loadSysTablePhysical)
		require.True(t, loadStatsPhysical)
	}
}

func TestRewriteKeyRangesUsesStorageCodec(t *testing.T) {
	preAlloced := [2]int64{11, 89}
	tableStart := tablecodec.EncodeTablePrefix(preAlloced[0])
	tableEnd := tablecodec.EncodeTablePrefix(preAlloced[1])
	legacyStart := codec.EncodeBytes(nil, tableStart)
	legacyEnd := codec.EncodeBytes(nil, tableEnd)

	ranges := rewriteKeyRanges(tikv.NewCodecV1(tikv.ModeTxn), preAlloced)
	require.Len(t, ranges, 1)
	require.Equal(t, legacyStart, []byte(ranges[0][0]))
	require.Equal(t, legacyEnd, []byte(ranges[0][1]))

	v2Codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Id:   1,
		Name: "user-keyspace",
	})
	require.NoError(t, err)

	ranges = rewriteKeyRanges(v2Codec, preAlloced)
	require.Len(t, ranges, 1)
	require.NotEqual(t, legacyStart, []byte(ranges[0][0]))
	require.NotEqual(t, legacyEnd, []byte(ranges[0][1]))

	decodedStart, decodedEnd, err := v2Codec.DecodeRegionRange(ranges[0][0], ranges[0][1])
	require.NoError(t, err)
	require.Equal(t, []byte(tableStart), decodedStart)
	require.Equal(t, []byte(tableEnd), decodedEnd)

	_, _, err = v2Codec.DecodeRegionRange(legacyStart, legacyEnd)
	require.Error(t, err)
	require.Nil(t, rewriteKeyRanges(v2Codec, [2]int64{}))
}
