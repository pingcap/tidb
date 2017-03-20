// Copyright 2016 PingCAP, Inc.
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

// +build !race

package tikv

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	mocktikv "github.com/pingcap/tidb/store/tikv/mock-tikv"
	"github.com/pingcap/tidb/util/codec"
	goctx "golang.org/x/net/context"
)

// The test takes too long under the race detector.
func (s *testCoprocessorSuite) TestBuildHugeTasks(c *C) {
	cluster := mocktikv.NewCluster()
	var splitKeys [][]byte
	for ch := byte('a'); ch <= byte('z'); ch++ {
		splitKeys = append(splitKeys, []byte{ch})
	}
	mocktikv.BootstrapWithMultiRegions(cluster, splitKeys...)

	bo := NewBackoffer(3000, goctx.Background())
	pdCli := &codecPDClient{mocktikv.NewPDClient(cluster)}
	cache := NewRegionCache(pdCli)

	const rangesPerRegion = 1e6
	ranges := make([]kv.KeyRange, 0, 26*rangesPerRegion)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		for i := 0; i < rangesPerRegion; i++ {
			start := make([]byte, 0, 9)
			end := make([]byte, 0, 9)
			ranges = append(ranges, kv.KeyRange{
				StartKey: codec.EncodeInt(append(start, ch), int64(i*2)),
				EndKey:   codec.EncodeInt(append(end, ch), int64(i*2+1)),
			})
		}
	}

	_, err := buildCopTasks(bo, cache, &copRanges{mid: ranges}, false)
	c.Assert(err, IsNil)
}
