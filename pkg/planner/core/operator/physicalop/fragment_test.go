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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package physicalop

import (
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestFragmentInitSingleton(t *testing.T) {
	r1, r2 := &PhysicalExchangeReceiver{}, &PhysicalExchangeReceiver{}
	r1.SetChildren(&PhysicalExchangeSender{ExchangeType: tipb.ExchangeType_PassThrough})
	r2.SetChildren(&PhysicalExchangeSender{ExchangeType: tipb.ExchangeType_Broadcast})
	p := &PhysicalHashJoin{}

	f := &Fragment{}
	p.SetChildren(r1, r1)
	err := f.init(p)
	require.NoError(t, err)
	require.Equal(t, f.singleton, true)

	f = &Fragment{}
	p.SetChildren(r1, r2)
	err = f.init(p)
	require.NoError(t, err)
	require.Equal(t, f.singleton, true)

	f = &Fragment{}
	p.SetChildren(r2, r1)
	err = f.init(p)
	require.NoError(t, err)
	require.Equal(t, f.singleton, true)

	f = &Fragment{}
	p.SetChildren(r2, r2)
	err = f.init(p)
	require.NoError(t, err)
	require.Equal(t, f.singleton, false)
}

func TestFillLocalCTECountsUsesLocalTaskCounts(t *testing.T) {
	task := func(addr string) *kv.MPPTask {
		return &kv.MPPTask{Meta: &mppAddr{addr: addr}}
	}

	// This models a CTE producer with UNION ALL split into two CTESink fragments:
	// one sink runs on tiflash0, the other runs on tiflash1, and both CTE consumers
	// run on both TiFlash nodes. Each TiFlash address should see one local sink
	// and two local sources for the CTE.
	sink0 := &PhysicalCTESink{IDForStorage: 1}
	sink0.SetSelfTasks([]*kv.MPPTask{task("tiflash0")})
	sink1 := &PhysicalCTESink{IDForStorage: 1}
	sink1.SetSelfTasks([]*kv.MPPTask{task("tiflash1")})

	source0 := &PhysicalCTESource{IDForStorage: 1}
	sourceSink0 := &PhysicalExchangeSender{}
	sourceSink0.SetChildren(source0)
	sourceSink0.SetSelfTasks([]*kv.MPPTask{task("tiflash0"), task("tiflash1")})
	source1 := &PhysicalCTESource{IDForStorage: 1}
	sourceSink1 := &PhysicalExchangeSender{}
	sourceSink1.SetChildren(source1)
	sourceSink1.SetSelfTasks([]*kv.MPPTask{task("tiflash0"), task("tiflash1")})

	frags := []*Fragment{
		{Sink: sink0},
		{Sink: sink1},
		{Sink: sourceSink0},
		{Sink: sourceSink1},
	}
	err := (&mppTaskGenerator{}).fillLocalCTECounts(frags)
	require.NoError(t, err)

	for _, sink := range []*PhysicalCTESink{sink0, sink1} {
		require.Equal(t, uint32(1), sink.CteSinkNum)
		require.Equal(t, uint32(2), sink.CteSourceNum)
	}
	for _, source := range []*PhysicalCTESource{source0, source1} {
		require.Equal(t, uint32(1), source.CteSinkNum)
		require.Equal(t, uint32(2), source.CteSourceNum)
	}
}
