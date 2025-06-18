// Copyright 2025 PingCAP, Inc.
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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pdgc "github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/client/constants"
)

func TestMockGCStatesManager(t *testing.T) {
	re := require.New(t)
	m := newGCStatesManager()
	ctx := context.Background()

	for _, keyspaceID := range []uint32{constants.NullKeyspaceID, 0, 1} {
		ctl := m.GetGCInternalController(keyspaceID)
		cli := m.GetGCStatesClient(keyspaceID)

		s, err := cli.GetGCState(ctx)
		re.NoError(err)
		re.Equal(uint64(0), s.GCSafePoint)
		re.Equal(uint64(0), s.TxnSafePoint)
		re.Empty(s.GCBarriers)

		tspRes, err := ctl.AdvanceTxnSafePoint(ctx, 10)
		re.NoError(err)
		re.Equal(uint64(10), tspRes.NewTxnSafePoint)
		re.Equal(uint64(10), tspRes.Target)
		re.Equal(uint64(0), tspRes.OldTxnSafePoint)
		re.Empty(tspRes.BlockerDescription)

		gspRes, err := ctl.AdvanceGCSafePoint(ctx, 9)
		re.NoError(err)
		re.Equal(uint64(9), gspRes.NewGCSafePoint)
		re.Equal(uint64(9), gspRes.Target)
		re.Equal(uint64(0), gspRes.OldGCSafePoint)

		// No decreasing
		_, err = ctl.AdvanceTxnSafePoint(ctx, 9)
		re.Error(err)
		_, err = ctl.AdvanceGCSafePoint(ctx, 8)
		re.Error(err)
		_, err = ctl.AdvanceGCSafePoint(ctx, 11)
		re.Error(err)

		s, err = cli.GetGCState(ctx)
		re.NoError(err)
		re.Equal(uint64(9), s.GCSafePoint)
		re.Equal(uint64(10), s.TxnSafePoint)
		re.Empty(s.GCBarriers)

		// Invalid arguments
		_, err = cli.SetGCBarrier(ctx, "", 20, pdgc.TTLNeverExpire)
		re.Error(err)
		_, err = cli.SetGCBarrier(ctx, "b1", 0, pdgc.TTLNeverExpire)
		re.Error(err)
		_, err = cli.SetGCBarrier(ctx, "b1", 20, 0)
		re.Error(err)

		// Disallow being less than txn safe point
		_, err = cli.SetGCBarrier(ctx, "b1", 9, pdgc.TTLNeverExpire)
		re.Error(err)

		b, err := cli.SetGCBarrier(ctx, "b2", 25, pdgc.TTLNeverExpire)
		re.NoError(err)
		re.Equal("b2", b.BarrierID)
		re.Equal(uint64(25), b.BarrierTS)

		s, err = cli.GetGCState(ctx)
		re.NoError(err)
		re.Equal(uint64(9), s.GCSafePoint)
		re.Equal(uint64(10), s.TxnSafePoint)
		re.Len(s.GCBarriers, 1)
		re.Equal("b2", s.GCBarriers[0].BarrierID)
		re.Equal(uint64(25), s.GCBarriers[0].BarrierTS)

		tspRes, err = ctl.AdvanceTxnSafePoint(ctx, 30)
		re.NoError(err)
		re.Equal(uint64(25), tspRes.NewTxnSafePoint)
		re.Equal(uint64(30), tspRes.Target)
		re.Equal(uint64(10), tspRes.OldTxnSafePoint)
		re.Contains(tspRes.BlockerDescription, "b2")

		s, err = cli.GetGCState(ctx)
		re.NoError(err)
		re.Equal(uint64(25), s.TxnSafePoint)

		b, err = cli.DeleteGCBarrier(ctx, "b2")
		re.NoError(err)
		re.Equal("b2", b.BarrierID)
		re.Equal(uint64(25), b.BarrierTS)

		tspRes, err = ctl.AdvanceTxnSafePoint(ctx, 30)
		re.NoError(err)
		re.Equal(uint64(30), tspRes.NewTxnSafePoint)
		re.Equal(uint64(30), tspRes.Target)
		re.Equal(uint64(25), tspRes.OldTxnSafePoint)
		re.Empty(tspRes.BlockerDescription)

		s, err = cli.GetGCState(ctx)
		re.NoError(err)
		re.Equal(uint64(30), s.TxnSafePoint)
		re.Equal(uint64(9), s.GCSafePoint)
		re.Empty(s.GCBarriers)
	}
}
