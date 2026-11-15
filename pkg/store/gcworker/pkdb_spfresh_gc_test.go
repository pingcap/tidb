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

package gcworker

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	pd "github.com/tikv/pd/client"
)

type persistedServicePin struct {
	floor  uint64
	expiry int64
}

// This model retains PD's service records across client recreation and uses
// a controllable wall clock. GC execution below uses the real GCWorker and
// MockTiKV's physical MVCC GC handler, rather than a canned clamp result.
type persistentPinPD struct {
	pd.Client
	now            int64
	pins           map[string]persistedServicePin
	serviceFailure bool
	uploads        []uint64
}

func (p *persistentPinPD) UpdateServiceGCSafePoint(_ context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	if p.serviceFailure {
		return 0, errors.New("temporary PD service safepoint outage")
	}
	if ttl < 0 {
		delete(p.pins, serviceID)
	} else {
		expiry := int64(math.MaxInt64)
		if ttl < math.MaxInt64-p.now {
			expiry = p.now + ttl
		}
		p.pins[serviceID] = persistedServicePin{safePoint, expiry}
	}
	minimum := uint64(math.MaxUint64)
	for _, pin := range p.pins {
		if pin.expiry >= p.now && pin.floor < minimum {
			minimum = pin.floor
		}
	}
	return minimum, nil
}

func (*persistentPinPD) UpdateServiceSafePointV2(context.Context, uint32, string, int64, uint64) (uint64, error) {
	return 0, errors.New("API v1 must use global service safepoints")
}

func (p *persistentPinPD) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	p.uploads = append(p.uploads, safePoint)
	return p.Client.UpdateGCSafePoint(ctx, safePoint)
}

func physicalVersions(t *testing.T, store tikv.Storage, key []byte) int {
	bo := tikv.NewBackoffer(context.Background(), 5000)
	loc, err := store.GetRegionCache().LocateKey(bo, key)
	require.NoError(t, err)
	req := tikvrpc.NewRequest(tikvrpc.CmdMvccGetByKey, &kvrpcpb.MvccGetByKeyRequest{Key: key})
	resp, err := store.SendReq(bo, req, loc.Region, time.Minute)
	require.NoError(t, err)
	regionErr, err := resp.GetRegionError()
	require.NoError(t, err)
	require.Nil(t, regionErr)
	result := resp.Resp.(*kvrpcpb.MvccGetByKeyResponse)
	require.Empty(t, result.Error)
	return len(result.Info.Writes)
}

func TestSPFreshAPIV1PersistentPinProtectsPhysicalHistory(t *testing.T) {
	restore := config.RestoreFunc()
	t.Cleanup(restore)
	config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = "" })
	oldInterval := gcSafePointCacheInterval
	gcSafePointCacheInterval = 0
	t.Cleanup(func() { gcSafePointCacheInterval = oldInterval })
	for _, distributed := range []bool{false, true} {
		t.Run(map[bool]string{false: "physical", true: "distributed"}[distributed], func(t *testing.T) {
			s := createGCWorkerSuiteWithStoreType(t, mockstore.MockTiKV, config.DefSchemaLease)
			probe := s.createGCProbe(t, "spfresh-api-v1")
			candidate := s.mustAllocTs(t)
			pdClient := &persistentPinPD{Client: s.pdClient, pins: make(map[string]persistedServicePin)}
			_, err := pdClient.UpdateServiceGCSafePoint(gcContext(), "spfresh-checkpoint-api-v1", math.MaxInt64, probe.v1Ts)
			require.NoError(t, err)
			// Simulate all workers being offline for a year and the GC client restarting.
			pdClient = &persistentPinPD{Client: pdClient.Client, pins: pdClient.pins, now: 365 * 24 * 3600}
			s.gcWorker.pdClient = pdClient
			run := func() error {
				if distributed {
					return RunDistributedGCJob(gcContext(), s.gcWorker.regionLockResolver, s.tikvStore, pdClient, candidate, "spfresh-api-v1", 1)
				}
				return RunGCJob(gcContext(), s.gcWorker.regionLockResolver, s.tikvStore, pdClient, candidate, "spfresh-api-v1", 1)
			}
			require.NoError(t, run())
			require.Equal(t, probe.v1Ts, s.loadEtcdSafePoint(t))
			if distributed {
				require.Equal(t, []uint64{probe.v1Ts}, pdClient.uploads)
				require.NoError(t, s.gcWorker.doGC(gcContext(), probe.v1Ts, 1))
			}
			require.Equal(t, 2, physicalVersions(t, s.tikvStore, []byte(probe.key)))
			pdClient.serviceFailure = true // fail the model's service-pin operation
			require.Error(t, run())
			require.Equal(t, probe.v1Ts, s.loadEtcdSafePoint(t))
			pdClient.serviceFailure = false
			_, err = pdClient.UpdateServiceGCSafePoint(gcContext(), "spfresh-checkpoint-api-v1", -1, math.MaxUint64)
			require.NoError(t, err)
			require.NoError(t, run())
			if distributed {
				require.NoError(t, s.gcWorker.doGC(gcContext(), candidate, 1))
			}
			require.Equal(t, 1, physicalVersions(t, s.tikvStore, []byte(probe.key)))
		})
	}
}
