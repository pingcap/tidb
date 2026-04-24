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

package ddl

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/pingcap/kvproto/pkg/logreplicationpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/constants"
	pdhttp "github.com/tikv/pd/client/http"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type scriptedStandbyInitPDHTTPClient struct {
	pdhttp.Client

	t *testing.T

	count int

	mu    sync.Mutex
	calls int
}

func (c *scriptedStandbyInitPDHTTPClient) GetRegionStatusByKeyRange(
	_ context.Context,
	keyRange *pdhttp.KeyRange,
	onlyCount bool,
) (*pdhttp.RegionStats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls++
	require.True(c.t, onlyCount)
	require.NotNil(c.t, keyRange)
	require.Nil(c.t, keyRange.StartKey)
	require.Nil(c.t, keyRange.EndKey)
	return &pdhttp.RegionStats{Count: c.count}, nil
}

func (c *scriptedStandbyInitPDHTTPClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

type scriptedStandbyInitEtcdPutter struct {
	clientv3.KV

	mu   sync.Mutex
	puts []struct {
		key string
		val string
	}
}

func (p *scriptedStandbyInitEtcdPutter) Put(
	_ context.Context,
	key, val string,
	_ ...clientv3.OpOption,
) (*clientv3.PutResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.puts = append(p.puts, struct {
		key string
		val string
	}{key: key, val: val})
	return &clientv3.PutResponse{}, nil
}

func (p *scriptedStandbyInitEtcdPutter) lastPut() (key, val string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.puts) == 0 {
		return "", ""
	}
	last := p.puts[len(p.puts)-1]
	return last.key, last.val
}

func TestStandbyInitProgressStateScanAndResume(t *testing.T) {
	pdHTTPCli := &scriptedStandbyInitPDHTTPClient{t: t, count: 4}
	etcd := &scriptedStandbyInitEtcdPutter{}
	tracker := newStandbyInitProgressTracker(pdHTTPCli, etcd)
	estRegionCnt, err := tracker.estimateTotalRegionCnt(context.Background())
	require.NoError(t, err)

	// First call: 2 continuous states, then a gap to infinity.
	s1 := newTestReplState(1, "", "b", 1, 1)
	s2 := newTestReplState(2, "b", "d", 1, 1)
	keys12, values12 := marshalReplStatesToScanKVs(t, []*logreplicationpb.LogReplicationState{s1, s2})
	kvCli1 := &scriptedRawKVScanClient{
		t: t,
		scanSteps: []rawKVScanStep{
			{startKey: nil, endKey: nil, keys: keys12, values: values12},
		},
	}
	finished, err := tracker.checkInitedAndUpdateProgress(context.Background(), kvCli1, estRegionCnt)
	require.NoError(t, err)
	require.False(t, finished)
	kvCli1.assertDone()

	require.Equal(t, 2, tracker.doneStateCnt)
	require.True(t, bytes.Equal([]byte("d"), tracker.resumeKey))
	key, val := etcd.lastPut()
	require.Equal(t, constants.PkdbInitPercentage, key)
	require.Equal(t, "50", val)

	// Second call: extend one more state and resume scanning from "d".
	s3 := newTestReplState(3, "d", "f", 1, 1)
	keys3, values3 := marshalReplStatesToScanKVs(t, []*logreplicationpb.LogReplicationState{s3})
	kvCli2 := &scriptedRawKVScanClient{
		t: t,
		scanSteps: []rawKVScanStep{
			{startKey: []byte("d"), endKey: nil, keys: keys3, values: values3},
		},
	}
	finished, err = tracker.checkInitedAndUpdateProgress(context.Background(), kvCli2, estRegionCnt)
	require.NoError(t, err)
	require.False(t, finished)
	kvCli2.assertDone()

	require.Equal(t, 3, tracker.doneStateCnt)
	require.True(t, bytes.Equal([]byte("f"), tracker.resumeKey))
	key, val = etcd.lastPut()
	require.Equal(t, constants.PkdbInitPercentage, key)
	require.Equal(t, "75", val)

	// Region count should be fetched only once.
	require.Equal(t, 1, pdHTTPCli.callCount())
}

func TestStandbyInitProgressReverseScanWhenMerged(t *testing.T) {
	pdHTTPCli := &scriptedStandbyInitPDHTTPClient{t: t, count: 10}
	etcd := &scriptedStandbyInitEtcdPutter{}
	tracker := newStandbyInitProgressTracker(pdHTTPCli, etcd)
	estRegionCnt, err := tracker.estimateTotalRegionCnt(context.Background())
	require.NoError(t, err)

	// Simulate resuming from a previous gap at "d".
	tracker.doneStateCnt = 2
	tracker.resumeKey = []byte("d")

	merge := newTestReplState(100, "c", "f", 1, 1)
	next := newTestReplState(101, "f", "g", 1, 1)
	mergeKeys, mergeValues := marshalReplStatesToScanKVs(t, []*logreplicationpb.LogReplicationState{merge})
	nextKeys, nextValues := marshalReplStatesToScanKVs(t, []*logreplicationpb.LogReplicationState{next})
	kvCli := &scriptedRawKVScanClient{
		t: t,
		reverseScanSteps: []rawKVReverseScanStep{
			{startKey: []byte("d"), endKey: nil, keys: mergeKeys, values: mergeValues},
		},
		scanSteps: []rawKVScanStep{
			{startKey: []byte("d"), endKey: nil, keys: nextKeys, values: nextValues},
		},
	}

	finished, err := tracker.checkInitedAndUpdateProgress(context.Background(), kvCli, estRegionCnt)
	require.NoError(t, err)
	require.False(t, finished)
	kvCli.assertDone()

	require.Equal(t, 4, tracker.doneStateCnt)
	require.True(t, bytes.Equal([]byte("g"), tracker.resumeKey))
	key, val := etcd.lastPut()
	require.Equal(t, constants.PkdbInitPercentage, key)
	require.Equal(t, "40", val)
}

func TestStandbyInitProgressClampToBelow100(t *testing.T) {
	pdHTTPCli := &scriptedStandbyInitPDHTTPClient{t: t, count: 2}
	etcd := &scriptedStandbyInitEtcdPutter{}
	tracker := newStandbyInitProgressTracker(pdHTTPCli, etcd)
	estRegionCnt, err := tracker.estimateTotalRegionCnt(context.Background())
	require.NoError(t, err)

	s1 := newTestReplState(1, "", "b", 1, 1)
	s2 := newTestReplState(2, "b", "d", 1, 1)
	s3 := newTestReplState(3, "d", "f", 1, 1)
	keys, values := marshalReplStatesToScanKVs(t, []*logreplicationpb.LogReplicationState{s1, s2, s3})
	kvCli := &scriptedRawKVScanClient{
		t: t,
		scanSteps: []rawKVScanStep{
			{startKey: nil, endKey: nil, keys: keys, values: values},
		},
	}

	finished, err := tracker.checkInitedAndUpdateProgress(context.Background(), kvCli, estRegionCnt)
	require.NoError(t, err)
	require.False(t, finished)
	kvCli.assertDone()

	key, val := etcd.lastPut()
	require.Equal(t, constants.PkdbInitPercentage, key)
	require.Equal(t, "75", val)
}

func TestStandbyInitProgressInitedWrites100(t *testing.T) {
	states := []*logreplicationpb.LogReplicationState{
		newTestReplState(1, "", "mid", 1, 1),
		newTestReplState(2, "mid", "", 1, 1),
	}
	keys, values := marshalReplStatesToScanKVs(t, states)
	kvCli := &scriptedRawKVScanClient{
		t: t,
		scanSteps: []rawKVScanStep{
			{startKey: nil, endKey: nil, keys: keys, values: values},
		},
	}

	pdHTTPCli := &scriptedStandbyInitPDHTTPClient{t: t, count: 42}
	etcd := &scriptedStandbyInitEtcdPutter{}
	tracker := newStandbyInitProgressTracker(pdHTTPCli, etcd)

	finished, err := tracker.checkInitedAndUpdateProgress(context.Background(), kvCli, 0)
	require.NoError(t, err)
	require.True(t, finished)
	kvCli.assertDone()

	key, val := etcd.lastPut()
	require.Equal(t, constants.PkdbInitPercentage, key)
	require.Equal(t, "100", val)

	// Should not talk to PD when already inited.
	require.Equal(t, 0, pdHTTPCli.callCount())
}

func TestStandbyInitCheckInitedReportsGapAtBeginning(t *testing.T) {
	// The first state starts after min-key, which should be treated as a gap from
	// min-key to that start key.
	state := newTestReplState(1, "a", "b", 1, 1)
	keys, values := marshalReplStatesToScanKVs(t, []*logreplicationpb.LogReplicationState{state})
	kvCli := &scriptedRawKVScanClient{
		t: t,
		scanSteps: []rawKVScanStep{
			{startKey: nil, endKey: nil, keys: keys, values: values},
		},
	}

	allDone, advanced, resumeKey, err := checkInited(context.Background(), kvCli, nil)
	require.NoError(t, err)
	require.False(t, allDone)
	require.Equal(t, 0, advanced)
	require.Len(t, resumeKey, 0)
	kvCli.assertDone()
}

func TestStandbyInitCheckInitedStopsAtInternalGap(t *testing.T) {
	// Two states with an internal hole: ["", "b") and ["c", "d"), so ["b", "c")
	// is a gap.
	s1 := newTestReplState(1, "", "b", 1, 1)
	s2 := newTestReplState(2, "c", "d", 1, 1)
	keys, values := marshalReplStatesToScanKVs(t, []*logreplicationpb.LogReplicationState{s1, s2})
	kvCli := &scriptedRawKVScanClient{
		t: t,
		scanSteps: []rawKVScanStep{
			{startKey: nil, endKey: nil, keys: keys, values: values},
		},
	}

	allDone, advanced, resumeKey, err := checkInited(context.Background(), kvCli, nil)
	require.NoError(t, err)
	require.False(t, allDone)
	require.Equal(t, 1, advanced)
	require.True(t, bytes.Equal([]byte("b"), resumeKey))
	kvCli.assertDone()
}

func TestStandbyInitCheckInitedReverseScanEmptyReportsGap(t *testing.T) {
	// Resume from a previously returned gap, but the exact resumeKey no longer
	// exists and there is no previous state covering it.
	next := newTestReplState(101, "f", "g", 1, 1)
	nextKeys, nextValues := marshalReplStatesToScanKVs(t, []*logreplicationpb.LogReplicationState{next})
	kvCli := &scriptedRawKVScanClient{
		t: t,
		reverseScanSteps: []rawKVReverseScanStep{
			{startKey: []byte("d"), endKey: nil},
		},
		scanSteps: []rawKVScanStep{
			{startKey: []byte("d"), endKey: nil, keys: nextKeys, values: nextValues},
		},
	}

	allDone, advanced, resumeKey, err := checkInited(context.Background(), kvCli, []byte("d"))
	require.NoError(t, err)
	require.False(t, allDone)
	require.Equal(t, 0, advanced)
	require.True(t, bytes.Equal([]byte("d"), resumeKey))
	kvCli.assertDone()
}

func TestStandbyInitCheckInitedReverseScanNotCoveringReportsGap(t *testing.T) {
	// Reverse-scan returns a previous state, but it ends exactly at resumeKey and
	// therefore does not cover resumeKey. This should still be treated as a gap.
	prev := newTestReplState(100, "c", "d", 1, 1)
	next := newTestReplState(101, "f", "g", 1, 1)
	prevKeys, prevValues := marshalReplStatesToScanKVs(t, []*logreplicationpb.LogReplicationState{prev})
	nextKeys, nextValues := marshalReplStatesToScanKVs(t, []*logreplicationpb.LogReplicationState{next})
	kvCli := &scriptedRawKVScanClient{
		t: t,
		reverseScanSteps: []rawKVReverseScanStep{
			{startKey: []byte("d"), endKey: nil, keys: prevKeys, values: prevValues},
		},
		scanSteps: []rawKVScanStep{
			{startKey: []byte("d"), endKey: nil, keys: nextKeys, values: nextValues},
		},
	}

	allDone, advanced, resumeKey, err := checkInited(context.Background(), kvCli, []byte("d"))
	require.NoError(t, err)
	require.False(t, allDone)
	require.Equal(t, 0, advanced)
	require.True(t, bytes.Equal([]byte("d"), resumeKey))
	kvCli.assertDone()
}

func TestStandbyInitCheckInitedReverseScanCoveringReportsGap(t *testing.T) {
	// Reverse-scan returns a previous state, but it covers resumeKey but still has a
	// gap.
	prev := newTestReplState(100, "c", "e", 1, 1)
	next := newTestReplState(101, "f", "g", 1, 1)
	prevKeys, prevValues := marshalReplStatesToScanKVs(t, []*logreplicationpb.LogReplicationState{prev})
	nextKeys, nextValues := marshalReplStatesToScanKVs(t, []*logreplicationpb.LogReplicationState{next})
	kvCli := &scriptedRawKVScanClient{
		t: t,
		reverseScanSteps: []rawKVReverseScanStep{
			{startKey: []byte("d"), endKey: nil, keys: prevKeys, values: prevValues},
		},
		scanSteps: []rawKVScanStep{
			{startKey: []byte("d"), endKey: nil, keys: nextKeys, values: nextValues},
		},
	}

	allDone, advanced, resumeKey, err := checkInited(context.Background(), kvCli, []byte("d"))
	require.NoError(t, err)
	require.False(t, allDone)
	require.Equal(t, 1, advanced)
	require.True(t, bytes.Equal([]byte("e"), resumeKey))
	kvCli.assertDone()
}
