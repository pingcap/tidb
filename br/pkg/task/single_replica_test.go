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

package task

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

// fakeReplicaConfig keeps the replica count the way PD does. Like PD, it
// follows the default rule when placement rules are on, refuses a new
// max-replicas that differs from the rule, and takes nothing with a cancelled
// context.
type fakeReplicaConfig struct {
	replicas     int
	rulesEnabled bool
	ruleCount    int
	ruleStartKey string

	// failSetTo1 refuses to lower the count, lostSetTo1 lowers it and loses the
	// response. failSetBack refuses every other count, failSets the next ones.
	failSetTo1  bool
	lostSetTo1  bool
	failSetBack bool
	failSets    int

	// regions keep stuckPeers peers when it is set, or their old count for the
	// first slowPolls polls.
	stuckPeers       int
	slowPolls        int
	regionPolls      int
	onRegionPoll     func()
	peersBeforeLower int

	configWrites []int
	ruleWrites   []int
}

func (f *fakeReplicaConfig) count() int {
	if f.rulesEnabled {
		return f.ruleCount
	}
	return f.replicas
}

func (f *fakeReplicaConfig) GetReplicateConfig(ctx context.Context) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	enabled := "false"
	if f.rulesEnabled {
		enabled = "true"
	}
	return map[string]any{"max-replicas": float64(f.replicas), "enable-placement-rules": enabled}, nil
}

// set tells whether a new replica count is taken and what the caller gets.
func (f *fakeReplicaConfig) set(ctx context.Context, count int) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if count == 1 {
		if f.failSetTo1 {
			return false, errors.New("lowering rejected")
		}
		if f.peersBeforeLower == 0 {
			f.peersBeforeLower = f.count()
		}
		if f.lostSetTo1 {
			return true, errors.New("response lost")
		}
		return true, nil
	}
	if f.failSetBack {
		return false, errors.New("pd is down")
	}
	if f.failSets > 0 {
		f.failSets--
		return false, errors.New("pd is electing a leader")
	}
	return true, nil
}

func (f *fakeReplicaConfig) SetConfig(ctx context.Context, cfg map[string]any, _ ...float64) error {
	n := cfg["max-replicas"].(int)
	if f.rulesEnabled && n != f.replicas && (f.ruleCount != f.replicas || f.ruleStartKey != "") {
		return errors.New("the default rules do not consistent with replication config")
	}
	taken, err := f.set(ctx, n)
	if taken {
		f.replicas = n
		if f.rulesEnabled {
			f.ruleCount = n
		}
		f.configWrites = append(f.configWrites, n)
	}
	return err
}

func (f *fakeReplicaConfig) GetPlacementRule(ctx context.Context, group, id string) (*pdhttp.Rule, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &pdhttp.Rule{
		GroupID: group, ID: id, Role: pdhttp.Voter, Count: f.ruleCount,
		StartKeyHex: f.ruleStartKey, LocationLabels: []string{"zone", "host"},
	}, nil
}

func (f *fakeReplicaConfig) SetPlacementRule(ctx context.Context, r *pdhttp.Rule) error {
	taken, err := f.set(ctx, r.Count)
	if taken {
		f.ruleCount = r.Count
		f.ruleWrites = append(f.ruleWrites, r.Count)
	}
	return err
}

func (f *fakeReplicaConfig) GetRegions(ctx context.Context) (*pdhttp.RegionsInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.regionPolls++
	if f.onRegionPoll != nil {
		f.onRegionPoll()
	}
	voters := f.count()
	if f.stuckPeers > 0 {
		voters = f.stuckPeers
	} else if f.regionPolls <= f.slowPolls {
		voters = f.peersBeforeLower
	}
	// A learner is not a replica PD removes for the count.
	peers := []pdhttp.RegionPeer{{ID: 100, StoreID: 100, IsLearner: true}}
	for i := range voters {
		peers = append(peers, pdhttp.RegionPeer{ID: int64(i + 1), StoreID: int64(i + 1)})
	}
	return &pdhttp.RegionsInfo{Count: 2, Regions: []pdhttp.RegionInfo{{ID: 1, Peers: peers}, {ID: 2, Peers: peers}}}, nil
}

// fakeSingleReplicaMark keeps the mark as the JSON the real one writes.
type fakeSingleReplicaMark struct {
	data     []byte
	saves    int
	failSave bool
}

func (m *fakeSingleReplicaMark) load(context.Context) (*replicaSettings, error) {
	if m.data == nil {
		return nil, nil
	}
	r := &replicaSettings{}
	return r, json.Unmarshal(m.data, r)
}

func (m *fakeSingleReplicaMark) save(ctx context.Context, r *replicaSettings) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.failSave {
		return errors.New("etcd is down")
	}
	m.saves++
	m.data, err = json.Marshal(r)
	return err
}

func (m *fakeSingleReplicaMark) remove(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.data = nil
	return nil
}

// fastSingleReplica makes the waits of the single replica restore short.
func fastSingleReplica(t *testing.T) {
	resetTimeout, resetRetry := singleReplicaResetTimeout, singleReplicaResetRetryInterval
	shrinkTimeout, shrinkPoll := singleReplicaShrinkTimeout, singleReplicaShrinkPollInterval
	singleReplicaResetTimeout, singleReplicaResetRetryInterval = 50*time.Millisecond, time.Millisecond
	singleReplicaShrinkTimeout, singleReplicaShrinkPollInterval = 50*time.Millisecond, time.Millisecond
	t.Cleanup(func() {
		singleReplicaResetTimeout, singleReplicaResetRetryInterval = resetTimeout, resetRetry
		singleReplicaShrinkTimeout, singleReplicaShrinkPollInterval = shrinkTimeout, shrinkPoll
	})
}

type singleReplicaTest struct {
	*singleReplicaRestore
	pd   *fakeReplicaConfig
	mark *fakeSingleReplicaMark
	out  *bytes.Buffer
}

func newSingleReplicaTest(t *testing.T, pd *fakeReplicaConfig) *singleReplicaTest {
	fastSingleReplica(t)
	mark, out := &fakeSingleReplicaMark{}, &bytes.Buffer{}
	return &singleReplicaTest{
		singleReplicaRestore: &singleReplicaRestore{pd: pd, mark: mark, console: out},
		pd:                   pd, mark: mark, out: out,
	}
}

func TestSingleReplicaRestore(t *testing.T) {
	boom := errors.New("restore failed")
	for _, tc := range []struct {
		name string
		pd   *fakeReplicaConfig
		// failSave and cancelEnter make enter fail, afterEnter changes PD once the
		// replica count is lowered, restoreErr and cancelLeave are how the restore ends.
		failSave    bool
		cancelEnter bool
		afterEnter  func(*fakeReplicaConfig)
		restoreErr  error
		cancelLeave bool

		enterErr []string
		leaveErr string
		count    int   // the replica count in the end
		writes   []int // the replica counts PD took
		markKept bool
		polls    int
		out      []string
		notOut   string
	}{
		{name: "placement rules", pd: &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3},
			count: 3, writes: []int{1, 3},
			out: []string{"Restored with one replica of each region. The replica count is back to 3.", "no redundancy"}},
		{name: "max-replicas", pd: &fakeReplicaConfig{replicas: 5, ruleCount: 7}, count: 5, writes: []int{1, 5}},
		{name: "rule that differs from max-replicas", pd: &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 5},
			count: 5, writes: []int{1, 5}, out: []string{"The replica count is back to 5."}},
		{name: "failed and cancelled restore", pd: &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3},
			restoreErr: boom, cancelLeave: true, count: 3, writes: []int{1, 3},
			out: []string{"The restore failed. The replica count is back to 3."}},
		{name: "PD takes the count back after retries", pd: &fakeReplicaConfig{replicas: 3},
			afterEnter: func(pd *fakeReplicaConfig) { pd.failSets = 2 }, count: 3, writes: []int{1, 3}},
		{name: "PD does not take the count back", pd: &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3},
			afterEnter: func(pd *fakeReplicaConfig) { pd.failSetBack = true }, leaveErr: "pd is down",
			count: 1, writes: []int{1}, markKept: true,
			out: []string{"Set the count of the default placement rule to 3 in PD by hand"}},
		{name: "PD removes the other peers after a while",
			pd:    &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3, slowPolls: 3},
			count: 3, writes: []int{1, 3}, polls: 4, notOut: "PD still keeps"},
		{name: "PD keeps the other peers",
			pd:    &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3, stuckPeers: 3},
			count: 3, writes: []int{1, 3}, out: []string{"PD still keeps more than one replica of some regions"}},
		{name: "one replica already", pd: &fakeReplicaConfig{replicas: 1, rulesEnabled: true, ruleCount: 1},
			count: 1, out: []string{"--single-replica changed nothing"}, notOut: "no redundancy"},

		{name: "default rule of a key range",
			pd:       &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3, ruleStartKey: "7480"},
			enterErr: []string{"cover the whole key space"}, count: 3},
		{name: "mark not saved", pd: &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3},
			failSave: true, enterErr: []string{"etcd is down"}, count: 3},
		{name: "PD does not lower the count",
			pd:       &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3, failSetTo1: true},
			enterErr: []string{"lowering rejected"}, count: 3, writes: []int{3}},
		{name: "PD lowers the count and its response is lost",
			pd:       &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3, lostSetTo1: true},
			enterErr: []string{"response lost"}, count: 3, writes: []int{1, 3}},
		{name: "PD lowers the count and then is down",
			pd:       &fakeReplicaConfig{replicas: 3, lostSetTo1: true, failSetBack: true},
			enterErr: []string{"response lost", "pd is down"}, count: 1, writes: []int{1}, markKept: true,
			out: []string{"Set max-replicas to 3 in PD by hand"}},
		{name: "cancelled while PD removes the other peers",
			pd:          &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3, stuckPeers: 3},
			cancelEnter: true, enterErr: []string{context.Canceled.Error()}, count: 3, writes: []int{1, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pd, s := tc.pd, newSingleReplicaTest(t, tc.pd)
			s.mark.failSave = tc.failSave
			before := pd.count()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelEnter {
				pd.onRegionPoll = cancel
			}

			err := s.enter(ctx)
			for _, want := range tc.enterErr {
				require.ErrorContains(t, err, want)
			}
			if len(tc.enterErr) == 0 {
				require.NoError(t, err)
				require.Equal(t, 1, pd.count())
				require.Equal(t, before > 1, s.mark.data != nil)
				require.Equal(t, uint64(before), s.replicasAfterRestore())
				if tc.afterEnter != nil {
					tc.afterEnter(pd)
				}
				if tc.cancelLeave {
					cancel()
				}
				err = s.leave(ctx, tc.restoreErr)
				switch {
				case tc.restoreErr != nil:
					require.ErrorIs(t, err, tc.restoreErr)
				case tc.leaveErr != "":
					require.ErrorContains(t, err, tc.leaveErr)
				default:
					require.NoError(t, err)
				}
			}

			// With placement rules on only the rule is set: PD sets it itself when
			// max-replicas changes, and refuses the change when the two differ.
			writes, other := pd.configWrites, pd.ruleWrites
			if pd.rulesEnabled {
				writes, other = other, writes
			}
			require.Equal(t, tc.writes, writes)
			require.Empty(t, other)
			require.Equal(t, tc.count, pd.count())
			require.Equal(t, tc.markKept, s.mark.data != nil)
			if tc.polls > 0 {
				require.Equal(t, tc.polls, pd.regionPolls)
			}
			for _, want := range tc.out {
				require.Contains(t, s.out.String(), want)
			}
			if tc.notOut != "" {
				require.NotContains(t, s.out.String(), tc.notOut)
			}
		})
	}
}

// killedSingleReplica is what a single replica restore leaves when it dies
// before it puts the replica count back.
func killedSingleReplica(t *testing.T, pd *fakeReplicaConfig) *singleReplicaTest {
	s := newSingleReplicaTest(t, pd)
	require.NoError(t, s.enter(context.Background()))
	require.Equal(t, 1, pd.count())
	require.NotNil(t, s.mark.data)
	s.out.Reset()
	return s
}

func TestSingleReplicaRestoreNextRunPutsBackWhatAKilledRestoreLeft(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		pd   *fakeReplicaConfig
		// afterKill changes PD after the restore died.
		afterKill func(*fakeReplicaConfig)

		err      string
		count    int
		markKept bool
		out      string // nothing is said when it is empty
	}{
		{name: "placement rules", pd: &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3}, count: 3,
			out: "An interrupted restore with --single-replica left one replica of each region. " +
				"The replica count is back to 3."},
		{name: "max-replicas", pd: &fakeReplicaConfig{replicas: 3}, count: 3, out: "The replica count is back to 3."},
		{name: "count set by hand since", pd: &fakeReplicaConfig{replicas: 3},
			afterKill: func(pd *fakeReplicaConfig) { pd.replicas = 5 }, count: 5},
		{name: "PD does not take the count back", pd: &fakeReplicaConfig{replicas: 3},
			afterKill: func(pd *fakeReplicaConfig) { pd.failSetBack = true }, err: "pd is down",
			count: 1, markKept: true, out: "Set max-replicas to 3 in PD by hand"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := killedSingleReplica(t, tc.pd)
			if tc.afterKill != nil {
				tc.afterKill(tc.pd)
			}

			err := putBackInterruptedSingleReplicaRestore(ctx, tc.pd, s.mark, s.out)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.count, tc.pd.count())
			require.Equal(t, tc.markKept, s.mark.data != nil)
			require.Contains(t, s.out.String(), tc.out)
			require.Equal(t, tc.out == "", s.out.Len() == 0)
		})
	}

	t.Run("no restore was killed", func(t *testing.T) {
		pd, out := &fakeReplicaConfig{replicas: 1}, &bytes.Buffer{}
		require.NoError(t, putBackInterruptedSingleReplicaRestore(ctx, pd, &fakeSingleReplicaMark{}, out))
		require.Empty(t, pd.configWrites)
		require.Empty(t, out.String())
	})

	// The next restore with the flag must not take the count of one as the
	// count to put back.
	t.Run("next restore is a single replica one", func(t *testing.T) {
		pd := &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3}
		killed := killedSingleReplica(t, pd)
		next := &singleReplicaRestore{pd: pd, mark: killed.mark, console: killed.out}

		require.NoError(t, putBackInterruptedSingleReplicaRestore(ctx, pd, next.mark, next.console))
		require.NoError(t, next.enter(ctx))
		require.Equal(t, 1, pd.count())
		require.NoError(t, next.leave(ctx, nil))
		require.Equal(t, 3, pd.count())
		require.Nil(t, killed.mark.data)
		require.NotContains(t, killed.out.String(), "changed nothing")
	})
}

func TestSingleReplicaFlag(t *testing.T) {
	parse := func(args ...string) *RestoreConfig {
		flags := pflag.NewFlagSet("restore", pflag.ContinueOnError)
		DefineRestoreFlags(flags)
		require.NoError(t, flags.Parse(args))
		cfg := &RestoreConfig{}
		require.NoError(t, cfg.ParseFromFlags(flags, true))
		return cfg
	}
	require.False(t, parse().SingleReplica)

	cfg := parse("--" + FlagSingleReplica)
	require.True(t, cfg.SingleReplica)
	require.NoError(t, cfg.checkSingleReplica())
	cfg.KeyspaceName = "ks1"
	require.ErrorContains(t, cfg.checkSingleReplica(), "--single-replica is not supported with a keyspace")
	cfg.SingleReplica = false
	require.NoError(t, cfg.checkSingleReplica())
}
