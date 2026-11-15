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

package executor

import (
	"encoding/binary"
	"hash/fnv"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/tikvapi"
)

const (
	spfreshEndpointMinFailureCooldown = time.Second
	spfreshEndpointMaxFailureCooldown = 8 * time.Second
)

type spfreshEndpointState struct {
	endpoint string

	searchInflight atomic.Int64
	cooldownUntil  atomic.Int64
	failures       atomic.Int64
}

func (s *spfreshEndpointState) healthy(now time.Time) bool {
	return s.cooldownUntil.Load() <= now.UnixNano()
}

func (s *spfreshEndpointState) markSuccess() {
	s.failures.Store(0)
	s.cooldownUntil.Store(0)
}

func (s *spfreshEndpointState) markFailure(now time.Time) {
	failures := s.failures.Add(1)
	cooldown := spfreshEndpointFailureCooldown(s.endpoint, failures)
	s.cooldownUntil.Store(now.Add(cooldown).UnixNano())
}

func spfreshEndpointFailureCooldown(endpoint string, failures int64) time.Duration {
	if failures < 1 {
		failures = 1
	}
	shift := failures - 1
	if shift > 3 {
		shift = 3
	}
	base := spfreshEndpointMinFailureCooldown << shift
	if base > spfreshEndpointMaxFailureCooldown {
		base = spfreshEndpointMaxFailureCooldown
	}
	jitter := base / 5
	if jitter <= 0 {
		return base
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(endpoint))
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(failures))
	_, _ = h.Write(buf[:])
	offset := time.Duration(int64(h.Sum64()%uint64(2*jitter+1)) - int64(jitter))
	cooldown := base + offset
	if cooldown < spfreshEndpointMinFailureCooldown {
		return spfreshEndpointMinFailureCooldown
	}
	if cooldown > spfreshEndpointMaxFailureCooldown {
		return spfreshEndpointMaxFailureCooldown
	}
	return cooldown
}

type spfreshEndpointRouter struct {
	mu       sync.Mutex
	snapshot atomic.Pointer[spfreshEndpointSnapshot]
	states   map[string]*spfreshEndpointState
	searchID atomic.Uint64
	statusID atomic.Uint64
}

type spfreshEndpointSnapshot struct {
	key        string
	configured []string
	ordered    []*spfreshEndpointState
}

var spfreshRouter = &spfreshEndpointRouter{}

func (r *spfreshEndpointRouter) configuredEndpointStates() ([]*spfreshEndpointState, error) {
	configured := config.GetGlobalConfig().TiKVAPIServiceAddrs
	if snapshot := r.snapshot.Load(); snapshot != nil && slices.Equal(snapshot.configured, configured) {
		return snapshot.ordered, nil
	}
	endpoints, err := tikvapi.NormalizeEndpoints(configured, true)
	if err != nil {
		return nil, err
	}
	key := strings.Join(endpoints, "\x00")
	return r.reconcile(key, configured, endpoints), nil
}

func (r *spfreshEndpointRouter) reconcile(key string, configured, endpoints []string) []*spfreshEndpointState {
	r.mu.Lock()
	defer r.mu.Unlock()

	if snapshot := r.snapshot.Load(); snapshot != nil && snapshot.key == key {
		if !slices.Equal(snapshot.configured, configured) {
			r.snapshot.Store(&spfreshEndpointSnapshot{
				key:        key,
				configured: append([]string(nil), configured...),
				ordered:    snapshot.ordered,
			})
		}
		return snapshot.ordered
	}

	nextStates := make(map[string]*spfreshEndpointState, len(endpoints))
	ordered := make([]*spfreshEndpointState, 0, len(endpoints))
	for _, endpoint := range endpoints {
		state := r.states[endpoint]
		if state == nil {
			state = &spfreshEndpointState{endpoint: endpoint}
		}
		nextStates[endpoint] = state
		ordered = append(ordered, state)
	}
	r.states = nextStates
	r.snapshot.Store(&spfreshEndpointSnapshot{
		key:        key,
		configured: append([]string(nil), configured...),
		ordered:    ordered,
	})
	return ordered
}

func (r *spfreshEndpointRouter) searchEndpointOrder() ([]*spfreshEndpointState, error) {
	states, err := r.configuredEndpointStates()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	candidates := spfreshEndpointCandidates(states, now)
	firstIdx := r.chooseSearchEndpointIndex(candidates)
	if len(candidates) <= 1 {
		return candidates, nil
	}

	ordered := make([]*spfreshEndpointState, 0, len(candidates))
	for offset := 0; offset < len(candidates); offset++ {
		ordered = append(ordered, candidates[(firstIdx+offset)%len(candidates)])
	}
	return ordered, nil
}

func (r *spfreshEndpointRouter) chooseSearchEndpointIndex(candidates []*spfreshEndpointState) int {
	if len(candidates) == 1 {
		return 0
	}

	salt := r.searchID.Add(1) - 1
	firstIdx := int(salt % uint64(len(candidates)))
	secondIdx := int(splitmix64(salt+0x9e3779b97f4a7c15) % uint64(len(candidates)-1))
	if secondIdx >= firstIdx {
		secondIdx++
	}
	first := candidates[firstIdx]
	second := candidates[secondIdx]

	firstInflight := first.searchInflight.Load()
	secondInflight := second.searchInflight.Load()
	if firstInflight < secondInflight {
		return firstIdx
	}
	if secondInflight < firstInflight {
		return secondIdx
	}
	return firstIdx
}

func (r *spfreshEndpointRouter) statusEndpointOrder() ([]*spfreshEndpointState, error) {
	states, err := r.configuredEndpointStates()
	if err != nil {
		return nil, err
	}
	candidates := healthySPFreshEndpointStates(states, time.Now())
	if len(candidates) == 0 {
		candidates = states
	}
	if len(candidates) <= 1 {
		return candidates, nil
	}

	start := int((r.statusID.Add(1) - 1) % uint64(len(candidates)))
	ordered := make([]*spfreshEndpointState, 0, len(candidates))
	ordered = append(ordered, candidates[start:]...)
	ordered = append(ordered, candidates[:start]...)
	return ordered, nil
}

func healthySPFreshEndpointStates(states []*spfreshEndpointState, now time.Time) []*spfreshEndpointState {
	healthy := make([]*spfreshEndpointState, 0, len(states))
	for _, state := range states {
		if state.healthy(now) {
			healthy = append(healthy, state)
		}
	}
	return healthy
}

func spfreshEndpointCandidates(states []*spfreshEndpointState, now time.Time) []*spfreshEndpointState {
	for i, state := range states {
		if state.healthy(now) {
			continue
		}
		candidates := make([]*spfreshEndpointState, 0, len(states)-1)
		candidates = append(candidates, states[:i]...)
		for _, state := range states[i+1:] {
			if state.healthy(now) {
				candidates = append(candidates, state)
			}
		}
		if len(candidates) == 0 {
			return states
		}
		return candidates
	}
	return states
}

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
