// Copyright 2024 PingCAP, Inc.
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

package gpool

import (
	"runtime"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/pingcap/tidb/pkg/util/cpu"
	_ "github.com/tiancaiamao/gp"
)

// PoolCapacityPerCore is the goroutine pool capacity per core
const PoolCapacityPerCore = 4000

// PoolCPUThreshold is the threshold of enabling goroutine pool.
// When the CPU utilization is low, though gp.Pool saves CPU resource, but it also introduces a little latency.
// To avoid the latency regression in light workloads, we only enable gp.Pool when the CPU utilization is high.
const PoolCPUThreshold = 0.8

// PoolRecycleInterval is the idle time before recycling a goroutine
const PoolRecycleInterval = time.Minute

// GPool wraps gp.Pool.
type GPool struct {
	gp *ants.Pool
}

type AntPool struct {
	*ants.Pool
}

func (p *AntPool) Go(fn func()) {
	p.Submit(fn)
}

func (p *AntPool) Close() {
	p.Release()
}

// NewGPool creates a new goroutine pool.
func NewGPool() Pool {
	_, supportCPUUsage := cpu.GetCPUUsage()
	//gpool := gp.New(PoolCapacityPerCore*runtime.NumCPU(), PoolRecycleInterval)
	gpool, err := ants.NewPool(PoolCapacityPerCore*runtime.NumCPU(), ants.WithExpiryDuration(PoolRecycleInterval))
	if err != nil {
		panic(err)
	}
	if !supportCPUUsage {
		return &AntPool{gpool}
	}
	return &GPool{gp: gpool}
}

// Go will call gp.Pool when CPU utilization exceeds PoolCPUThreshold, otherwise, it simply creates new goroutine.
func (p *GPool) Go(fn func()) {
	if usage, support := cpu.GetCPUUsage(); !support || usage < PoolCPUThreshold {
		go fn()
		return
	}
	p.gp.Submit(fn)
}

// Close closes the goroutine pool.
func (p *GPool) Close() {
	p.gp.Release()
}

// Pool is a simple interface for global goroutine pool.
// note it may return error when the pool is full.
type Pool interface {
	Go(func())
	Close()
}

type mockGPool struct{}

// MockGPool is a mock implementation of Pool, it actually opens new goroutine instead of using pool.
var MockGPool = &mockGPool{}

func (*mockGPool) Go(fn func()) {
	go fn()
}

func (*mockGPool) Close() {}

type tikvGPool struct {
	Pool
}

// NewTikvGPool wraps a goroutine pool into kv client interface.
func NewTikvGPool(p Pool) *tikvGPool {
	return &tikvGPool{p}
}

func (t *tikvGPool) Run(fn func()) error {
	t.Pool.Go(fn)
	return nil
}

func (*tikvGPool) Close() {
	// the global pool cannot be closed, so we ignore the Close call from kv client.
}
