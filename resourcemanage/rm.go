// Copyright 2022 PingCAP, Inc.
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

package resourcemanage

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/cpu"
)

// GlobalReourceManage is a global resource manage
var GlobalReourceManage ResourceManage = NewResourceMange()

// ResourceManage is a resource manage
type ResourceManage struct {
	poolMap map[string]*PoolContainer

	cpuObserver cpu.Observer
	exitCh      chan struct{}
}

// NewResourceMange is to create a new resource manage
func NewResourceMange() ResourceManage {
	return ResourceManage{
		poolMap: make(map[string]*PoolContainer),
	}
}

func (r *ResourceManage) Start() {
	go r.cpuObserver.Start()

	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
		case <-r.exitCh:
			return
		}
	}
}

func (r *ResourceManage) Stop() {
	r.cpuObserver.Stop()
	close(r.exitCh)
}

// Register is to register pool into resource manage
func (r *ResourceManage) Register(pool GorotinuePool, name string, _ TaskPriority, component Component) error {
	p := PoolContainer{Pool: pool, component: component}
	return r.registerPool(name, &p)

}

func (r *ResourceManage) registerPool(name string, pool *PoolContainer) error {
	if _, contain := r.poolMap[name]; contain {
		return errors.New("pool name is already exist")
	}
	r.poolMap[name] = pool
	return nil
}

// GorotinuePool is a pool interface
type GorotinuePool interface {
	Release()

	Tune(size int)
	LastTunerTs() time.Time
	MaxInFlight() int64
	InFlight() int64
	MinRT() uint64
	MaxPASS() uint64
	Cap() int
	LongRTT() float64
	ShortRTT() uint64
	GetQueueSize() int64
	Running() int
}

// PoolContainer is a pool container
type PoolContainer struct {
	Pool      GorotinuePool
	component Component
}

// TaskPriority is the priority of the task.
type TaskPriority int

const (
	HighPriority TaskPriority = iota
	NormalPriority
	LowPriority
)

// Component is ID for difference component
type Component int

const (
	UNKNOWN Component = iota // it is only for test
	DDL
)
