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
	highPriorityPoolMap   map[string]*PoolContainer
	normalPriorityPoolMap map[string]*PoolContainer
	lowPriorityPoolMap    map[string]*PoolContainer

	cpuObserver cpu.Observer
	exitCh      chan struct{}
}

// NewResourceMange is to create a new resource manage
func NewResourceMange() ResourceManage {
	return ResourceManage{
		highPriorityPoolMap:   make(map[string]*PoolContainer),
		normalPriorityPoolMap: make(map[string]*PoolContainer),
		lowPriorityPoolMap:    make(map[string]*PoolContainer),
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
func (r *ResourceManage) Register(pool GorotinuePool, name string, priority TaskPriority, component Component) error {
	p := PoolContainer{pool: pool, component: component}
	switch priority {
	case HighPriority:
		return r.registerHighPriorityPool(name, &p)
	case NormalPriority:
		return r.registerNormalPriorityPool(name, &p)
	case LowPriority:
		return r.registerLowPriorityPool(name, &p)
	default:
		return errors.New("priority is not valid")
	}
}

func (r *ResourceManage) registerHighPriorityPool(name string, pool *PoolContainer) error {
	if _, contain := r.highPriorityPoolMap[name]; contain {
		return errors.New("pool name is already exist")
	}
	r.highPriorityPoolMap[name] = pool
	return nil
}

func (r *ResourceManage) registerNormalPriorityPool(name string, pool *PoolContainer) error {
	if _, contain := r.normalPriorityPoolMap[name]; contain {
		return errors.New("pool name is already exist")
	}
	r.normalPriorityPoolMap[name] = pool
	return nil
}

func (r *ResourceManage) registerLowPriorityPool(name string, pool *PoolContainer) error {
	if _, contain := r.lowPriorityPoolMap[name]; contain {
		return errors.New("pool name is already exist")
	}
	r.lowPriorityPoolMap[name] = pool
	return nil
}

// GorotinuePool is a pool interface
type GorotinuePool interface {
	Release()

	Tune(size int)
	Cap() int
}

// PoolContainer is a pool container
type PoolContainer struct {
	pool      GorotinuePool
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
