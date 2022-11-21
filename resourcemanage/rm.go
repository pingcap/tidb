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
	"github.com/pingcap/tidb/resourcemanage/limiter"
	"github.com/pingcap/tidb/resourcemanage/scheduler"
	"github.com/pingcap/tidb/resourcemanage/util"
	"github.com/pingcap/tidb/util/cpu"
)

// ResourceManage is a resource manage
type ResourceManage struct {
	poolMap map[string]*util.PoolContainer

	limiter     []limiter.Limiter
	scheduler   []scheduler.Scheduler
	cpuObserver cpu.Observer
	exitCh      chan struct{}
}

// NewResourceMange is to create a new resource manage
func NewResourceMange() *ResourceManage {
	li := make([]limiter.Limiter, 0, 1)
	li = append(li, limiter.NewBBRLimiter(80))
	sc := make([]scheduler.Scheduler, 0, 1)
	sc = append(sc, scheduler.NewGradient2Scheduler())
	return &ResourceManage{
		poolMap:   make(map[string]*util.PoolContainer),
		limiter:   li,
		scheduler: sc,
	}
}

func (r *ResourceManage) Start() {
	go r.cpuObserver.Start()

	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			r.schedule()
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
func (r *ResourceManage) Register(pool util.GorotinuePool, name string, _ util.TaskPriority, component util.Component) error {
	p := util.PoolContainer{Pool: pool, Component: component}
	return r.registerPool(name, &p)

}

func (r *ResourceManage) registerPool(name string, pool *util.PoolContainer) error {
	if _, contain := r.poolMap[name]; contain {
		return errors.New("pool name is already exist")
	}
	r.poolMap[name] = pool
	return nil
}
