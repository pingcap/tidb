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

package resourcemanager

import (
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/resourcemanager/scheduler"
	"github.com/pingcap/tidb/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/cpu"
)

// InstanceResourceManager is a local instance resource manager
var InstanceResourceManager = NewResourceManger()

// RandomName is to get a random name for register pool. It is just for test.
func RandomName() string {
	return uuid.New().String()
}

// ResourceManager is a resource manager
type ResourceManager struct {
	poolMap     *util.ShardPoolMap
	scheduler   []scheduler.Scheduler
	cpuObserver *cpu.Observer
	exitCh      chan struct{}
	wg          tidbutil.WaitGroupWrapper
}

// NewResourceManger is to create a new resource manager
func NewResourceManger() *ResourceManager {
	sc := make([]scheduler.Scheduler, 0, 1)
	sc = append(sc, scheduler.NewCPUScheduler())
	return &ResourceManager{
		cpuObserver: cpu.NewCPUObserver(),
		exitCh:      make(chan struct{}),
		poolMap:     util.NewShardPoolMap(),
		scheduler:   sc,
	}
}

// Start is to start resource manager
func (r *ResourceManager) Start() {
	r.wg.Run(r.cpuObserver.Start)
	r.wg.Run(func() {
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
	})
}

// Stop is to stop resource manager
func (r *ResourceManager) Stop() {
	r.cpuObserver.Stop()
	close(r.exitCh)
	r.wg.Wait()
}

// Register is to register pool into resource manager
func (r *ResourceManager) Register(pool util.GoroutinePool, name string, component util.Component) error {
	p := util.PoolContainer{Pool: pool, Component: component}
	return r.registerPool(name, &p)
}

func (r *ResourceManager) registerPool(name string, pool *util.PoolContainer) error {
	return r.poolMap.Add(name, pool)
}

// Unregister is to unregister pool into resource manager.
func (r *ResourceManager) Unregister(name string) {
	r.poolMap.Del(name)
}

// Reset is to Reset resource manager. it is just for test.
func (r *ResourceManager) Reset() {
	r.poolMap = util.NewShardPoolMap()
}
