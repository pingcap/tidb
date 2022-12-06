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

	"github.com/pingcap/tidb/resourcemanager/scheduler"
	"github.com/pingcap/tidb/resourcemanager/util"
	tidbutil "github.com/pingcap/tidb/util"
)

// GlobalResourceManage is a global resource manage
var GlobalResourceManage = NewResourceMange()

// ResourceManage is a resource manage
type ResourceManage struct {
	poolMap   *util.ShardPoolMap
	scheduler []scheduler.Scheduler
	exitCh    chan struct{}
	wg        tidbutil.WaitGroupWrapper
}

// NewResourceMange is to create a new resource manage
func NewResourceMange() *ResourceManage {
	sc := make([]scheduler.Scheduler, 0, 1)
	sc = append(sc, scheduler.NewGradient2Scheduler())
	return &ResourceManage{
		poolMap:   util.NewShardPoolMap(),
		exitCh:    make(chan struct{}),
		scheduler: sc,
	}
}

// Start is to start resource manage
func (r *ResourceManage) Start() {
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
func (r *ResourceManage) Stop() {
	close(r.exitCh)
	r.wg.Wait()
}

// Register is to register pool into resource manage
func (r *ResourceManage) Register(pool util.GorotinuePool, name string, component util.Component) error {
	p := util.PoolContainer{Pool: pool, Component: component}
	return r.registerPool(name, &p)
}

func (r *ResourceManage) registerPool(name string, pool *util.PoolContainer) error {
	return r.poolMap.Add(name, pool)
}
