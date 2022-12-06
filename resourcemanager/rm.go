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

	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/cpu"
	"github.com/pingcap/tidb/util/memory"
)

// GlobalResourceManager is a global resource manager
var GlobalResourceManager = NewResourceManger()

// ResourceManager is a resource manager
type ResourceManager struct {
	cpuObserver *cpu.Observer
	exitCh      chan struct{}
	wg          tidbutil.WaitGroupWrapper
}

// NewResourceManger is to create a new resource manager
func NewResourceManger() *ResourceManager {
	return &ResourceManager{
		cpuObserver: cpu.NewCPUObserver(),
		exitCh:      make(chan struct{}),
	}
}

// Start is to start resource manager
func (r *ResourceManager) Start() {
	r.wg.Run(r.cpuObserver.Start)
	r.wg.Run(func() {
		readMemTricker := time.NewTicker(memory.ReadMemInterval)
		defer readMemTricker.Stop()
		for {
			select {
			case <-readMemTricker.C:
				memory.ForceReadMemStats()
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
	r.wg.Done()
}
