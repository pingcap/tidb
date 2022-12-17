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
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/cpu"
)

// GlobalResourceManager is a global resource manager
var GlobalResourceManager = NewResourceManger()

// ResourceManager is a resource manager
type ResourceManager struct {
	cpuObserver *cpu.Observer
	wg          tidbutil.WaitGroupWrapper
}

// NewResourceManger is to create a new resource manager
func NewResourceManger() *ResourceManager {
	return &ResourceManager{
		cpuObserver: cpu.NewCPUObserver(),
	}
}

// Start is to start resource manager
func (r *ResourceManager) Start() {
	r.wg.Run(r.cpuObserver.Start)
}

// Stop is to stop resource manager
func (r *ResourceManager) Stop() {
	r.cpuObserver.Stop()
	r.wg.Done()
}
