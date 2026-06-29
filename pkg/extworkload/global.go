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

package extworkload

import (
	"sync"

	"github.com/pingcap/tidb/pkg/config/deploymode"
)

var globalManager struct {
	sync.RWMutex
	mgr Manager
}

// SetGlobalManager installs the process-wide external workload manager.
func SetGlobalManager(mgr Manager) {
	globalManager.Lock()
	globalManager.mgr = mgr
	globalManager.Unlock()
}

// GetGlobalManager returns the installed manager only in Starter deploy mode.
func GetGlobalManager() Manager {
	if !deploymode.IsStarter() {
		return nil
	}
	globalManager.RLock()
	defer globalManager.RUnlock()
	return globalManager.mgr
}
