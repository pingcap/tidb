// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crossks

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema/issyncer/mdldef"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// schemaCoordinator is used to manage internal sessions to coordinate schema
// changes, see InfoSchemaCoordinator for more details.
type schemaCoordinator struct {
	printMDLLogTime time.Time
	mu              sync.RWMutex
	sessions        map[sessionctx.Context]struct{}
}

func newSchemaCoordinator() *schemaCoordinator {
	return &schemaCoordinator{
		printMDLLogTime: time.Now(),
		sessions:        make(map[sessionctx.Context]struct{}),
	}
}

func (c *schemaCoordinator) StoreInternalSession(se any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessions[se.(sessionctx.Context)] = struct{}{}
}

func (c *schemaCoordinator) DeleteInternalSession(se any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sessions, se.(sessionctx.Context))
}

func (c *schemaCoordinator) ContainsInternalSession(se any) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.sessions[se.(sessionctx.Context)]
	return ok
}

func (c *schemaCoordinator) InternalSessionCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.sessions)
}

func (c *schemaCoordinator) CheckOldRunningTxn(jobs map[int64]*mdldef.JobMDL) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	printLog := false
	if time.Since(c.printMDLLogTime) > 10*time.Second {
		printLog = true
		c.printMDLLogTime = time.Now()
	}
	for se := range c.sessions {
		variable.RemoveLockDDLJobs(se.GetSessionVars(), jobs, printLog)
	}
}

func (*schemaCoordinator) KillNonFlashbackClusterConn() {}
