// Copyright 2025 PingCAP, Inc.
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

package standby

import (
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/signal"
	"go.uber.org/zap"
)

// OnConnActive makes sure `lastActive` is not less than current time.
func (c *LoadKeyspaceController) OnConnActive() {
	t := time.Now()
	for {
		last := atomic.LoadInt64(&c.lastActive)
		if last >= t.Unix() {
			return
		}
		if atomic.CompareAndSwapInt64(&c.lastActive, last, t.Unix()) {
			return
		}
	}
}

// OnServerCreated watches `lastActive` and exits the process if it is not updated for a long time.
func (c *LoadKeyspaceController) OnServerCreated(svr *server.Server) {
	maxIdleSecs := int(config.GetGlobalConfig().Standby.MaxIdleSeconds)
	if maxIdleSecs <= 0 {
		return
	}

	c.OnConnActive()
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
		for range ticker.C {
			last := atomic.LoadInt64(&c.lastActive)
			if time.Now().Unix()-last > int64(maxIdleSecs) {
				connCount := svr.ConnectionCount()
				var processCount, inTransCount int
				for _, p := range svr.GetUserProcessList() {
					if p.Command != mysql.ComSleep { // ignore sleep sessions (waiting for client query).
						processCount++
					}
					if p.State&mysql.ServerStatusInTrans > 0 {
						inTransCount++
					}
				}
				var clientInteractiveCount int
				for _, c := range svr.GetClientCapabilityList() {
					if c&mysql.ClientInteractive > 0 {
						clientInteractiveCount++
					}
				}
				logutil.BgLogger().Info("connection idle for too long",
					zap.Int("max-idle-seconds", maxIdleSecs),
					zap.Int("connection-count", connCount),
					zap.Int("process-count", processCount),
					zap.Int("inTrans-count", inTransCount),
					zap.Int("client-interactive-count", clientInteractiveCount))

				// if zero backend enabled. We shouldn't skip graceful shutdown to wait for session migration.
				// And clientInteractiveCount don't need to be considered because session can be restored by gateway.
				if config.GetGlobalConfig().Standby.EnableZeroBackend && (connCount == 0 || processCount == 0) && inTransCount == 0 {
					SaveTidbNormalRestartInfo("connection idle for too long")
					signal.TiDBExit(syscall.SIGTERM)
				} else if (connCount == 0 || processCount == 0) && inTransCount == 0 && clientInteractiveCount == 0 {
					SaveTidbNormalRestartInfo("connection idle for too long")
					signal.TiDBExit(syscall.SIGINT)
				}
			}
		}
	}()
}
