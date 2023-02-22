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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/resourcemanager/scheduler"
	"github.com/pingcap/tidb/resourcemanager/util"
	"go.uber.org/zap"
)

func (r *ResourceManager) schedule() {
	r.poolMap.Iter(func(pool *util.PoolContainer) {
		cmd := r.schedulePool(pool)
		r.exec(pool, cmd)
	})
}

func (r *ResourceManager) schedulePool(pool *util.PoolContainer) scheduler.Command {
	if pool.Pool.Running() == 0 {
		return scheduler.Hold
	}
	for _, sch := range r.scheduler {
		cmd := sch.Tune(pool.Component, pool.Pool)
		switch cmd {
		case scheduler.Hold:
			continue
		default:
			return cmd
		}
	}
	return scheduler.Hold
}

func (*ResourceManager) exec(pool *util.PoolContainer, cmd scheduler.Command) {
	if cmd == scheduler.Hold {
		return
	}
	if time.Since(pool.Pool.LastTunerTs()) > util.MinSchedulerInterval.Load() {
		con := pool.Pool.Cap()
		switch cmd {
		case scheduler.Downclock:
			concurrency := con - 1
			log.Debug("[resource manager] downclock goroutine pool",
				zap.Int("origin concurrency", con),
				zap.Int("concurrency", concurrency),
				zap.String("name", pool.Pool.Name()))
			pool.Pool.Tune(concurrency)
		case scheduler.Overclock:
			concurrency := con + 1
			log.Debug("[resource manager] overclock goroutine pool",
				zap.Int("origin concurrency", con),
				zap.Int("concurrency", concurrency),
				zap.String("name", pool.Pool.Name()))
			pool.Pool.Tune(concurrency)
		}
	}
}
