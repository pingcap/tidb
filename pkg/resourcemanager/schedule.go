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
	"github.com/pingcap/tidb/pkg/resourcemanager/scheduler"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"go.uber.org/zap"
)

func (r *ResourceManager) schedule() {
	r.poolMap.Iter(func(pool *util.PoolContainer) {
		if pool.Component == util.DistTask {
			return
		}
		cmd := r.schedulePool(pool)
		r.Exec(pool, cmd)
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
			if cmd == scheduler.Downclock {
				if pool.Pool.Cap() == 1 || pool.Pool.Running() > pool.Pool.Cap() {
					continue
				}
			}
			return cmd
		}
	}
	return scheduler.Hold
}

// Exec is to executor the command from scheduler.
func (*ResourceManager) Exec(pool *util.PoolContainer, cmd scheduler.Command) {
	if cmd == scheduler.Hold {
		return
	}
	if time.Since(pool.Pool.LastTunerTs()) > util.MinSchedulerInterval.Load() {
		con := pool.Pool.Cap()
		switch cmd {
		case scheduler.Downclock:
			concurrency := con - 1
			log.Debug("downclock goroutine pool", zap.String("category", "resource manager"),
				zap.Int32("origin concurrency", con),
				zap.Int32("concurrency", concurrency),
				zap.String("name", pool.Pool.Name()))
			pool.Pool.Tune(concurrency)
		case scheduler.Overclock:
			concurrency := con + 1
			// The maximum increase in concurrency compared to the original amount is limited to MaxOverclockCount.
			if concurrency > pool.Pool.GetOriginConcurrency()+util.MaxOverclockCount {
				return
			}
			log.Debug("overclock goroutine pool", zap.String("category", "resource manager"),
				zap.Int32("origin concurrency", con),
				zap.Int32("concurrency", concurrency),
				zap.String("name", pool.Pool.Name()))
			pool.Pool.Tune(concurrency)
		}
	}
}
