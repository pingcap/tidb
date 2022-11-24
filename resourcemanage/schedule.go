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

	"github.com/pingcap/tidb/resourcemanage/scheduler"
	"github.com/pingcap/tidb/resourcemanage/util"
)

func (r *ResourceManage) schedule() {
	for _, pool := range r.poolMap {
		cmd := r.schedulePool(pool)
		isLimit := r.limitPool(pool)
		r.exec(pool, cmd, isLimit)
	}
}

func (r *ResourceManage) schedulePool(pool *util.PoolContainer) scheduler.Command {
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

func (r *ResourceManage) limitPool(pool *util.PoolContainer) bool {
	for _, lim := range r.limiter {
		if lim.Limit(pool.Component, pool.Pool) {
			return true
		}
	}
	return false
}

func (*ResourceManage) exec(pool *util.PoolContainer, cmd scheduler.Command, isLimit bool) {
	if cmd == scheduler.Hold && !isLimit {
		return
	}
	if time.Since(pool.Pool.LastTunerTs()) > 200*time.Millisecond {
		con := pool.Pool.Cap()
		switch cmd {
		case scheduler.Downclock:
			pool.Pool.Tune(con-1, isLimit)
		case scheduler.Overclock:
			pool.Pool.Tune(con+1, isLimit)
		}
	}
}
