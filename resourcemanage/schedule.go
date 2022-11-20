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
	"github.com/pingcap/tidb/resourcemanage/scheduler"
	"github.com/pingcap/tidb/resourcemanage/util"
)

func (r *ResourceManage) schedule() {
	for _, pool := range r.poolMap {
		r.schedulePool(pool)
	}
	for _, pool := range r.poolMap {
		r.limitPool(pool)
	}
}

func (r *ResourceManage) schedulePool(pool *util.PoolContainer) {
	for _, sch := range r.scheduler {
		cmd := sch.Tune(pool.Component, pool.Pool)
		switch cmd {
		case scheduler.Overclock:
			cap := pool.Pool.Cap()
			pool.Pool.Tune(cap + 1)
		case scheduler.Downclock:
			cap := pool.Pool.Cap()
			pool.Pool.Tune(cap - 1)
		case scheduler.Hold:
			continue
		case scheduler.NoIdea:
			continue
		}
	}
}

func (r *ResourceManage) limitPool(pool *util.PoolContainer) {
	for _, lim := range r.limiter {
		lim.Limit(pool.Component, pool.Pool)
	}
}
