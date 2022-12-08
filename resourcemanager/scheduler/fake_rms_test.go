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

package scheduler

import "github.com/pingcap/tidb/resourcemanager/util"

type FakeResourceManage struct {
	scheduler Scheduler
	pool      *util.FakeGPool
}

// NewFakeResourceManage creates a fake resource manage.
func NewFakeResourceManage() *FakeResourceManage {
	return &FakeResourceManage{}
}

// Register registers a scheduler.
func (f *FakeResourceManage) Register(sch Scheduler) {
	f.scheduler = sch
}

// Register registers a scheduler.
func (f *FakeResourceManage) RegisterPool(pool *util.FakeGPool) {
	f.pool = pool
}

// Next get scheduler command.
func (f *FakeResourceManage) Next() Command {
	if f.scheduler != nil {
		defer f.pool.Next()
		return f.scheduler.Tune(util.UNKNOWN, f.pool)
	}
	return Hold
}

func (f *FakeResourceManage) GetPool() *util.FakeGPool {
	return f.pool
}
