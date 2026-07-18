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

package backup

import "sync"

type ResourceConcurrentLimiter struct {
	cond      *sync.Cond
	current   int
	threshold int
}

func NewResourceMemoryLimiter(threshold int) *ResourceConcurrentLimiter {
	return &ResourceConcurrentLimiter{
		cond:      sync.NewCond(new(sync.Mutex)),
		current:   0,
		threshold: threshold,
	}
}

func (rcl *ResourceConcurrentLimiter) Acquire(resource int) int {
	rcl.cond.L.Lock()
	defer rcl.cond.L.Unlock()
	for rcl.current >= rcl.threshold {
		rcl.cond.Wait()
	}
	rcl.current += resource
	return rcl.current
}

func (rcl *ResourceConcurrentLimiter) Release(resource int) {
	rcl.cond.L.Lock()
	defer rcl.cond.L.Unlock()
	rcl.current -= resource
	rcl.cond.Broadcast()
}
