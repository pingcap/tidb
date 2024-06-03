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

package cache

import (
	"time"
)

type baseCache struct {
	interval time.Duration

	updateTime time.Time
}

func newBaseCache(interval time.Duration) baseCache {
	return baseCache{
		interval: interval,
	}
}

// ShouldUpdate returns whether this cache needs update
func (bc *baseCache) ShouldUpdate() bool {
	return time.Since(bc.updateTime) > bc.interval
}

// SetInterval sets the interval of updating cache
func (bc *baseCache) SetInterval(interval time.Duration) {
	bc.interval = interval
}

func (bc *baseCache) GetInterval() time.Duration {
	return bc.interval
}
