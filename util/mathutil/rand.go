// Copyright 2020 PingCAP, Inc.
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

package mathutil

import (
	"sync"
	"time"
)

const maxRandValue = 0x3FFFFFFF

// MysqlRng is random number generator and this implementation is ported from MySQL.
// See https://github.com/tikv/tikv/pull/6117#issuecomment-562489078.
type MysqlRng struct {
	seed1 uint32
	seed2 uint32
	mu    *sync.Mutex
}

// NewWithSeed create a rng with random seed.
func NewWithSeed(seed int64) *MysqlRng {
	seed1 := uint32(seed*0x10001+55555555) % maxRandValue
	seed2 := uint32(seed*0x10000001) % maxRandValue
	return &MysqlRng{
		seed1: seed1,
		seed2: seed2,
		mu:    &sync.Mutex{},
	}
}

// NewWithTime create a rng with time stamp.
func NewWithTime() *MysqlRng {
	return NewWithSeed(time.Now().UnixNano())
}

// Gen will generate random number.
func (rng *MysqlRng) Gen() float64 {
	rng.mu.Lock()
	defer rng.mu.Unlock()
	rng.seed1 = (rng.seed1*3 + rng.seed2) % maxRandValue
	rng.seed2 = (rng.seed1 + rng.seed2 + 33) % maxRandValue
	return float64(rng.seed1) / float64(maxRandValue)
}

// SetSeed1 is a interface to set seed1
func (rng *MysqlRng) SetSeed1(seed uint32) {
	rng.mu.Lock()
	defer rng.mu.Unlock()
	rng.seed1 = seed
}

// SetSeed2 is a interface to set seed2
func (rng *MysqlRng) SetSeed2(seed uint32) {
	rng.mu.Lock()
	defer rng.mu.Unlock()
	rng.seed2 = seed
}
