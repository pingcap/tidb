// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package oracles

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
)

const epochShiftBits = 18

var _ oracle.Oracle = &localOracle{}

type localOracle struct {
	mu              sync.Mutex
	lastTimeStampTs int64
	n               int64
}

// NewLocalOracle creates an Oracle that uses local time as data source.
func NewLocalOracle() oracle.Oracle {
	return &localOracle{}
}

func (l *localOracle) IsExpired(lockTs uint64, TTL uint64) (bool, error) {
	beginMs := lockTs >> epochShiftBits
	return uint64(time.Now().UnixNano()/int64(time.Millisecond)) >= (beginMs + TTL), nil
}

func (l *localOracle) GetTimestamp() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := (time.Now().UnixNano() / int64(time.Millisecond)) << epochShiftBits
	if l.lastTimeStampTs == ts {
		l.n++
		return uint64(ts + l.n), nil
	}
	l.lastTimeStampTs = ts
	l.n = 0
	return uint64(ts), nil
}
