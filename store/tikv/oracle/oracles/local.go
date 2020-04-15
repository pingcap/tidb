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
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/v4/store/tikv/oracle"
)

var _ oracle.Oracle = &localOracle{}

type localOracle struct {
	sync.Mutex
	lastTimeStampTS uint64
	n               uint64
	hook            *struct {
		currentTime time.Time
	}
}

// NewLocalOracle creates an Oracle that uses local time as data source.
func NewLocalOracle() oracle.Oracle {
	return &localOracle{}
}

func (l *localOracle) IsExpired(lockTS uint64, TTL uint64) bool {
	now := time.Now()
	if l.hook != nil {
		now = l.hook.currentTime
	}
	return oracle.GetPhysical(now) >= oracle.ExtractPhysical(lockTS)+int64(TTL)
}

func (l *localOracle) GetTimestamp(context.Context) (uint64, error) {
	l.Lock()
	defer l.Unlock()
	now := time.Now()
	if l.hook != nil {
		now = l.hook.currentTime
	}
	physical := oracle.GetPhysical(now)
	ts := oracle.ComposeTS(physical, 0)
	if l.lastTimeStampTS == ts {
		l.n++
		return ts + l.n, nil
	}
	l.lastTimeStampTS = ts
	l.n = 0
	return ts, nil
}

func (l *localOracle) GetTimestampAsync(ctx context.Context) oracle.Future {
	return &future{
		ctx: ctx,
		l:   l,
	}
}

func (l *localOracle) GetLowResolutionTimestamp(ctx context.Context) (uint64, error) {
	return l.GetTimestamp(ctx)
}

func (l *localOracle) GetLowResolutionTimestampAsync(ctx context.Context) oracle.Future {
	return l.GetTimestampAsync(ctx)
}

type future struct {
	ctx context.Context
	l   *localOracle
}

func (f *future) Wait() (uint64, error) {
	return f.l.GetTimestamp(f.ctx)
}

// UntilExpired implement oracle.Oracle interface.
func (l *localOracle) UntilExpired(lockTimeStamp uint64, TTL uint64) int64 {
	now := time.Now()
	if l.hook != nil {
		now = l.hook.currentTime
	}
	return oracle.ExtractPhysical(lockTimeStamp) + int64(TTL) - oracle.GetPhysical(now)
}

func (l *localOracle) Close() {
}
