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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

var _ oracle.Oracle = &pdOracle{}

const slowDist = 30 * time.Millisecond

// pdOracle is an Oracle that uses a placement driver client as source.
type pdOracle struct {
	c  pd.Client
	mu struct {
		sync.RWMutex
		lastTS uint64
	}
	quit    chan struct{}
	updated chan struct{}
}

// NewPdOracle create an Oracle that uses a pd client source.
// Refer https://github.com/pingcap/pd/blob/master/pd-client/client.go for more details.
// PdOracle mantains `lastTS` to store the last timestamp got from PD server. If
// `GetTimestamp()` is not called after `updateInterval`, it will be called by
// itself to keep up with the timestamp on PD server.
func NewPdOracle(pdClient pd.Client, updateInterval time.Duration) (oracle.Oracle, error) {
	o := &pdOracle{
		c:       pdClient,
		quit:    make(chan struct{}),
		updated: make(chan struct{}),
	}
	go o.updateTS(updateInterval)
	// Initialize lastTS by Get.
	_, err := o.GetTimestamp()
	if err != nil {
		o.Close()
		return nil, errors.Trace(err)
	}
	return o, nil
}

// IsExpired returns whether lockTS+TTL is expired, both are ms. It uses `lastTS`
// to compare, may return false negative result temporarily.
func (o *pdOracle) IsExpired(lockTS, TTL uint64) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()

	return oracle.ExtractPhysical(o.mu.lastTS) >= oracle.ExtractPhysical(lockTS)+int64(TTL)
}

// GetTimestamp gets a new increasing time.
func (o *pdOracle) GetTimestamp() (uint64, error) {
	ts, err := o.getTimestamp()
	if err != nil {
		return 0, errors.Trace(err)
	}
	o.setLastTS(ts)
	o.updated <- struct{}{}
	return ts, nil
}

func (o *pdOracle) getTimestamp() (uint64, error) {
	now := time.Now()
	physical, logical, err := o.c.GetTS()
	if err != nil {
		return 0, errors.Trace(err)
	}
	dist := time.Since(now)
	if dist > slowDist {
		log.Warnf("get timestamp too slow: %s", dist)
	}
	return oracle.ComposeTS(physical, logical), nil
}

func (o *pdOracle) setLastTS(ts uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if ts >= o.mu.lastTS {
		o.mu.lastTS = ts
	}
}

func (o *pdOracle) updateTS(interval time.Duration) {
	for {
		select {
		case <-time.After(interval):
			ts, err := o.getTimestamp()
			if err != nil {
				log.Errorf("updateTS error: %v", err)
				break
			}
			o.setLastTS(ts)
		case <-o.updated:
		case <-o.quit:
			return
		}
	}
}

func (o *pdOracle) Close() {
	close(o.quit)
}
