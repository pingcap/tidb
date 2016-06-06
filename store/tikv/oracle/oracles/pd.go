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
	c pd.Client
}

// NewPdOracle create an Oracle that uses a pd client source.
// Refer https://github.com/pingcap/pd/blob/master/pd-client/client.go for more details.
func NewPdOracle(pdClient pd.Client) oracle.Oracle {
	return &pdOracle{
		c: pdClient,
	}
}

// IsExpired returns whether lockTs+TTL is expired, both are ms.
func (t *pdOracle) IsExpired(lockTs, TTL uint64) (bool, error) {
	physical, _, err := t.c.GetTS()
	if err != nil {
		return false, errors.Trace(err)
	}
	return uint64(physical) >= (lockTs>>epochShiftBits)+TTL, nil
}

// GetTimestamp gets a new increasing time.
func (t *pdOracle) GetTimestamp() (uint64, error) {
	now := time.Now()
	physical, logical, err := t.c.GetTS()
	if err != nil {
		return 0, errors.Trace(err)
	}
	dist := time.Since(now)
	if dist > slowDist {
		log.Warnf("get timestamp too slow: %s", dist)
	}
	return uint64((physical << epochShiftBits) + logical), nil
}
