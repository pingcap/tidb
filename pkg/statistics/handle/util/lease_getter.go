// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"time"

	"go.uber.org/atomic"
)

// LeaseOffset represents the time offset for the stats cache to load statistics from the store.
// This value is crucial to ensure that the stats are retrieved at the correct interval.
// See more at where it is used.
const LeaseOffset = 5

// LeaseGetter is used to get the stats lease.
type LeaseGetter interface {
	// Lease returns the stats lease.
	Lease() time.Duration
}

// LeaseHolder holds the stats lease, which tests can change.
type LeaseHolder struct {
	lease *atomic.Duration
}

var _ LeaseGetter = (*LeaseHolder)(nil)

// NewLeaseHolder creates a new LeaseHolder.
func NewLeaseHolder(lease time.Duration) *LeaseHolder {
	return &LeaseHolder{
		lease: atomic.NewDuration(lease),
	}
}

// Lease implements LeaseGetter.
func (g *LeaseHolder) Lease() time.Duration {
	return g.lease.Load()
}

// SetLeaseForTest sets the stats lease.
func (g *LeaseHolder) SetLeaseForTest(lease time.Duration) {
	g.lease.Store(lease)
}
