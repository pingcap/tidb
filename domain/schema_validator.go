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

package domain

import (
	"sync"
	"time"
)

// SchemaValidator is the interface for checking the validity of schema version.
type SchemaValidator interface {
	// Update the schema validator, add a new item, delete the expired items.
	// The schemaVer is valid within leaseGrantTime plus lease duration.
	Update(leaseGrantTime uint64, schemaVer int64)
	// Check is it valid for a transaction to use schemaVer, at timestamp txnTS.
	Check(txnTS uint64, schemaVer int64) bool
	// Latest returns the latest schema version it knows, but not necessary a valid one.
	Latest() int64
}

type schemaValidator struct {
	mux             sync.RWMutex
	lease           time.Duration
	items           map[int64]time.Time
	latestSchemaVer int64
}

func newSchemaValidator(lease time.Duration) SchemaValidator {
	return &schemaValidator{
		lease: lease,
		items: make(map[int64]time.Time),
	}
}

func (s *schemaValidator) Update(leaseGrantTS uint64, schemaVer int64) {
	s.mux.Lock()

	s.latestSchemaVer = schemaVer
	leaseGrantTime := extractPhysicalTime(leaseGrantTS)
	leaseExpire := leaseGrantTime.Add(s.lease - time.Millisecond)

	// Renewal lease.
	s.items[schemaVer] = leaseExpire

	// Delete expired items, leaseGrantTime is server current time, actually.
	for k, expire := range s.items {
		if leaseGrantTime.After(expire) {
			delete(s.items, k)
		}
	}

	s.mux.Unlock()
}

// Check checks schema validity, returns true if use schemaVer at txnTS is legal.
func (s *schemaValidator) Check(txnTS uint64, schemaVer int64) bool {
	s.mux.RLock()
	defer s.mux.RUnlock()

	if s.lease == 0 {
		return true
	}

	expire, ok := s.items[schemaVer]
	if !ok {
		// Can't find schema version means it's already expired.
		return false
	}

	t := extractPhysicalTime(txnTS)
	if t.After(expire) {
		return false
	}

	return true
}

// Latest returns the latest schema version it knows.
func (s *schemaValidator) Latest() int64 {
	s.mux.RLock()
	ret := s.latestSchemaVer
	s.mux.RUnlock()
	return ret
}

func extractPhysicalTime(ts uint64) time.Time {
	t := int64(ts >> 18) // 18 for physicalShiftBits
	return time.Unix(t/1e3, (t%1e3)*1e6)
}
