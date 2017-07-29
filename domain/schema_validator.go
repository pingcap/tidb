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

	"github.com/ngaut/log"
)

// SchemaValidator is the interface for checking the validity of schema version.
type SchemaValidator interface {
	// Update the schema validator, add a new item, delete the expired items.
	// The schemaVer is valid within leaseGrantTime plus lease duration.
	Update(leaseGrantTime uint64, oldSchemaVer, newSchemaVer int64, changedTableIDs []int64)
	// Check is it valid for a transaction to use schemaVer, at timestamp txnTS.
	Check(txnTS uint64, schemaVer int64) bool
	// Latest returns the latest schema version it knows, but not necessary a valid one.
	Latest() int64
	// Stop stops checking the valid of transaction.
	IsAllExpired(txnTS uint64) bool
	IsRelatedTablesChanged(txnTS uint64, currVer int64, tableIDs []int64) bool
	Stop()
	// Restart restarts the schema validator after it is stopped.
	Restart()
}

type deltaSchemaInfo struct {
	expire            time.Time
	nextSchemaVersion int64
	relatedTableIDs   []int64
}

type schemaValidator struct {
	mux              sync.RWMutex
	lease            time.Duration
	items            map[int64]*deltaSchemaInfo
	oldestSchemaVer  int64
	latestSchemaVer  int64
	latestSchemaInfo *deltaSchemaInfo
}

func NewSchemaValidator(lease time.Duration) SchemaValidator {
	return &schemaValidator{
		lease: lease,
		items: make(map[int64]*deltaSchemaInfo),
	}
}

func (s *schemaValidator) Stop() {
	log.Info("the schema validator stops")
	s.mux.Lock()
	defer s.mux.Unlock()
	s.items = nil
	s.oldestSchemaVer = 0
	s.latestSchemaVer = 0
	s.latestSchemaInfo = nil
}

func (s *schemaValidator) Restart() {
	log.Info("the schema validator restarts")
	s.mux.Lock()
	defer s.mux.Unlock()
	s.items = make(map[int64]*deltaSchemaInfo)
}

func (s *schemaValidator) Update(leaseGrantTS uint64, oldVer, currVer int64, changedTableIDs []int64) {
	s.mux.Lock()

	if s.items == nil || currVer == 0 {
		s.mux.Unlock()
		log.Infof("the schema validator stopped before updating")
		return
	}

	// Renew the lease.
	leaseGrantTime := extractPhysicalTime(leaseGrantTS)
	leaseExpire := leaseGrantTime.Add(s.lease - time.Millisecond)
	// Update the info.
	if currVer != oldVer {
		currInfo := &deltaSchemaInfo{expire: leaseExpire}
		oldInfo, ok := s.items[oldVer]
		if ok {
			oldInfo.nextSchemaVersion = currVer
		}
		currInfo.relatedTableIDs = changedTableIDs
		s.items[currVer] = currInfo
		s.latestSchemaInfo = currInfo
	} else {
		log.Warnf("update old ver %v, current ver %v, latest ver %v", oldVer, currVer, s.latestSchemaVer)
		s.items[currVer].expire = leaseExpire
		s.latestSchemaInfo.expire = leaseExpire
	}
	s.latestSchemaVer = currVer
	log.Warnf("update old ver %v, current ver %v, latest ver %v", oldVer, currVer, s.latestSchemaVer)

	// Delete expired items, leaseGrantTime is server current time, actually.
	// TODO: Remove the should update all schema.
	minVer := currVer
	for k, info := range s.items {
		if leaseGrantTime.After(info.expire) && shouldUpdateAllSchema(currVer, k) {
			delete(s.items, k)
		} else if k < minVer {
			minVer = k
		}
	}
	s.oldestSchemaVer = minVer

	s.mux.Unlock()
}

func (s *schemaValidator) IsAllExpired(txnTS uint64) bool {
	s.mux.RLock()
	defer s.mux.RUnlock()

	if s.latestSchemaInfo == nil {
		return true
	}
	t := extractPhysicalTime(txnTS)
	if t.After(s.latestSchemaInfo.expire) {
		return true
	}
	return false
}

func hasRelatedTableID(relatedTableIDs, updateTableIDs []int64) bool {
	for _, tblID := range updateTableIDs {
		for _, relatedTblID := range relatedTableIDs {
			log.Debugf("tbl id %v, related id %v", tblID, relatedTblID)
			if tblID == relatedTblID {
				return true
			}
		}
	}
	return false
}

func (s *schemaValidator) IsRelatedTablesChanged(txnTS uint64, currVer int64, tableIDs []int64) bool {
	s.mux.RLock()
	defer s.mux.RUnlock()

	if s.items == nil {
		// TODO: It's better to return an error.
		log.Infof("the schema validator stopped before judging")
		return true
	}

	log.Warnf("curr ver %v, oldest ver %v, ids %v", currVer, s.oldestSchemaVer, tableIDs)
	// TODO: If the current schema version is deleted.
	info, ok := s.items[currVer]
	if !ok {
		currVer = s.oldestSchemaVer
	}

	for {
		info, ok = s.items[currVer]
		log.Warnf("ok %v, info %v, curr ver %v, tableIDs %v", ok, info, currVer, tableIDs)
		if !ok {
			return false
		}
		if hasRelatedTableID(tableIDs, info.relatedTableIDs) {
			return true
		}
		currVer = info.nextSchemaVersion
	}
}

// Check checks schema validity, returns true if use schemaVer at txnTS is legal.
func (s *schemaValidator) Check(txnTS uint64, schemaVer int64) bool {
	s.mux.RLock()
	defer s.mux.RUnlock()
	if s.items == nil {
		log.Infof("the schema validator stopped before checking")
		return false
	}

	if s.lease == 0 {
		return true
	}

	info, ok := s.items[schemaVer]
	if !ok {
		// Can't find schema version means it's already expired.
		return false
	}

	t := extractPhysicalTime(txnTS)
	if t.After(info.expire) {
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
