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
	// Update the schema validator, add a new item, delete the expired cacheItemInfos.
	// The schemaVer is valid within leaseGrantTime plus lease duration.
	// Add the changed table IDs to the new schema information,
	// which is produced when the oldSchemaVer is updated to the newSchemaVer.
	Update(leaseGrantTime uint64, oldSchemaVer, newSchemaVer int64, changedTableIDs []int64)
	// Check is it valid for a transaction to use schemaVer, at timestamp txnTS.
	Check(txnTS uint64, schemaVer int64) bool
	// Latest returns the latest schema version it knows, but not necessary a valid one.
	Latest() int64
	// IsRelatedTablesChanged returns the result whether relatedTableIDs is changed from usedVer to the latest schema version,
	// and an error.
	IsRelatedTablesChanged(txnTS uint64, usedVer int64, relatedTableIDs []int64) (bool, error)
	// Stop stops checking the valid of transaction.
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
	mux        sync.RWMutex
	lease      time.Duration
	validItems map[int64]time.Time
	// cacheItemInfos caches the items' information, and some items may be expired.
	// It's used to cache the updated table IDs, which is produced when the previous item's version is updated to current item's version.
	cacheItemInfos   map[int64]*deltaSchemaInfo
	latestSchemaVer  int64
	latestSchemaInfo *deltaSchemaInfo
}

// NewSchemaValidator returns a SchemaValidator structure.
func NewSchemaValidator(lease time.Duration) SchemaValidator {
	return &schemaValidator{
		lease:          lease,
		validItems:     make(map[int64]time.Time),
		cacheItemInfos: make(map[int64]*deltaSchemaInfo),
	}
}

func (s *schemaValidator) Stop() {
	log.Info("the schema validator stops")
	s.mux.Lock()
	defer s.mux.Unlock()
	s.cacheItemInfos = nil
	s.validItems = nil
	s.latestSchemaVer = 0
	s.latestSchemaInfo = nil
}

func (s *schemaValidator) Restart() {
	log.Info("the schema validator restarts")
	s.mux.Lock()
	defer s.mux.Unlock()
	s.validItems = make(map[int64]time.Time)
	s.cacheItemInfos = make(map[int64]*deltaSchemaInfo)
}

func (s *schemaValidator) Update(leaseGrantTS uint64, oldVer, currVer int64, changedTableIDs []int64) {
	s.mux.Lock()

	if s.cacheItemInfos == nil {
		s.mux.Unlock()
		log.Infof("the schema validator stopped before updating")
		return
	}

	// Renew the lease.
	leaseGrantTime := extractPhysicalTime(leaseGrantTS)
	leaseExpire := leaseGrantTime.Add(s.lease - time.Millisecond)
	s.validItems[currVer] = leaseExpire
	_, hasCurrVerItemInfo := s.cacheItemInfos[currVer]
	// Update the schema information.
	if currVer != oldVer || currVer == 0 || !hasCurrVerItemInfo {
		s.cacheItemInfos[currVer] = &deltaSchemaInfo{
			expire:          leaseExpire,
			relatedTableIDs: changedTableIDs,
		}
		oldInfo, ok := s.cacheItemInfos[oldVer]
		if ok {
			oldInfo.nextSchemaVersion = currVer
		}
	} else {
		s.cacheItemInfos[currVer].expire = leaseExpire
	}
	s.latestSchemaVer = currVer
	s.latestSchemaInfo = s.cacheItemInfos[currVer]

	// Delete expired cacheItemInfos, leaseGrantTime is server current time, actually.
	for k, info := range s.cacheItemInfos {
		if leaseGrantTime.After(info.expire) {
			delete(s.validItems, k)
			// We cache some expired schema versions to store recently updated table IDs.
			if shouldUpdateAllSchema(currVer, k) {
				delete(s.cacheItemInfos, k)
			}
		}
	}

	s.mux.Unlock()
}

func hasRelatedTableID(relatedTableIDs, updateTableIDs []int64) bool {
	for _, tblID := range updateTableIDs {
		for _, relatedTblID := range relatedTableIDs {
			if tblID == relatedTblID {
				return true
			}
		}
	}
	return false
}

func (s *schemaValidator) isAllExpired(txnTS uint64) bool {
	if s.latestSchemaInfo == nil {
		return true
	}
	t := extractPhysicalTime(txnTS)
	return t.After(s.latestSchemaInfo.expire)
}

func (s *schemaValidator) IsRelatedTablesChanged(txnTS uint64, currVer int64, tableIDs []int64) (bool, error) {
	s.mux.RLock()
	defer s.mux.RUnlock()

	if s.cacheItemInfos == nil {
		log.Infof("the schema validator stopped before judging")
		return false, ErrInfoSchemaExpired
	}
	if s.isAllExpired(txnTS) {
		log.Infof("the schema validator's latest schema version %d is expired", s.latestSchemaVer)
		return false, ErrInfoSchemaExpired
	}

	_, isExisting := s.cacheItemInfos[currVer]
	if !isExisting {
		log.Infof("the schema version %d is much older than the latest version %d", currVer, s.latestSchemaVer)
		return false, ErrInfoSchemaChanged
	}

	for {
		info, ok := s.cacheItemInfos[currVer]
		if !ok {
			return false, nil
		}
		if hasRelatedTableID(tableIDs, info.relatedTableIDs) {
			return true, nil
		}
		currVer = info.nextSchemaVersion
	}
}

// Check checks schema validity, returns true if use schemaVer at txnTS is legal.
func (s *schemaValidator) Check(txnTS uint64, schemaVer int64) bool {
	s.mux.RLock()
	defer s.mux.RUnlock()
	if s.cacheItemInfos == nil {
		log.Infof("the schema validator stopped before checking")
		return false
	}

	if s.lease == 0 {
		return true
	}

	expire, ok := s.validItems[schemaVer]
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
