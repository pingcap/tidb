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

	log "github.com/Sirupsen/logrus"
)

type checkResult int

const (
	// ResultSucc means schemaValidator's check is passing.
	ResultSucc checkResult = iota
	// ResultFail means schemaValidator's check is fail.
	ResultFail
	// ResultUnknown means schemaValidator doesn't know the check would be success or fail.
	ResultUnknown
)

// SchemaValidator is the interface for checking the validity of schema version.
type SchemaValidator interface {
	// Update the schema validator, add a new item, delete the expired detalItemInfos.
	// The latest schemaVer is valid within leaseGrantTime plus lease duration.
	// Add the changed table IDs to the new schema information,
	// which is produced when the oldSchemaVer is updated to the newSchemaVer.
	Update(leaseGrantTime uint64, oldSchemaVer, newSchemaVer int64, changedTableIDs []int64)
	// Check is it valid for a transaction to use schemaVer and related tables, at timestamp txnTS.
	Check(txnTS uint64, schemaVer int64, relatedTableIDs []int64) checkResult
	// Stop stops checking the valid of transaction.
	Stop()
	// Restart restarts the schema validator after it is stopped.
	Restart()
}

type deltaSchemaInfo struct {
	schemaVersion   int64
	relatedTableIDs []int64
}

type schemaValidator struct {
	isStarted          bool
	mux                sync.RWMutex
	lease              time.Duration
	latestSchemaVer    int64
	latestSchemaExpire time.Time
	// detalItemInfos caches the items' information, and the item will be remove when it's is much older than latest schema version.
	// It's used to cache the updated table IDs, which is produced when the previous item's version is updated to current item's version.
	detalItemInfos []*deltaSchemaInfo
	// itemSchemaVers caches the schema version in detalItemInfos. It's used to quickly find the schema version in detalItemInfos.
	itemSchemaVers map[int64]struct{}
}

// NewSchemaValidator returns a SchemaValidator structure.
func NewSchemaValidator(lease time.Duration) SchemaValidator {
	return &schemaValidator{
		isStarted:      true,
		lease:          lease,
		itemSchemaVers: make(map[int64]struct{}),
		detalItemInfos: make([]*deltaSchemaInfo, 0, maxNumberOfDiffsToLoad),
	}
}

func (s *schemaValidator) Stop() {
	log.Info("the schema validator stops")
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = false
	s.latestSchemaVer = 0
	s.detalItemInfos = nil
	s.itemSchemaVers = nil
}

func (s *schemaValidator) Restart() {
	log.Info("the schema validator restarts")
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = true
	s.itemSchemaVers = make(map[int64]struct{})
	s.detalItemInfos = make([]*deltaSchemaInfo, 0, maxNumberOfDiffsToLoad)
}

func (s *schemaValidator) Update(leaseGrantTS uint64, oldVer, currVer int64, changedTableIDs []int64) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if !s.isStarted {
		log.Infof("the schema validator stopped before updating")
		return
	}

	// Renew the lease.
	s.latestSchemaVer = currVer
	leaseGrantTime := extractPhysicalTime(leaseGrantTS)
	leaseExpire := leaseGrantTime.Add(s.lease - time.Millisecond)
	s.latestSchemaExpire = leaseExpire

	// Update the schema information map and slice.
	_, hasCurrVerItemInfo := s.itemSchemaVers[currVer]
	if currVer != oldVer || !hasCurrVerItemInfo {
		s.itemSchemaVers[currVer] = struct{}{}
		s.detalItemInfos = append(s.detalItemInfos, &deltaSchemaInfo{schemaVersion: currVer, relatedTableIDs: changedTableIDs})
	}

	// We cache some schema versions to store recently updated table IDs.
	// If the schema version is much older than the latest schema version, we will delete it from itemSchemaVers.
	offset := -1
	for i, info := range s.detalItemInfos {
		if !isTooOldSchema(info.schemaVersion, currVer) {
			break
		}
		delete(s.itemSchemaVers, info.schemaVersion)
		offset = i
	}
	// If the schema version is much older than the latest schema version, we will delete it from detalItemInfos.
	s.detalItemInfos = s.detalItemInfos[offset+1:]
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

// isRelatedTablesChanged returns the result whether relatedTableIDs is changed
// from usedVer to the latest schema version.
// NOTE, this function should be called under lock!
func (s *schemaValidator) isRelatedTablesChanged(currVer int64, tableIDs []int64) bool {
	if _, ok := s.itemSchemaVers[currVer]; !ok {
		log.Infof("the schema version %d is much older than the latest version %d", currVer, s.latestSchemaVer)
		return true
	}

	// Find currVer's offset in detaItemInfos.
	offset := 0
	for i, info := range s.detalItemInfos {
		if info.schemaVersion == currVer {
			offset = i
			break
		}
	}
	for i := offset + 1; i < len(s.detalItemInfos); i++ {
		info := s.detalItemInfos[i]
		if hasRelatedTableID(tableIDs, info.relatedTableIDs) {
			return true
		}
	}
	return false
}

// Check checks schema validity, returns true if use schemaVer and related tables at txnTS is legal.
func (s *schemaValidator) Check(txnTS uint64, schemaVer int64, relatedTableIDs []int64) checkResult {
	s.mux.RLock()
	defer s.mux.RUnlock()
	if !s.isStarted {
		log.Infof("the schema validator stopped before checking")
		return ResultFail
	}
	if s.lease == 0 {
		return ResultSucc
	}

	// Schema changed, result decided by whether related tables change.
	if schemaVer < s.latestSchemaVer {
		if s.isRelatedTablesChanged(schemaVer, relatedTableIDs) {
			return ResultFail
		}
		return ResultSucc
	}

	// Schema unchanged, maybe success or the schema validator is unavailable.
	t := extractPhysicalTime(txnTS)
	if t.After(s.latestSchemaExpire) {
		return ResultUnknown
	}
	return ResultSucc
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
