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
	detalItemInfos     *simpleQueue
}

// NewSchemaValidator returns a SchemaValidator structure.
func NewSchemaValidator(lease time.Duration) SchemaValidator {
	return &schemaValidator{
		isStarted:      true,
		lease:          lease,
		detalItemInfos: &simpleQueue{},
	}
}

func (s *schemaValidator) Stop() {
	log.Info("the schema validator stops")
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = false
	s.latestSchemaVer = 0
	s.detalItemInfos.reset()
}

func (s *schemaValidator) Restart() {
	log.Info("the schema validator restarts")
	s.mux.Lock()
	defer s.mux.Unlock()
	s.isStarted = true
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

	// Update the schema deltaItem information.
	if currVer != oldVer {
		log.Debug("update schema validator:", oldVer, currVer, changedTableIDs)
		s.detalItemInfos.enqueue(currVer, changedTableIDs)
	}
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
	q := s.detalItemInfos
	if q.head == q.tail {
		log.Infof("schema change history is empty, checking %d", currVer)
		return true
	}
	oldestVersion := q.data[q.head].schemaVersion
	if versionNewerThan(oldestVersion, currVer) {
		log.Infof("the schema version %d is much older than the latest version %d", currVer, s.latestSchemaVer)
		return true
	}

	for iter := q.iter(); iter != nil; iter = iter.next() {
		item := iter.value()
		if !versionNewerThan(item.schemaVersion, currVer) {
			break
		}

		if hasRelatedTableID(item.relatedTableIDs, tableIDs) {
			return true
		}
	}

	return false
}

func versionNewerThan(ver1, ver2 int64) bool {
	return ver1 > ver2
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

// simpleQueue is a fixed size queue, when queue is full, enqueue will overwrite the oldest.
// empty, initial state: head == tail
// queue full:  (tail+1) % size == head
// data in range [head, tail)
type simpleQueue struct {
	data [maxNumberOfDiffsToLoad]deltaSchemaInfo
	head int
	tail int
}

func (q *simpleQueue) enqueue(schemaVersion int64, relatedTableIDs []int64) {
	// Write to the queue tail.
	target := &q.data[q.tail]
	target.schemaVersion = schemaVersion
	target.relatedTableIDs = relatedTableIDs

	// Move tail cursor forward.
	q.tail = nextPos(q.tail)

	// If queue is full, head has been overwrite, need to forward it.
	if q.tail == q.head {
		q.head = nextPos(q.head)
	}
}

func nextPos(pos int) int {
	return (pos + 1) % maxNumberOfDiffsToLoad
}

func prevPos(pos int) int {
	return (pos + maxNumberOfDiffsToLoad - 1) % maxNumberOfDiffsToLoad
}

func (q *simpleQueue) reset() {
	q.head = 0
	q.tail = 0
}

// iter returns an iterator which could visit the queue from tail to head.
func (q *simpleQueue) iter() *simpleIter {
	if q.head == q.tail {
		return nil // empty queue
	}

	return &simpleIter{
		simpleQueue: q,
		pos:         prevPos(q.tail),
	}
}

type simpleIter struct {
	*simpleQueue
	pos int
}

func (iter *simpleIter) next() *simpleIter {
	if iter.pos == iter.simpleQueue.head {
		return nil
	}
	iter.pos = prevPos(iter.pos)
	return iter
}

func (iter *simpleIter) value() *deltaSchemaInfo {
	q := iter.simpleQueue
	return &q.data[iter.pos]
}
