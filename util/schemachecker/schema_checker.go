// Copyright 2018 PingCAP, Inc.
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

package schemachecker

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/metrics"
)

// SchemaLeaseChecker is used for checking schema-validity.
type SchemaLeaseChecker struct {
	domain.SchemaValidator
	schemaVer       int64
	relatedTableIDs []int64
}

var (
	// SchemaOutOfDateRetryInterval is the sleeping time when we fail to try.
	SchemaOutOfDateRetryInterval = int64(500 * time.Millisecond)
	// SchemaOutOfDateRetryTimes is upper bound of retry times when the schema is out of date.
	SchemaOutOfDateRetryTimes = int32(10)
)

// NewSchemaChecker creates a new schema checker.
func NewSchemaChecker(do *domain.Domain, schemaVer int64, relatedTableIDs []int64) *SchemaLeaseChecker {
	return &SchemaLeaseChecker{
		SchemaValidator: do.SchemaValidator,
		schemaVer:       schemaVer,
		relatedTableIDs: relatedTableIDs,
	}
}

// Check checks the validity of the schema version.
func (s *SchemaLeaseChecker) Check(txnTS uint64) error {
	schemaOutOfDateRetryInterval := atomic.LoadInt64(&SchemaOutOfDateRetryInterval)
	schemaOutOfDateRetryTimes := int(atomic.LoadInt32(&SchemaOutOfDateRetryTimes))
	for i := 0; i < schemaOutOfDateRetryTimes; i++ {
		result := s.SchemaValidator.Check(txnTS, s.schemaVer, s.relatedTableIDs)
		switch result {
		case domain.ResultSucc:
			return nil
		case domain.ResultFail:
			metrics.SchemaLeaseErrorCounter.WithLabelValues("changed").Inc()
			return domain.ErrInfoSchemaChanged
		case domain.ResultUnknown:
			metrics.SchemaLeaseErrorCounter.WithLabelValues("outdated").Inc()
			time.Sleep(time.Duration(schemaOutOfDateRetryInterval))
		}

	}
	return domain.ErrInfoSchemaExpired
}
