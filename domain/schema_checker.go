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

package domain

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv"
)

// SchemaChecker is used for checking schema-validity.
type SchemaChecker struct {
	SchemaValidator
	schemaVer       int64
	relatedTableIDs []int64
}

type intSchemaVer int64

func (i intSchemaVer) SchemaMetaVersion() int64 {
	return int64(i)
}

var (
	// SchemaOutOfDateRetryInterval is the backoff time before retrying.
	SchemaOutOfDateRetryInterval = int64(500 * time.Millisecond)
	// SchemaOutOfDateRetryTimes is the max retry count when the schema is out of date.
	SchemaOutOfDateRetryTimes = int32(10)
)

// NewSchemaChecker creates a new schema checker.
func NewSchemaChecker(do *Domain, schemaVer int64, relatedTableIDs []int64) *SchemaChecker {
	return &SchemaChecker{
		SchemaValidator: do.SchemaValidator,
		schemaVer:       schemaVer,
		relatedTableIDs: relatedTableIDs,
	}
}

// Check checks the validity of the schema version.
func (s *SchemaChecker) Check(txnTS uint64) (*tikv.RelatedSchemaChange, error) {
	return s.CheckBySchemaVer(txnTS, intSchemaVer(s.schemaVer))
}

// CheckBySchemaVer checks if the schema version valid or not at txnTS.
func (s *SchemaChecker) CheckBySchemaVer(txnTS uint64, startSchemaVer tikv.SchemaVer) (*tikv.RelatedSchemaChange, error) {
	schemaOutOfDateRetryInterval := atomic.LoadInt64(&SchemaOutOfDateRetryInterval)
	schemaOutOfDateRetryTimes := int(atomic.LoadInt32(&SchemaOutOfDateRetryTimes))
	for i := 0; i < schemaOutOfDateRetryTimes; i++ {
		relatedChange, CheckResult := s.SchemaValidator.Check(txnTS, startSchemaVer.SchemaMetaVersion(), s.relatedTableIDs)
		switch CheckResult {
		case ResultSucc:
			return nil, nil
		case ResultFail:
			metrics.SchemaLeaseErrorCounter.WithLabelValues("changed").Inc()
			return relatedChange, ErrInfoSchemaChanged
		case ResultUnknown:
			time.Sleep(time.Duration(schemaOutOfDateRetryInterval))
		}

	}
	metrics.SchemaLeaseErrorCounter.WithLabelValues("outdated").Inc()
	return nil, ErrInfoSchemaExpired
}
