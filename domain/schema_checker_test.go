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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/store/tikv"
)

func (s *testSuite) TestSchemaCheckerSimple(c *C) {
	lease := 5 * time.Millisecond
	validator := NewSchemaValidator(lease, nil)
	checker := &SchemaChecker{SchemaValidator: validator}

	// Add some schema versions and delta table IDs.
	ts := uint64(time.Now().UnixNano())
	validator.Update(ts, 0, 2, &tikv.RelatedSchemaChange{PhyTblIDS: []int64{1}, ActionTypes: []uint64{1}})
	validator.Update(ts, 2, 4, &tikv.RelatedSchemaChange{PhyTblIDS: []int64{2}, ActionTypes: []uint64{2}})

	// checker's schema version is the same as the current schema version.
	checker.schemaVer = 4
	_, err := checker.Check(ts)
	c.Assert(err, IsNil)

	// checker's schema version is less than the current schema version, and it doesn't exist in validator's items.
	// checker's related table ID isn't in validator's changed table IDs.
	checker.schemaVer = 2
	checker.relatedTableIDs = []int64{3}
	_, err = checker.Check(ts)
	c.Assert(err, IsNil)
	// The checker's schema version isn't in validator's items.
	checker.schemaVer = 1
	checker.relatedTableIDs = []int64{3}
	_, err = checker.Check(ts)
	c.Assert(terror.ErrorEqual(err, ErrInfoSchemaChanged), IsTrue)
	// checker's related table ID is in validator's changed table IDs.
	checker.relatedTableIDs = []int64{2}
	_, err = checker.Check(ts)
	c.Assert(terror.ErrorEqual(err, ErrInfoSchemaChanged), IsTrue)

	// validator's latest schema version is expired.
	time.Sleep(lease + time.Microsecond)
	checker.schemaVer = 4
	checker.relatedTableIDs = []int64{3}
	_, err = checker.Check(ts)
	c.Assert(err, IsNil)
	nowTS := uint64(time.Now().UnixNano())
	// Use checker.SchemaValidator.Check instead of checker.Check here because backoff make CI slow.
	_, result := checker.SchemaValidator.Check(nowTS, checker.schemaVer, checker.relatedTableIDs)
	c.Assert(result, Equals, ResultUnknown)
}
