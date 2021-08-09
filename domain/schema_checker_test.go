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
	"testing"
	"time"

	"github.com/pingcap/parser/terror"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func TestSchemaCheckerSimple(t *testing.T) {
	t.Parallel()

	lease := 5 * time.Millisecond
	validator := NewSchemaValidator(lease, nil)
	checker := &SchemaChecker{SchemaValidator: validator}

	// Add some schema versions and delta table IDs.
	ts := uint64(time.Now().UnixNano())
	validator.Update(ts, 0, 2, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{1}, ActionTypes: []uint64{1}})
	validator.Update(ts, 2, 4, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{2}, ActionTypes: []uint64{2}})

	// checker's schema version is the same as the current schema version.
	checker.schemaVer = 4
	_, err := checker.Check(ts)
	require.NoError(t, err)

	// checker's schema version is less than the current schema version, and it doesn't exist in validator's items.
	// checker's related table ID isn't in validator's changed table IDs.
	checker.schemaVer = 2
	checker.relatedTableIDs = []int64{3}
	_, err = checker.Check(ts)
	require.NoError(t, err)

	// The checker's schema version isn't in validator's items.
	checker.schemaVer = 1
	checker.relatedTableIDs = []int64{3}
	_, err = checker.Check(ts)
	require.True(t, terror.ErrorEqual(err, ErrInfoSchemaChanged))

	// checker's related table ID is in validator's changed table IDs.
	checker.relatedTableIDs = []int64{2}
	_, err = checker.Check(ts)
	require.True(t, terror.ErrorEqual(err, ErrInfoSchemaChanged))

	// validator's latest schema version is expired.
	time.Sleep(lease + time.Microsecond)
	checker.schemaVer = 4
	checker.relatedTableIDs = []int64{3}
	_, err = checker.Check(ts)
	require.NoError(t, err)

	// Use checker.SchemaValidator.Check instead of checker.Check here because backoff make CI slow.
	nowTS := uint64(time.Now().UnixNano())
	_, result := checker.SchemaValidator.Check(nowTS, checker.schemaVer, checker.relatedTableIDs)
	require.Equal(t, ResultUnknown, result)
}
