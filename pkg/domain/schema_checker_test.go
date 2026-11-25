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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/infoschema/isvalidator"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func TestSchemaCheckerSimple(t *testing.T) {
	lease := 5 * time.Millisecond
	validator := isvalidator.New(lease)
	checker := &SchemaChecker{Validator: validator, needCheckSchemaByDelta: true}

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

	// Use checker.schemaValidator.Check instead of checker.Check here because backoff make CI slow.
	nowTS := uint64(time.Now().UnixNano())
	_, result := checker.Validator.Check(nowTS, checker.schemaVer, checker.relatedTableIDs, true)
	require.Equal(t, validatorapi.ResultUnknown, result)

	t.Run("delta check IDs and MDL disable fallback", func(t *testing.T) {
		origMDL := vardef.IsMDLEnabled()
		t.Cleanup(func() {
			vardef.SetEnableMDL(origMDL)
		})

		v := isvalidator.New(time.Second)
		vts := uint64(time.Now().UnixNano())
		// Seed an older delta at version 10 so schemaVer 10 stays within the retained delta window.
		v.Update(vts, 0, 10, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{99}, ActionTypes: []uint64{1}})
		// Table 20 changed between schemaVer 10 -> 12; table 10 (system-related) did not.
		v.Update(vts, 10, 12, &transaction.RelatedSchemaChange{PhyTblIDS: []int64{20}, ActionTypes: []uint64{1}})

		// Constructed while MDL is enabled: checks only skippedMDLTableIDs ({10}), ignoring MDL-locked {20}.
		vardef.SetEnableMDL(true)
		sc := NewSchemaCheckerWithSkippedMDL(v, 10, []int64{10, 20}, []int64{10})
		_, checkErr := sc.Check(vts)
		require.NoError(t, checkErr)

		// In nextgen kernel, MDL is permanently enabled and IsMDLEnabled always returns true.
		if kerneltype.IsClassic() {
			// If MDL is disabled before Check runs, it falls back to relatedTableIDs ({10, 20}) and catches table 20.
			vardef.SetEnableMDL(false)
			_, checkErr = sc.Check(vts)
			require.True(t, terror.ErrorEqual(checkErr, ErrInfoSchemaChanged))
		}
	})
}
