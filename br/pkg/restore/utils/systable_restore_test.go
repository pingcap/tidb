// Copyright 2025 PingCAP, Inc.
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

package utils_test

import (
	"testing"

	"github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/stretchr/testify/require"
)

// NOTICE: Once there is a new system table, BR needs to ensure that it is correctly classified:
//
// - IF it is an unrecoverable table, please add the table name into `unRecoverableTable`.
// - IF it is a system privilege table, please add the table name into `sysPrivilegeTableMap`.
// - IF it is a statistics table, please add the table name into `statsTables`.
//

// The above variables are in the file br/pkg/restore/utils/systable_restore.go
func TestMonitorTheSystemTableIncremental(t *testing.T) {
	require.Equal(t, int64(245), session.CurrentBootstrapVersion)
}

// TestSysTablesCount ensures we maintain the correct count of system tables
// in the sysTables map. If this test fails after adding or removing tables,
// update the expected count accordingly.
func TestSysTablesCount(t *testing.T) {
	const expectedSysTablesCount = 59
	actualCount := len(utils.GetSysTables()["mysql"])
	require.Equal(t, expectedSysTablesCount, actualCount, "System tables count should be %d, currently have %d tables",
		expectedSysTablesCount, actualCount)
}
