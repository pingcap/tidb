// Copyright 2024 PingCAP, Inc.
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

package analyzehelper

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TriggerPredicateColumnsCollection triggers the collection of predicate columns for the specified table.
func TriggerPredicateColumnsCollection(t *testing.T, tk *testkit.TestKit, store kv.Storage, tableName string, columns ...string) {
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	statsHandle := do.StatsHandle()

	// Execute a SELECT statement for each column in the provided list to trigger a predicate column collection.
	for _, column := range columns {
		query := fmt.Sprintf("SELECT * FROM %s WHERE %s = '1'", tableName, column)
		tk.MustExec(query)
	}

	// After executing the SELECT statements, dump the column statistics usage to KV.
	require.NoError(t, statsHandle.DumpColStatsUsageToKV())
}
