// Copyright 2022 PingCAP, Inc.
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

package session_test

import (
	"reflect"
	"testing"

	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestInitMetaTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, sql := range session.DDLJobTables {
		tk.MustExec(sql)
	}

	tbls := map[string]struct{}{
		"tidb_ddl_job":     {},
		"tidb_ddl_reorg":   {},
		"tidb_ddl_history": {},
	}

	for tbl := range tbls {
		metaInMySQL := external.GetTableByName(t, tk, "mysql", tbl).Meta()
		metaInTest := external.GetTableByName(t, tk, "test", tbl).Meta()

		require.Greater(t, metaInMySQL.ID, int64(0))
		require.Greater(t, metaInMySQL.UpdateTS, uint64(0))

		metaInTest.ID = metaInMySQL.ID
		metaInMySQL.UpdateTS = metaInTest.UpdateTS
		require.True(t, reflect.DeepEqual(metaInMySQL, metaInTest))
	}
}
