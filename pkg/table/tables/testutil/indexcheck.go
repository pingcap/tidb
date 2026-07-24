// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// CheckIndexKVCount checks the number of index key-value pairs in the specified index of the specified table.
func CheckIndexKVCount(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, tableName string, indexName string, expected int) {
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr(tableName))
	require.NoError(t, err)
	require.NotNil(t, tbl)
	var idx table.Index
	for _, i := range tbl.Indices() {
		if i.Meta().Name.L == indexName {
			idx = i
			break
		}
	}

	minimumKey, _, err := tablecodec.GenIndexKey(time.Local, tbl.Meta(), idx.Meta(), tbl.Meta().ID, nil, nil, nil)
	require.NoError(t, err)

	tk.MustExec("BEGIN")
	defer tk.MustExec("COMMIT")
	txnManager := sessiontxn.GetTxnManager(tk.Session())
	snapshot, err := txnManager.GetSnapshotWithStmtReadTS()
	require.NoError(t, err)

	iter, err := snapshot.Iter(minimumKey, nil)
	require.NoError(t, err)
	defer iter.Close()
	count := 0
	for iter.Valid() {
		key := iter.Key()
		if !tablecodec.IsIndexKey(key) {
			break
		}
		_, idxID, _, err := tablecodec.DecodeIndexKey(key)
		require.NoError(t, err)
		if idxID != idx.Meta().ID {
			break
		}
		count++
		err = iter.Next()
		require.NoError(t, err)
	}
	require.Equal(t, expected, count)
}
