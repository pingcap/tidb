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

package ddl

import (
	"bytes"
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
)

func TestFlashbackMaskingPolicyKeyRanges(t *testing.T) {
	excluded := addToSlice("mysql", "user", 41, nil)
	excluded = addToSlice("mysql", "tidb_masking_policy", 42, excluded)
	ranges := getTableDataKeyRanges(excluded)
	contains := func(id int64) bool {
		key := tablecodec.EncodeTablePrefix(id)
		for _, r := range ranges {
			if bytes.Compare(key, r.StartKey) >= 0 && bytes.Compare(key, r.EndKey) < 0 {
				return true
			}
		}
		return false
	}
	require.False(t, contains(41))
	require.True(t, contains(42), "policy rows must flash back with their table/column ID bindings")
	require.True(t, contains(43))
}

func TestFlashbackChecksEveryTargetOfRenameTables(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	const sysDB, userDB, otherUserDB = int64(1), int64(2), int64(3)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		for id, name := range map[int64]string{sysDB: "mysql", userDB: "test", otherUserDB: "test2"} {
			if err := m.CreateDatabase(&model.DBInfo{ID: id, Name: pmodel.NewCIStr(name), State: model.StatePublic}); err != nil {
				return err
			}
		}
		return nil
	}))
	renameTables := func(targets ...int64) *model.SchemaDiff {
		diff := &model.SchemaDiff{Type: model.ActionRenameTables, SchemaID: targets[0], OldSchemaID: userDB, TableID: 100}
		for i, target := range targets[1:] {
			diff.AffectedOpts = append(diff.AffectedOpts, &model.AffectedOption{
				SchemaID: target, OldSchemaID: userDB, TableID: int64(101 + i), OldTableID: int64(101 + i),
			})
		}
		return diff
	}
	require.NoError(t, kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		reader := meta.NewReader(txn)
		// Only the first table of a multi-table rename has its target schema in
		// SchemaID. A later table moved into a system schema must be seen too.
		require.NoError(t, checkSystemSchemaDiff(reader, renameTables(userDB, otherUserDB, userDB), "ts"))
		require.ErrorContains(t, checkSystemSchemaDiff(reader, renameTables(sysDB, userDB), "ts"), "Detected modified system table")
		require.ErrorContains(t, checkSystemSchemaDiff(reader, renameTables(userDB, sysDB), "ts"), "Detected modified system table")
		require.ErrorContains(t, checkSystemSchemaDiff(reader, renameTables(userDB, otherUserDB, sysDB), "ts"), "Detected modified system table")
		return nil
	}))
}
