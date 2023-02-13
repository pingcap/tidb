// Copyright 2021 PingCAP, Inc.
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

package gluetidb

import (
	"context"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestBatchCreateTables(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_1")

	d := dom.DDL()
	require.NotNil(t, d)

	infos1 := []*model.TableInfo{}
	infos1 = append(infos1, &model.TableInfo{
		ID:   124,
		Name: model.NewCIStr("table_id_resued"),
	})

	se := &tidbSession{se: tk.Session()}

	// keep/reused table id verification
	tk.Session().SetValue(sessionctx.QueryString, "skip")

	err := se.SplitBatchCreateTable(model.NewCIStr("test"), infos1, ddl.AllocTableIDIf(func(ti *model.TableInfo) bool {
		return false
	}))
	require.NoError(t, err)

	tk.MustQuery("select tidb_table_id from information_schema.tables where table_name = 'table_id_resued'").Check(testkit.Rows("124"))
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)

	// allocate new table id verification
	// query the global id
	var id int64
	err = kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		var err error
		id, err = m.GenGlobalID()
		return err
	})

	require.NoError(t, err)

	infos2 := []*model.TableInfo{}
	infos2 = append(infos2, &model.TableInfo{
		ID:   124,
		Name: model.NewCIStr("table_id_new"),
	})

	tk.Session().SetValue(sessionctx.QueryString, "skip")

	err = se.SplitBatchCreateTable(model.NewCIStr("test"), infos2, ddl.AllocTableIDIf(func(ti *model.TableInfo) bool {
		return true
	}))
	require.NoError(t, err)

	idGen, ok := tk.MustQuery("select tidb_table_id from information_schema.tables where table_name = 'table_id_new'").Rows()[0][0].(string)
	require.True(t, ok)
	idGenNum, err := strconv.ParseInt(idGen, 10, 64)
	require.NoError(t, err)
	require.Greater(t, idGenNum, id)
}
