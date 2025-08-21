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

package sessiontest

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestGCOldVersion(t *testing.T) {
	defer config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.CoprCache.CapacityMB = 0
	})
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_schema_cache_size = 512 * 1024 * 1024")
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t1 (id int key, b int)")
	tk.MustExec("create table t2 (id int key, b int)")
	tk.MustExec("create table t3 (id int key, b int)")

	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	oldIS := dom.InfoSchema()

	t1, err := oldIS.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	t2, err := oldIS.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	t3, err := oldIS.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t3"))
	require.NoError(t, err)

	s := store.(helper.Storage)
	h := helper.NewHelper(s)
	old, err := meta.GetOldestSchemaVersion(h)
	require.NoError(t, err)

	for range 10 {
		tk.MustExec("alter table t1 add index i_b(b)")
		tk.MustExec("alter table t1 drop index i_b")
	}

	for range 10 {
		tk.MustExec("alter table t2 add column (c int)")
		tk.MustExec("alter table t2 drop column c")
	}

	for range 10 {
		tk.MustExec("truncate table t3")
	}

	nowIS := dom.InfoSchema()
	curr := nowIS.SchemaMetaVersion()
	require.True(t, curr > old)

	ok, v2 := infoschema.IsV2(oldIS)
	require.True(t, ok)

	// After GC, the related table item are deleted.
	deleted, _ := v2.GCOldVersion(curr - 5)
	require.True(t, deleted > 0)

	// So TableByID using old ID with the old schema version would fail.
	_, ok = oldIS.TableByID(context.Background(), t1.ID)
	require.False(t, ok)
	_, ok = oldIS.TableByID(context.Background(), t2.ID)
	require.False(t, ok)
	_, ok = oldIS.TableByID(context.Background(), t3.ID)
	require.False(t, ok)
	_, err = oldIS.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.Error(t, err)
	_, err = oldIS.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.Error(t, err)
	_, err = oldIS.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("t3"))
	require.Error(t, err)

	// GC will not delete the current schema version.
	// add index and add column does not change table id
	_, ok = nowIS.TableByID(context.Background(), t1.ID)
	require.True(t, ok)
	_, ok = nowIS.TableByID(context.Background(), t2.ID)
	require.True(t, ok)
	_, ok = nowIS.TableByID(context.Background(), t3.ID)
	// truncate table changes table id
	require.False(t, ok)
}
