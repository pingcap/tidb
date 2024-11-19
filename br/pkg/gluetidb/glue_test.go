// Copyright 2023 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestTheSessionIsoation(t *testing.T) {
	req := require.New(t)
	store, dom := session.CreateStoreAndBootstrap(t)
	ctx := context.Background()

	// we want to test glue start domain explicitly, so close it first.
	dom.Close()
	g := New()
	glueSe, err := g.CreateSession(store)
	req.NoError(err)
	t.Cleanup(func() {
		existDom, _ := session.GetDomain(nil)
		if existDom != nil {
			existDom.Close()
		}
	})

	require.NoError(t, glueSe.CreateDatabase(ctx, &model.DBInfo{
		Name: pmodel.NewCIStr("test_db"),
	}))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test_db")
	tk.MustExec("create table t(id int)")

	req.NoError(glueSe.ExecuteInternal(ctx, "use test;"))
	infos := []*model.TableInfo{}
	infos = append(infos, &model.TableInfo{
		Name: pmodel.NewCIStr("tables_1"),
		Columns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("foo"), FieldType: *types.NewFieldType(types.KindBinaryLiteral), State: model.StatePublic},
		},
	})
	infos = append(infos, &model.TableInfo{
		Name: pmodel.NewCIStr("tables_2"),
		PlacementPolicyRef: &model.PolicyRefInfo{
			Name: pmodel.NewCIStr("threereplication"),
		},
		Columns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("foo"), FieldType: *types.NewFieldType(types.KindBinaryLiteral), State: model.StatePublic},
		},
	})
	infos = append(infos, &model.TableInfo{
		Name: pmodel.NewCIStr("tables_3"),
		PlacementPolicyRef: &model.PolicyRefInfo{
			Name: pmodel.NewCIStr("fivereplication"),
		},
		Columns: []*model.ColumnInfo{
			{Name: pmodel.NewCIStr("foo"), FieldType: *types.NewFieldType(types.KindBinaryLiteral), State: model.StatePublic},
		},
	})
	polices := []*model.PolicyInfo{
		{
			PlacementSettings: &model.PlacementSettings{
				Followers: 4,
			},
			Name: pmodel.NewCIStr("fivereplication"),
		},
		{
			PlacementSettings: &model.PlacementSettings{
				Followers: 2,
			},
			Name: pmodel.NewCIStr("threereplication"),
		},
	}
	for _, pinfo := range polices {
		before := glueSe.(*tidbSession).se.GetInfoSchema().SchemaMetaVersion()
		req.NoError(glueSe.CreatePlacementPolicy(ctx, pinfo))
		after := glueSe.(*tidbSession).se.GetInfoSchema().SchemaMetaVersion()
		req.Greater(after, before)
	}
	req.NoError(glueSe.(glue.BatchCreateTableSession).CreateTables(ctx, map[string][]*model.TableInfo{
		"test": infos,
	}))
}
