// Copyright 2026 PingCAP, Inc.
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

package mviewutil

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestResolveRefreshSources(t *testing.T) {
	for _, tc := range []string{"all logs", "missing logs", "missing source"} {
		t.Run(tc, func(t *testing.T) {
			a := &model.TableInfo{ID: 1, Name: pmodel.NewCIStr("a"), MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: 11}}
			b := &model.TableInfo{ID: 2, Name: pmodel.NewCIStr("b"), MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: 12}}
			c := &model.TableInfo{ID: 3, Name: pmodel.NewCIStr("c")}
			mv := &model.TableInfo{ID: 100, Name: pmodel.NewCIStr("mv"), MaterializedView: &model.MaterializedViewInfo{BaseTableIDs: []int64{2, 1}}}
			tables := []*model.TableInfo{a, b, c, mv}
			switch tc {
			case "missing logs":
				mv.MaterializedView.BaseTableIDs = []int64{3, 2, 1}
				b.MaterializedViewBase = nil
			case "missing source":
				tables = []*model.TableInfo{a, mv}
			}
			is := infoschema.MockInfoSchema(tables)
			sources, err := ResolveRefreshSources(context.Background(), is, mv.ID)
			if tc != "all logs" && tc != "missing logs" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			logs := []int64{11, 12}
			if tc == "missing logs" {
				logs = []int64{0, 11}
			}
			require.Equal(t, logs, RefreshSourceLogIDs(sources))
			require.Equal(t, int64(1), sources[0].TableID)
			require.Equal(t, int64(2), sources[1].TableID)
		})
	}
}
