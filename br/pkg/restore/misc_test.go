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

package restore_test

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestGetExistedUserDBs(t *testing.T) {
	m, err := mock.NewCluster()
	require.Nil(t, err)
	defer m.Stop()
	dom := m.Domain

	dbs := restore.GetExistedUserDBs(dom)
	require.Equal(t, 0, len(dbs))

	builder, err := infoschema.NewBuilder(dom, nil, nil).InitWithDBInfos(
		[]*model.DBInfo{
			{Name: model.NewCIStr("mysql")},
			{Name: model.NewCIStr("test")},
		},
		nil, nil, 1)
	require.Nil(t, err)
	dom.MockInfoCacheAndLoadInfoSchema(builder.Build(math.MaxUint64))
	dbs = restore.GetExistedUserDBs(dom)
	require.Equal(t, 0, len(dbs))

	builder, err = infoschema.NewBuilder(dom, nil, nil).InitWithDBInfos(
		[]*model.DBInfo{
			{Name: model.NewCIStr("mysql")},
			{Name: model.NewCIStr("test")},
			{Name: model.NewCIStr("d1")},
		},
		nil, nil, 1)
	require.Nil(t, err)
	dom.MockInfoCacheAndLoadInfoSchema(builder.Build(math.MaxUint64))
	dbs = restore.GetExistedUserDBs(dom)
	require.Equal(t, 1, len(dbs))

	builder, err = infoschema.NewBuilder(dom, nil, nil).InitWithDBInfos(
		[]*model.DBInfo{
			{Name: model.NewCIStr("mysql")},
			{Name: model.NewCIStr("d1")},
			{
				Name:   model.NewCIStr("test"),
				Tables: []*model.TableInfo{{ID: 1, Name: model.NewCIStr("t1"), State: model.StatePublic}},
				State:  model.StatePublic,
			},
		},
		nil, nil, 1)
	require.Nil(t, err)
	dom.MockInfoCacheAndLoadInfoSchema(builder.Build(math.MaxUint64))
	dbs = restore.GetExistedUserDBs(dom)
	require.Equal(t, 2, len(dbs))
}
