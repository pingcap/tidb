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

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	fieldtypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

func TestBootstrapInlineSPFreshIndexesUsesCreateTableSnapshotAndIndexIDs(t *testing.T) {
	tbInfo := newSPFreshBootstrapTestTableInfo()
	job := &model.Job{
		ID:          1002,
		RealStartTS: 11223344,
		ReorgMeta:   &model.DDLReorgMeta{},
	}
	indexIDs := collectInlineSPFreshVectorIndexes(tbInfo)

	var (
		gotBaseSnapshotTS []uint64
		gotIndexIDs       []int64
		gotWorkerAddr     string
	)
	store := createMockStore(t)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	origResolve := resolveSPFreshWorkerTargets
	resolveSPFreshWorkerTargets = func(context.Context, kv.Storage) (spfreshWorkerTargets, error) {
		return spfreshWorkerTargets{clusterID: 7, keyspaceID: uint32(store.GetCodec().GetKeyspaceID()), workerAddrs: []string{"http://127.0.0.1:20180"}}, nil
	}
	t.Cleanup(func() { resolveSPFreshWorkerTargets = origResolve })
	origBootstrap := bootstrapSPFreshReadyIndexes
	origCfg := config.GetGlobalConfig()
	newCfg := *origCfg
	newCfg.TiKVAPIServiceAddrs = []string{"127.0.0.1:20180"}
	config.StoreGlobalConfig(&newCfg)
	bootstrapSPFreshReadyIndexes = func(_ context.Context, _ kv.Storage, workerAddr string, _ *model.TableInfo, indexIDs []int64, baseSnapshotTS uint64) error {
		gotBaseSnapshotTS = append(gotBaseSnapshotTS, baseSnapshotTS)
		gotIndexIDs = append([]int64(nil), indexIDs...)
		gotWorkerAddr = workerAddr
		return nil
	}
	t.Cleanup(func() {
		bootstrapSPFreshReadyIndexes = origBootstrap
		config.StoreGlobalConfig(origCfg)
	})

	require.NoError(t, bootstrapInlineSPFreshIndexes(context.Background(), store, tbInfo, job, indexIDs))
	require.NoError(t, bootstrapInlineSPFreshIndexes(context.Background(), store, tbInfo, job, indexIDs))

	require.Equal(t, []uint64{22334455, 22334455}, gotBaseSnapshotTS)
	require.Equal(t, []int64{11, 13}, gotIndexIDs)
	require.Equal(t, "http://127.0.0.1:20180", gotWorkerAddr)
}

func newSPFreshBootstrapTestTableInfo() *model.TableInfo {
	vecType1 := fieldtypes.NewFieldType(mysql.TypeTiDBVectorFloat32)
	vecType1.SetFlen(3)
	vecType2 := fieldtypes.NewFieldType(mysql.TypeTiDBVectorFloat32)
	vecType2.SetFlen(4)
	runtimeConfig := model.DefaultSPFreshVectorRuntimeConfig()

	return &model.TableInfo{
		ID:       101,
		Name:     pmodel.NewCIStr("t"),
		State:    model.StatePublic,
		UpdateTS: 22334455,
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      pmodel.NewCIStr("id"),
				Offset:    0,
				State:     model.StatePublic,
				FieldType: *fieldtypes.NewFieldType(mysql.TypeLonglong),
			},
			{
				ID:        2,
				Name:      pmodel.NewCIStr("vec1"),
				Offset:    1,
				State:     model.StatePublic,
				FieldType: *vecType1,
			},
			{
				ID:        3,
				Name:      pmodel.NewCIStr("vec2"),
				Offset:    2,
				State:     model.StatePublic,
				FieldType: *vecType2,
			},
		},
		Indices: []*model.IndexInfo{
			{
				ID:    11,
				Name:  pmodel.NewCIStr("idx_sp_1"),
				Table: pmodel.NewCIStr("t"),
				State: model.StatePublic,
				Tp:    pmodel.IndexTypeSPFresh,
				Columns: []*model.IndexColumn{
					{Name: pmodel.NewCIStr("vec1"), Offset: 1, Length: fieldtypes.UnspecifiedLength},
				},
				VectorInfo: &model.VectorIndexInfo{
					Kind:           model.VectorIndexKindSPFresh,
					Dimension:      3,
					DistanceMetric: model.DistanceMetricCosine,
					RuntimeConfig:  &runtimeConfig,
				},
			},
			{
				ID:    12,
				Name:  pmodel.NewCIStr("idx_plain"),
				Table: pmodel.NewCIStr("t"),
				State: model.StatePublic,
				Tp:    pmodel.IndexTypeBtree,
				Columns: []*model.IndexColumn{
					{Name: pmodel.NewCIStr("id"), Offset: 0, Length: fieldtypes.UnspecifiedLength},
				},
			},
			{
				ID:    13,
				Name:  pmodel.NewCIStr("idx_sp_2"),
				Table: pmodel.NewCIStr("t"),
				State: model.StatePublic,
				Tp:    pmodel.IndexTypeSPFresh,
				Columns: []*model.IndexColumn{
					{Name: pmodel.NewCIStr("vec2"), Offset: 2, Length: fieldtypes.UnspecifiedLength},
				},
				VectorInfo: &model.VectorIndexInfo{
					Kind:           model.VectorIndexKindSPFresh,
					Dimension:      4,
					DistanceMetric: model.DistanceMetricL2,
					RuntimeConfig:  &runtimeConfig,
				},
			},
		},
	}
}
