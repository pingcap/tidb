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

	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

type flashbackSPFreshMetaReader struct {
	meta.Reader
	dbs    map[int64]*model.DBInfo
	tables map[flashbackSchemaTableID]*model.TableInfo
}

type flashbackSchemaTableID struct {
	schemaID int64
	tableID  int64
}

func newFlashbackSPFreshMetaReader(schemaID int64, tables ...*model.TableInfo) *flashbackSPFreshMetaReader {
	reader := &flashbackSPFreshMetaReader{
		dbs: map[int64]*model.DBInfo{
			schemaID: {
				ID:    schemaID,
				Name:  pmodel.NewCIStr("test"),
				State: model.StatePublic,
			},
		},
		tables: make(map[flashbackSchemaTableID]*model.TableInfo, len(tables)),
	}
	for _, tblInfo := range tables {
		reader.tables[flashbackSchemaTableID{schemaID: schemaID, tableID: tblInfo.ID}] = tblInfo
	}
	return reader
}

func newEmptyFlashbackSPFreshMetaReader(schemaID int64) *flashbackSPFreshMetaReader {
	return newFlashbackSPFreshMetaReader(schemaID)
}

func (r *flashbackSPFreshMetaReader) GetDatabase(dbID int64) (*model.DBInfo, error) {
	return r.dbs[dbID], nil
}

func (r *flashbackSPFreshMetaReader) ListDatabases() ([]*model.DBInfo, error) {
	dbs := make([]*model.DBInfo, 0, len(r.dbs))
	for _, dbInfo := range r.dbs {
		dbs = append(dbs, dbInfo)
	}
	return dbs, nil
}

func (r *flashbackSPFreshMetaReader) GetTable(dbID int64, tableID int64) (*model.TableInfo, error) {
	return r.tables[flashbackSchemaTableID{schemaID: dbID, tableID: tableID}], nil
}

func (r *flashbackSPFreshMetaReader) ListTables(_ context.Context, dbID int64) ([]*model.TableInfo, error) {
	var tables []*model.TableInfo
	for key, tblInfo := range r.tables {
		if key.schemaID == dbID {
			tables = append(tables, tblInfo)
		}
	}
	return tables, nil
}

func (r *flashbackSPFreshMetaReader) IterTables(dbID int64, fn func(info *model.TableInfo) error) error {
	tables, err := r.ListTables(context.Background(), dbID)
	if err != nil {
		return err
	}
	for _, tblInfo := range tables {
		if err := fn(tblInfo); err != nil {
			return err
		}
	}
	return nil
}

func spfreshFlashbackTable(tableID int64) *model.TableInfo {
	return &model.TableInfo{
		ID:    tableID,
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
		Indices: []*model.IndexInfo{{
			ID:    11,
			Name:  pmodel.NewCIStr("idx_vec"),
			Tp:    pmodel.IndexTypeSPFresh,
			State: model.StatePublic,
			VectorInfo: &model.VectorIndexInfo{
				Kind: model.VectorIndexKindSPFresh,
			},
		}},
	}
}

func normalFlashbackTable(tableID int64) *model.TableInfo {
	return &model.TableInfo{
		ID:    tableID,
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
	}
}

func TestCheckFlashbackClusterSPFreshVectorIndexesRejectsCurrentMeta(t *testing.T) {
	current := newFlashbackSPFreshMetaReader(1, spfreshFlashbackTable(100))
	target := newEmptyFlashbackSPFreshMetaReader(1)
	err := checkFlashbackClusterSPFreshVectorIndexes(context.Background(), current, target, "2026-04-29 00:00:00")
	require.ErrorContains(t, err, "SPFresh vector index")
	require.ErrorContains(t, err, "current schema")
	require.ErrorContains(t, err, "can't do flashback")
}

func TestCheckFlashbackClusterSPFreshVectorIndexesRejectsTargetMeta(t *testing.T) {
	current := newEmptyFlashbackSPFreshMetaReader(1)
	target := newFlashbackSPFreshMetaReader(1, spfreshFlashbackTable(100))
	err := checkFlashbackClusterSPFreshVectorIndexes(context.Background(), current, target, "2026-04-29 00:00:00")
	require.ErrorContains(t, err, "SPFresh vector index")
	require.ErrorContains(t, err, "flashback target schema")
}

func TestCheckFlashbackClusterSPFreshVectorIndexesAllowsNonSPFreshMetas(t *testing.T) {
	current := newFlashbackSPFreshMetaReader(1, normalFlashbackTable(100))
	target := newFlashbackSPFreshMetaReader(1, normalFlashbackTable(101))
	err := checkFlashbackClusterSPFreshVectorIndexes(context.Background(), current, target, "2026-04-29 00:00:00")
	require.NoError(t, err)
}

type cancelDuringSPFreshScanMetaReader struct {
	meta.Reader
	cancel context.CancelFunc
	calls  int
}

func (r *cancelDuringSPFreshScanMetaReader) ListDatabases() ([]*model.DBInfo, error) {
	return []*model.DBInfo{{
		ID:    1,
		Name:  pmodel.NewCIStr("test"),
		State: model.StatePublic,
	}}, nil
}

func (r *cancelDuringSPFreshScanMetaReader) IterTables(_ int64, fn func(info *model.TableInfo) error) error {
	tables := []*model.TableInfo{
		normalFlashbackTable(100),
		normalFlashbackTable(101),
		spfreshFlashbackTable(102),
	}
	for _, tblInfo := range tables {
		r.calls++
		if err := fn(tblInfo); err != nil {
			return err
		}
		if r.calls == 1 {
			r.cancel()
		}
	}
	return nil
}

func TestCheckFlashbackClusterSPFreshVectorIndexesStopsWhenContextCanceledDuringScan(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	current := &cancelDuringSPFreshScanMetaReader{cancel: cancel}
	target := newEmptyFlashbackSPFreshMetaReader(1)

	err := checkFlashbackClusterSPFreshVectorIndexes(ctx, current, target, "2026-04-29 00:00:00")

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, current.calls)
}
