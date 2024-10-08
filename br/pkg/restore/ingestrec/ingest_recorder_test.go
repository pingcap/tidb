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

package ingestrec_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

const (
	SchemaName string = "test_db"
	TableName  string = "test_tbl"
	TableID    int64  = 80
)

func fakeJob(reorgTp model.ReorgType, jobTp model.ActionType, state model.JobState,
	rowCnt int64, indices []*model.IndexInfo, rawArgs json.RawMessage) *model.Job {
	return &model.Job{
		Version:    model.JobVersion1,
		SchemaName: SchemaName,
		TableName:  TableName,
		TableID:    TableID,
		Type:       jobTp,
		State:      state,
		RowCount:   rowCnt,
		RawArgs:    rawArgs,
		ReorgMeta: &model.DDLReorgMeta{
			ReorgTp: reorgTp,
		},
		BinlogInfo: &model.HistoryInfo{
			TableInfo: &model.TableInfo{
				Indices: indices,
			},
		},
	}
}

func getIndex(id int64, columnsName []string) *model.IndexInfo {
	columns := make([]*model.IndexColumn, 0, len(columnsName))
	for _, columnName := range columnsName {
		columns = append(columns, &model.IndexColumn{
			Name: pmodel.CIStr{
				O: columnName,
				L: columnName,
			},
		})
	}
	return &model.IndexInfo{
		ID: id,
		Name: pmodel.CIStr{
			O: columnsName[0],
			L: columnsName[0], // noused
		},
		Columns: columns,
	}
}

type iterateFunc func(tableID, indexID int64, info *ingestrec.IngestIndexInfo) error

func noItem(tableID, indexID int64, info *ingestrec.IngestIndexInfo) error {
	return errors.Errorf("should no items, but have one: [%d, %d, %v]", tableID, indexID, info)
}

func hasOneItem(idxID int64, columnList string, columnArgs []any) (iterateFunc, *int) {
	count := 0
	return func(tableID, indexID int64, info *ingestrec.IngestIndexInfo) error {
		count += 1
		if indexID != idxID || info.ColumnList != columnList {
			return errors.Errorf("should has one items, but have another one: [%d, %d, %v]", tableID, indexID, info)
		}
		for i, arg := range info.ColumnArgs {
			if columnArgs[i] != arg {
				return errors.Errorf("should has one items, but have another one: [%d, %d, %v]", tableID, indexID, info)
			}
		}
		return nil
	}, &count
}

func createMeta(t *testing.T, store kv.Storage, fn func(m *meta.Mutator)) {
	txn, err := store.Begin()
	require.NoError(t, err)

	fn(meta.NewMutator(txn))

	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func TestAddIngestRecorder(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	createMeta(t, store, func(m *meta.Mutator) {
		dbInfo := &model.DBInfo{
			ID:    1,
			Name:  pmodel.NewCIStr(SchemaName),
			State: model.StatePublic,
		}
		err := m.CreateDatabase(dbInfo)
		require.NoError(t, err)
		tblInfo := &model.TableInfo{
			ID:   TableID,
			Name: pmodel.NewCIStr(TableName),
			Columns: []*model.ColumnInfo{
				{
					Name:   pmodel.NewCIStr("x"),
					Hidden: false,
					State:  model.StatePublic,
				},
				{
					Name:   pmodel.NewCIStr("y"),
					Hidden: false,
					State:  model.StatePublic,
				},
			},
			Indices: []*model.IndexInfo{
				{
					ID:    1,
					Name:  pmodel.NewCIStr("x"),
					Table: pmodel.NewCIStr(TableName),
					Columns: []*model.IndexColumn{
						{
							Name:   pmodel.NewCIStr("x"),
							Offset: 0,
							Length: -1,
						},
						{
							Name:   pmodel.NewCIStr("y"),
							Offset: 1,
							Length: -1,
						},
					},
					Comment: "123",
					Tp:      pmodel.IndexTypeBtree,
					State:   model.StatePublic,
				},
			},
			State: model.StatePublic,
		}
		err = m.CreateTableOrView(1, tblInfo)
		require.NoError(t, err)
	})
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	infoSchema := dom.InfoSchema()

	recorder := ingestrec.New()
	// no ingest job, should ignore it
	err = recorder.TryAddJob(fakeJob(
		model.ReorgTypeTxn,
		model.ActionAddIndex,
		model.JobStateSynced,
		100,
		[]*model.IndexInfo{
			getIndex(1, []string{"x", "y"}),
		},
		nil,
	), false)
	require.NoError(t, err)
	err = recorder.UpdateIndexInfo(infoSchema)
	require.NoError(t, err)
	err = recorder.Iterate(noItem)
	require.NoError(t, err)

	// no add-index job, should ignore it
	err = recorder.TryAddJob(fakeJob(
		model.ReorgTypeLitMerge,
		model.ActionDropIndex,
		model.JobStateSynced,
		100,
		[]*model.IndexInfo{
			getIndex(1, []string{"x", "y"}),
		},
		nil,
	), false)
	require.NoError(t, err)
	err = recorder.UpdateIndexInfo(infoSchema)
	require.NoError(t, err)
	err = recorder.Iterate(noItem)
	require.NoError(t, err)

	// no synced job, should ignore it
	err = recorder.TryAddJob(fakeJob(
		model.ReorgTypeLitMerge,
		model.ActionAddIndex,
		model.JobStateRollbackDone,
		100,
		[]*model.IndexInfo{
			getIndex(1, []string{"x", "y"}),
		},
		nil,
	), false)
	require.NoError(t, err)
	err = recorder.UpdateIndexInfo(infoSchema)
	require.NoError(t, err)
	err = recorder.Iterate(noItem)
	require.NoError(t, err)

	{
		recorder := ingestrec.New()
		// a normal ingest add index job
		err = recorder.TryAddJob(fakeJob(
			model.ReorgTypeLitMerge,
			model.ActionAddIndex,
			model.JobStateSynced,
			1000,
			[]*model.IndexInfo{
				getIndex(1, []string{"x", "y"}),
			},
			json.RawMessage(`[1, "a"]`),
		), false)
		require.NoError(t, err)
		f, cnt := hasOneItem(1, "%n,%n", []any{"x", "y"})
		err = recorder.UpdateIndexInfo(infoSchema)
		require.NoError(t, err)
		err = recorder.Iterate(f)
		require.NoError(t, err)
		require.Equal(t, *cnt, 1)
	}

	{
		recorder := ingestrec.New()
		// a normal ingest add primary index job
		err = recorder.TryAddJob(fakeJob(
			model.ReorgTypeLitMerge,
			model.ActionAddPrimaryKey,
			model.JobStateSynced,
			1000,
			[]*model.IndexInfo{
				getIndex(1, []string{"x", "y"}),
			},
			json.RawMessage(`[1, "a"]`),
		), false)
		require.NoError(t, err)
		f, cnt := hasOneItem(1, "%n,%n", []any{"x", "y"})
		err = recorder.UpdateIndexInfo(infoSchema)
		require.NoError(t, err)
		err = recorder.Iterate(f)
		require.NoError(t, err)
		require.Equal(t, *cnt, 1)
	}

	{
		// a sub job as add primary index job
		err = recorder.TryAddJob(fakeJob(
			model.ReorgTypeLitMerge,
			model.ActionAddPrimaryKey,
			model.JobStateDone,
			1000,
			[]*model.IndexInfo{
				getIndex(1, []string{"x", "y"}),
			},
			json.RawMessage(`[1, "a"]`),
		), true)
		require.NoError(t, err)
		f, cnt := hasOneItem(1, "%n,%n", []any{"x", "y"})
		err = recorder.UpdateIndexInfo(infoSchema)
		require.NoError(t, err)
		err = recorder.Iterate(f)
		require.NoError(t, err)
		require.Equal(t, *cnt, 1)
	}
}

func TestIndexesKind(t *testing.T) {
	//ctx := context.Background()
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()
	/*se, err := session.CreateSession(store)
	require.NoError(t, err)
	_, err := se.ExecuteInternal(ctx)
	*/
	createMeta(t, store, func(m *meta.Mutator) {
		dbInfo := &model.DBInfo{
			ID:    1,
			Name:  pmodel.NewCIStr(SchemaName),
			State: model.StatePublic,
		}
		err := m.CreateDatabase(dbInfo)
		require.NoError(t, err)
		tblInfo := &model.TableInfo{
			ID:   TableID,
			Name: pmodel.NewCIStr(TableName),
			Columns: []*model.ColumnInfo{
				{
					Name:   pmodel.NewCIStr("x"),
					Hidden: false,
					State:  model.StatePublic,
				},
				{
					Name:                pmodel.NewCIStr("_V$_x_0"),
					Hidden:              true,
					GeneratedExprString: "`x` * 2",
					State:               model.StatePublic,
				},
				{
					Name:   pmodel.NewCIStr("z"),
					Hidden: false,
					State:  model.StatePublic,
				},
			},
			Indices: []*model.IndexInfo{
				{
					ID:    1,
					Name:  pmodel.NewCIStr("x"),
					Table: pmodel.NewCIStr(TableName),
					Columns: []*model.IndexColumn{
						{
							Name:   pmodel.NewCIStr("x"),
							Offset: 0,
							Length: -1,
						},
						{
							Name:   pmodel.NewCIStr("_V$_x_0"),
							Offset: 1,
							Length: -1,
						},
						{
							Name:   pmodel.NewCIStr("z"),
							Offset: 2,
							Length: 4,
						},
					},
					Comment:   "123",
					Tp:        pmodel.IndexTypeHash,
					Invisible: true,
					State:     model.StatePublic,
				},
			},
			State: model.StatePublic,
		}
		err = m.CreateTableOrView(1, tblInfo)
		require.NoError(t, err)
	})
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	infoSchema := dom.InfoSchema()

	recorder := ingestrec.New()
	err = recorder.TryAddJob(fakeJob(
		model.ReorgTypeLitMerge,
		model.ActionAddIndex,
		model.JobStateSynced,
		1000,
		[]*model.IndexInfo{
			getIndex(1, []string{"x"}),
		},
		json.RawMessage(`[1, "a"]`),
	), false)
	require.NoError(t, err)
	err = recorder.UpdateIndexInfo(infoSchema)
	require.NoError(t, err)
	var (
		tableID int64
		indexID int64
		info    *ingestrec.IngestIndexInfo
		count   int = 0
	)
	recorder.Iterate(func(tblID, idxID int64, i *ingestrec.IngestIndexInfo) error {
		tableID = tblID
		indexID = idxID
		info = i
		count++
		return nil
	})
	require.Equal(t, 1, count)
	require.Equal(t, TableID, tableID)
	require.Equal(t, int64(1), indexID)
	require.Equal(t, pmodel.NewCIStr(SchemaName), info.SchemaName)
	require.Equal(t, "%n,(`x` * 2),%n(4)", info.ColumnList)
	require.Equal(t, []any{"x", "z"}, info.ColumnArgs)
	require.Equal(t, TableName, info.IndexInfo.Table.O)
}

func TestRewriteTableID(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	createMeta(t, store, func(m *meta.Mutator) {
		dbInfo := &model.DBInfo{
			ID:    1,
			Name:  pmodel.NewCIStr(SchemaName),
			State: model.StatePublic,
		}
		err := m.CreateDatabase(dbInfo)
		require.NoError(t, err)
		tblInfo := &model.TableInfo{
			ID:   TableID,
			Name: pmodel.NewCIStr(TableName),
			Columns: []*model.ColumnInfo{
				{
					Name:   pmodel.NewCIStr("x"),
					Hidden: false,
					State:  model.StatePublic,
				},
				{
					Name:   pmodel.NewCIStr("y"),
					Hidden: false,
					State:  model.StatePublic,
				},
			},
			Indices: []*model.IndexInfo{
				{
					ID:    1,
					Name:  pmodel.NewCIStr("x"),
					Table: pmodel.NewCIStr(TableName),
					Columns: []*model.IndexColumn{
						{
							Name:   pmodel.NewCIStr("x"),
							Offset: 0,
							Length: -1,
						},
						{
							Name:   pmodel.NewCIStr("y"),
							Offset: 1,
							Length: -1,
						},
					},
					Comment: "123",
					Tp:      pmodel.IndexTypeBtree,
					State:   model.StatePublic,
				},
			},
			State: model.StatePublic,
		}
		err = m.CreateTableOrView(1, tblInfo)
		require.NoError(t, err)
	})
	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	infoSchema := dom.InfoSchema()

	recorder := ingestrec.New()
	err = recorder.TryAddJob(fakeJob(
		model.ReorgTypeLitMerge,
		model.ActionAddIndex,
		model.JobStateSynced,
		1000,
		[]*model.IndexInfo{
			getIndex(1, []string{"x", "y"}),
		},
		json.RawMessage(`[1, "a"]`),
	), false)
	require.NoError(t, err)
	err = recorder.UpdateIndexInfo(infoSchema)
	require.NoError(t, err)
	recorder.RewriteTableID(func(tableID int64) (int64, bool, error) {
		return tableID + 1, false, nil
	})
	count := 0
	err = recorder.Iterate(func(tableID, indexID int64, info *ingestrec.IngestIndexInfo) error {
		count += 1
		require.Equal(t, TableID+1, tableID)
		return nil
	})
	require.NoError(t, err)
	recorder.RewriteTableID(func(tableID int64) (int64, bool, error) {
		count += 1
		return tableID + 1, true, nil
	})
	err = recorder.Iterate(noItem)
	require.NoError(t, err)
	require.Equal(t, 2, count)
}
