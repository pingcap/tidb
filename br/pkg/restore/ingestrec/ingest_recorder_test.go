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
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

const (
	SchemaName string = "test_db"
	TableName  string = "test_tbl"
	TableID    int64  = 80
)

func fakeJob(reorgTp model.ReorgType, jobTp model.ActionType, state model.JobState, rowCnt int64, indices []*model.IndexInfo, rawArgs json.RawMessage) *model.Job {
	return &model.Job{
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
			Name: model.CIStr{
				O: columnName,
				L: columnName,
			},
		})
	}
	return &model.IndexInfo{
		ID: id,
		Name: model.CIStr{
			O: columnsName[0],
			L: columnsName[0], // noused
		},
		Columns: columns,
	}
}

type iterateFunc func(tableID, indexID int64, info ingestrec.IngestIndexInfo) error

func noItem(tableID, indexID int64, info ingestrec.IngestIndexInfo) error {
	return errors.Errorf("should no items, but have one: [%d, %d, %v]", tableID, indexID, info)
}

func hasOneItem(idxID int64, columnList string) (iterateFunc, *int) {
	count := 0
	return func(tableID, indexID int64, info ingestrec.IngestIndexInfo) error {
		count += 1
		if indexID != idxID || info.ColumnList != columnList {
			return errors.Errorf("should has one items, but have another one: [%d, %d, %v]", tableID, indexID, info)
		}
		return nil
	}, &count
}

func TestAddIngestRecorder(t *testing.T) {
	recorder := ingestrec.New()
	// no ingest job, should ignore it
	err := recorder.AddJob(fakeJob(
		model.ReorgTypeTxn,
		model.ActionAddIndex,
		model.JobStateSynced,
		100,
		[]*model.IndexInfo{
			getIndex(1, []string{"x", "y"}),
		},
		nil,
	))
	require.NoError(t, err)
	err = recorder.Iterate(noItem)
	require.NoError(t, err)

	// no add-index job, should ignore it
	err = recorder.AddJob(fakeJob(
		model.ReorgTypeLitMerge,
		model.ActionDropIndex,
		model.JobStateSynced,
		100,
		[]*model.IndexInfo{
			getIndex(1, []string{"x", "y"}),
		},
		nil,
	))
	require.NoError(t, err)
	err = recorder.Iterate(noItem)
	require.NoError(t, err)

	// no synced job, should ignore it
	err = recorder.AddJob(fakeJob(
		model.ReorgTypeLitMerge,
		model.ActionAddIndex,
		model.JobStateRollbackDone,
		100,
		[]*model.IndexInfo{
			getIndex(1, []string{"x", "y"}),
		},
		nil,
	))
	require.NoError(t, err)
	err = recorder.Iterate(noItem)
	require.NoError(t, err)

	{
		recorder := ingestrec.New()
		// a normal ingest add index job
		err = recorder.AddJob(fakeJob(
			model.ReorgTypeLitMerge,
			model.ActionAddIndex,
			model.JobStateSynced,
			1000,
			[]*model.IndexInfo{
				getIndex(1, []string{"x", "y"}),
			},
			json.RawMessage(`[1, "a"]`),
		))
		require.NoError(t, err)
		f, cnt := hasOneItem(1, "`x`,`y`")
		err = recorder.Iterate(f)
		require.NoError(t, err)
		require.Equal(t, *cnt, 1)
	}

	{
		recorder := ingestrec.New()
		// a normal ingest add primary index job
		err = recorder.AddJob(fakeJob(
			model.ReorgTypeLitMerge,
			model.ActionAddPrimaryKey,
			model.JobStateSynced,
			1000,
			[]*model.IndexInfo{
				getIndex(1, []string{"x", "y"}),
			},
			json.RawMessage(`[1, "a"]`),
		))
		require.NoError(t, err)
		f, cnt := hasOneItem(1, "`x`,`y`")
		err = recorder.Iterate(f)
		require.NoError(t, err)
		require.Equal(t, *cnt, 1)
	}
}

func newRecorderWithInitJob(t *testing.T) *ingestrec.IngestRecorder {
	recorder := ingestrec.New()
	err := recorder.AddJob(fakeJob(
		model.ReorgTypeLitMerge,
		model.ActionAddIndex,
		model.JobStateSynced,
		1000,
		[]*model.IndexInfo{
			getIndex(1, []string{"x", "y"}),
		},
		json.RawMessage(`[1, "a"]`),
	))
	require.NoError(t, err)
	f, cnt := hasOneItem(1, "`x`,`y`")
	err = recorder.Iterate(f)
	require.NoError(t, err)
	require.Equal(t, *cnt, 1)
	return recorder
}

func fakeDropJob(jobTp model.ActionType, state model.JobState, rawArgs json.RawMessage) *model.Job {
	return &model.Job{
		SchemaName: SchemaName,
		TableName:  TableName,
		TableID:    TableID,
		Type:       jobTp,
		State:      state,
		RowCount:   0,
		RawArgs:    rawArgs,
		ReorgMeta: &model.DDLReorgMeta{
			ReorgTp: model.ReorgTypeTxn,
		},
	}
}

func TestDropIngestRecorder(t *testing.T) {
	{
		recorder := newRecorderWithInitJob(t)
		// a failed job, should ignore it
		err := recorder.DelJob(fakeDropJob(
			model.ActionDropSchema,
			model.JobStateRollbackDone,
			nil,
		))
		require.NoError(t, err)
		f, cnt := hasOneItem(1, "`x`,`y`")
		err = recorder.Iterate(f)
		require.NoError(t, err)
		require.Equal(t, *cnt, 1)
	}

	{
		recorder := newRecorderWithInitJob(t)
		// drop database
		err := recorder.DelJob(fakeDropJob(
			model.ActionDropSchema,
			model.JobStateSynced,
			json.RawMessage(`[[80], "a"]`),
		))
		require.NoError(t, err)
		err = recorder.Iterate(noItem)
		require.NoError(t, err)
	}

	{
		recorder := newRecorderWithInitJob(t)
		// drop table
		err := recorder.DelJob(fakeDropJob(
			model.ActionDropTable,
			model.JobStateSynced,
			nil,
		))
		require.NoError(t, err)
		err = recorder.Iterate(noItem)
		require.NoError(t, err)
	}

	{
		recorder := newRecorderWithInitJob(t)
		// truncate table
		err := recorder.DelJob(fakeDropJob(
			model.ActionTruncateTable,
			model.JobStateSynced,
			nil,
		))
		require.NoError(t, err)
		err = recorder.Iterate(noItem)
		require.NoError(t, err)
	}

	{
		recorder := newRecorderWithInitJob(t)
		// drop index
		err := recorder.DelJob(fakeDropJob(
			model.ActionDropTable,
			model.JobStateSynced,
			json.RawMessage(`["x", false, 1]`),
		))
		require.NoError(t, err)
		err = recorder.Iterate(noItem)
		require.NoError(t, err)
	}

	{
		recorder := newRecorderWithInitJob(t)
		// drop primary key
		err := recorder.DelJob(fakeDropJob(
			model.ActionDropPrimaryKey,
			model.JobStateSynced,
			json.RawMessage(`["x", false, 1]`),
		))
		require.NoError(t, err)
		err = recorder.Iterate(noItem)
		require.NoError(t, err)
	}

	{
		recorder := newRecorderWithInitJob(t)
		// drop indexes
		err := recorder.DelJob(fakeDropJob(
			model.ActionDropIndexes,
			model.JobStateSynced,
			json.RawMessage(`[[1], false, 1]`),
		))
		require.NoError(t, err)
		err = recorder.Iterate(noItem)
		require.NoError(t, err)
	}

	{
		recorder := newRecorderWithInitJob(t)
		// drop column
		err := recorder.DelJob(fakeDropJob(
			model.ActionDropColumn,
			model.JobStateSynced,
			json.RawMessage(`["x", false, [1]]`),
		))
		require.NoError(t, err)
		err = recorder.Iterate(noItem)
		require.NoError(t, err)
	}

	{
		recorder := newRecorderWithInitJob(t)
		// drop columns
		err := recorder.DelJob(fakeDropJob(
			model.ActionDropColumns,
			model.JobStateSynced,
			json.RawMessage(`[["x"], [false], [1]]`),
		))
		require.NoError(t, err)
		err = recorder.Iterate(noItem)
		require.NoError(t, err)
	}

	{
		recorder := newRecorderWithInitJob(t)
		// modify column
		err := recorder.DelJob(fakeDropJob(
			model.ActionModifyColumn,
			model.JobStateSynced,
			json.RawMessage(`[[1], false, 1]`),
		))
		require.NoError(t, err)
		err = recorder.Iterate(noItem)
		require.NoError(t, err)
	}
}
