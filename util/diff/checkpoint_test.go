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

package diff

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

type testCheckpointSuite struct{}

func TestCheckpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := &testCheckpointSuite{}
	conn, err := createConn()
	require.NoError(t, err)
	defer conn.Close()

	defer dropCheckpoint(ctx, conn)
	s.testInitAndGetSummary(t, conn)
	s.testSaveAndLoadChunk(t, conn)
	s.testUpdateSummary(t, conn)
}

func (s *testCheckpointSuite) testInitAndGetSummary(t *testing.T, db *sql.DB) {
	err := createCheckpointTable(context.Background(), db)
	require.NoError(t, err)

	_, _, _, _, _, err = getTableSummary(context.Background(), db, "test", "checkpoint")
	t.Log(err)
	require.Regexp(t, "*not found*", err.Error())

	err = initTableSummary(context.Background(), db, "test", "checkpoint", "123")
	require.NoError(t, err)

	total, successNum, failedNum, ignoreNum, state, err := getTableSummary(context.Background(), db, "test", "checkpoint")
	require.NoError(t, err)
	require.Equal(t, int64(0), total)
	require.Equal(t, int64(0), successNum)
	require.Equal(t, int64(0), failedNum)
	require.Equal(t, int64(0), ignoreNum)
	require.Equal(t, notCheckedState, state)
}

func (s *testCheckpointSuite) testSaveAndLoadChunk(t *testing.T, db *sql.DB) {
	chunk := &ChunkRange{
		ID:           1,
		Bounds:       []*Bound{{Column: "a", Lower: "1"}},
		State:        successState,
		columnOffset: map[string]int{"a": 0},
	}

	err := saveChunk(context.Background(), db, chunk.ID, "target", "test", "checkpoint", "", chunk)
	require.NoError(t, err)

	newChunk, err := getChunk(context.Background(), db, "target", "test", "checkpoint", chunk.ID)
	require.NoError(t, err)
	newChunk.updateColumnOffset()
	require.Equal(t, chunk, newChunk)

	chunks, err := loadChunks(context.Background(), db, "target", "test", "checkpoint")
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	require.Equal(t, chunk, chunks[0])
}

func (s *testCheckpointSuite) testUpdateSummary(t *testing.T, db *sql.DB) {
	summaryInfo := newTableSummaryInfo(3)
	summaryInfo.addSuccessNum()
	summaryInfo.addFailedNum()
	summaryInfo.addIgnoreNum()

	err := updateTableSummary(context.Background(), db, "target", "test", "checkpoint", summaryInfo)
	require.NoError(t, err)

	total, successNum, failedNum, ignoreNum, state, err := getTableSummary(context.Background(), db, "test", "checkpoint")
	require.NoError(t, err)
	require.Equal(t, int64(3), total)
	require.Equal(t, int64(1), successNum)
	require.Equal(t, int64(1), failedNum)
	require.Equal(t, int64(1), ignoreNum)
	require.Equal(t, failedState, state)
}

func TestLoadFromCheckPoint(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"state", "config_hash"}).AddRow("success", "123")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	useCheckpoint, err := loadFromCheckPoint(context.Background(), db, "test", "test", "123")
	require.NoError(t, err)
	require.Equal(t, false, useCheckpoint)

	rows = sqlmock.NewRows([]string{"state", "config_hash"}).AddRow("success", "123")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	useCheckpoint, err = loadFromCheckPoint(context.Background(), db, "test", "test", "456")
	require.NoError(t, err)
	require.Equal(t, false, useCheckpoint)

	rows = sqlmock.NewRows([]string{"state", "config_hash"}).AddRow("failed", "123")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	useCheckpoint, err = loadFromCheckPoint(context.Background(), db, "test", "test", "123")
	require.NoError(t, err)
	require.Equal(t, true, useCheckpoint)
}

func TestInitChunks(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)

	chunks := []*ChunkRange{
		{
			ID:           1,
			Bounds:       []*Bound{{Column: "a", Lower: "1"}},
			State:        notCheckedState,
			columnOffset: map[string]int{"a": 0},
		}, {
			ID:           2,
			Bounds:       []*Bound{{Column: "a", Lower: "0", Upper: "1"}},
			State:        notCheckedState,
			columnOffset: map[string]int{"a": 0},
		},
	}

	// init chunks will insert chunk's information with update time, which use time.Now(), so can't know the value and can't fill the `WithArgs`
	// so just skip the `ExpectQuery` and check the error message
	//mock.ExpectQuery("INSERT INTO `sync_diff_inspector`.`chunk` VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?)").WithArgs(......)
	err = initChunks(context.Background(), db, "target", "diff_test", "test", chunks)
	require.Regexp(t, ".*INSERT INTO `sync_diff_inspector`.`chunk` VALUES\\(\\?, \\?, \\?, \\?, \\?, \\?, \\?, \\?, \\?\\), \\(\\?, \\?, \\?, \\?, \\?, \\?, \\?, \\?, \\?\\).*", err)
}
