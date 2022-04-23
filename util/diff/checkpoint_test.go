// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package diff

import (
	"context"
	"database/sql"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

var _ = Suite(&testCheckpointSuite{})

type testCheckpointSuite struct{}

func (s *testCheckpointSuite) TestCheckpoint(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := createConn()
	c.Assert(err, IsNil)
	defer conn.Close()

	defer dropCheckpoint(ctx, conn)
	s.testInitAndGetSummary(c, conn)
	s.testSaveAndLoadChunk(c, conn)
	s.testUpdateSummary(c, conn)
}

func (s *testCheckpointSuite) testInitAndGetSummary(c *C, db *sql.DB) {
	err := createCheckpointTable(context.Background(), db)
	c.Assert(err, IsNil)

	_, _, _, _, _, err = getTableSummary(context.Background(), db, "test", "checkpoint")
	c.Log(err)
	c.Assert(err, ErrorMatches, "*not found*")

	err = initTableSummary(context.Background(), db, "test", "checkpoint", "123")
	c.Assert(err, IsNil)

	total, successNum, failedNum, ignoreNum, state, err := getTableSummary(context.Background(), db, "test", "checkpoint")
	c.Assert(err, IsNil)
	c.Assert(total, Equals, int64(0))
	c.Assert(successNum, Equals, int64(0))
	c.Assert(failedNum, Equals, int64(0))
	c.Assert(ignoreNum, Equals, int64(0))
	c.Assert(state, Equals, notCheckedState)
}

func (s *testCheckpointSuite) testSaveAndLoadChunk(c *C, db *sql.DB) {
	chunk := &ChunkRange{
		ID:           1,
		Bounds:       []*Bound{{Column: "a", Lower: "1"}},
		State:        successState,
		columnOffset: map[string]int{"a": 0},
	}

	err := saveChunk(context.Background(), db, chunk.ID, "target", "test", "checkpoint", "", chunk)
	c.Assert(err, IsNil)

	newChunk, err := getChunk(context.Background(), db, "target", "test", "checkpoint", chunk.ID)
	c.Assert(err, IsNil)
	newChunk.updateColumnOffset()
	c.Assert(newChunk, DeepEquals, chunk)

	chunks, err := loadChunks(context.Background(), db, "target", "test", "checkpoint")
	c.Assert(err, IsNil)
	c.Assert(chunks, HasLen, 1)
	c.Assert(chunks[0], DeepEquals, chunk)
}

func (s *testCheckpointSuite) testUpdateSummary(c *C, db *sql.DB) {
	summaryInfo := newTableSummaryInfo(3)
	summaryInfo.addSuccessNum()
	summaryInfo.addFailedNum()
	summaryInfo.addIgnoreNum()

	err := updateTableSummary(context.Background(), db, "target", "test", "checkpoint", summaryInfo)
	c.Assert(err, IsNil)

	total, successNum, failedNum, ignoreNum, state, err := getTableSummary(context.Background(), db, "test", "checkpoint")
	c.Assert(err, IsNil)
	c.Assert(total, Equals, int64(3))
	c.Assert(successNum, Equals, int64(1))
	c.Assert(failedNum, Equals, int64(1))
	c.Assert(ignoreNum, Equals, int64(1))
	c.Assert(state, Equals, failedState)
}

func (s *testUtilSuite) TestloadFromCheckPoint(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	rows := sqlmock.NewRows([]string{"state", "config_hash"}).AddRow("success", "123")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	useCheckpoint, err := loadFromCheckPoint(context.Background(), db, "test", "test", "123")
	c.Assert(useCheckpoint, Equals, false)

	rows = sqlmock.NewRows([]string{"state", "config_hash"}).AddRow("success", "123")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	useCheckpoint, err = loadFromCheckPoint(context.Background(), db, "test", "test", "456")
	c.Assert(useCheckpoint, Equals, false)

	rows = sqlmock.NewRows([]string{"state", "config_hash"}).AddRow("failed", "123")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	useCheckpoint, err = loadFromCheckPoint(context.Background(), db, "test", "test", "123")
	c.Assert(useCheckpoint, Equals, true)
}

func (s *testUtilSuite) TestInitChunks(c *C) {
	db, _, err := sqlmock.New()
	c.Assert(err, IsNil)

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
	c.Assert(err, ErrorMatches, ".*INSERT INTO `sync_diff_inspector`.`chunk` VALUES\\(\\?, \\?, \\?, \\?, \\?, \\?, \\?, \\?, \\?\\), \\(\\?, \\?, \\?, \\?, \\?, \\?, \\?, \\?, \\?\\).*")
}
