// Copyright 2018 PingCAP, Inc.
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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/importer"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

var _ = Suite(&testChunkSuite{})

type testChunkSuite struct{}

type chunkTestCase struct {
	chunk        *ChunkRange
	chunkCnt     int64
	expectChunks []*ChunkRange
}

func (*testChunkSuite) TestSplitRange(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := createConn()
	c.Assert(err, IsNil)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `test`")
	c.Assert(err, IsNil)

	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`test_chunk`")
	c.Assert(err, IsNil)

	createTableSQL := `CREATE TABLE test.test_chunk (
		a date NOT NULL,
		b datetime DEFAULT NULL,
		c time DEFAULT NULL,
		d varchar(10) COLLATE latin1_bin DEFAULT NULL,
		e int(10) DEFAULT NULL,
		h year(4) DEFAULT NULL,
		PRIMARY KEY (a))`

	dataCount := 10000
	cfg := &importer.Config{
		TableSQL:    createTableSQL,
		WorkerCount: 5,
		JobCount:    dataCount,
		Batch:       100,
		DBCfg:       dbutil.GetDBConfigFromEnv("test"),
	}

	// generate data for test.test_chunk
	importer.DoProcess(cfg)
	defer conn.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`test_chunk`")

	// only work on tidb, so don't assert err here
	_, _ = conn.ExecContext(ctx, "ANALYZE TABLE `test`.`test_chunk`")

	tableInfo, err := dbutil.GetTableInfo(ctx, conn, "test", "test_chunk")
	c.Assert(err, IsNil)

	tableInstance := &TableInstance{
		Conn:   conn,
		Schema: "test",
		Table:  "test_chunk",
		info:   tableInfo,
	}

	// split chunks
	fields, err := getSplitFields(tableInstance.info, nil)
	c.Assert(err, IsNil)
	chunks, err := getChunksForTable(tableInstance, fields, 100, "TRUE", "", false)
	c.Assert(err, IsNil)

	// get data count from every chunk, and the sum of them should equal to the table's count.
	chunkDataCount := 0
	for _, chunk := range chunks {
		conditions, args := chunk.toString("")
		count, err := dbutil.GetRowCount(ctx, tableInstance.Conn, tableInstance.Schema, tableInstance.Table, conditions, stringSliceToInterfaceSlice(args))
		c.Assert(err, IsNil)
		chunkDataCount += int(count)
	}
	c.Assert(chunkDataCount, Equals, dataCount)
}

func (*testChunkSuite) TestChunkUpdate(c *C) {
	chunk := &ChunkRange{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: true,
			},
		},
	}

	testCases := []struct {
		boundArgs  []string
		expectStr  string
		expectArgs []string
	}{
		{
			[]string{"a", "5", "6"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
			[]string{"5", "5", "3", "6", "6", "4"},
		}, {
			[]string{"b", "5", "6"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
			[]string{"1", "1", "5", "2", "2", "6"},
		}, {
			[]string{"c", "7", "8"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))",
			[]string{"1", "1", "3", "1", "3", "7", "2", "2", "4", "2", "4", "8"},
		},
	}

	for _, cs := range testCases {
		newChunk := chunk.copyAndUpdate(cs.boundArgs[0], cs.boundArgs[1], cs.boundArgs[2], true, true)
		conditions, args := newChunk.toString("")
		c.Assert(conditions, Equals, cs.expectStr)
		c.Assert(args, DeepEquals, cs.expectArgs)
	}

	// the origin chunk is not changed
	conditions, args := chunk.toString("")
	c.Assert(conditions, Equals, "((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))")
	expectArgs := []string{"1", "1", "3", "2", "2", "4"}
	c.Assert(args, DeepEquals, expectArgs)
}

func (*testChunkSuite) TestChunkToString(c *C) {
	chunk := &ChunkRange{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: true,
				HasUpper: true,
			},
		},
	}

	conditions, args := chunk.toString("")
	c.Assert(conditions, Equals, "((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))")
	expectArgs := []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	conditions, args = chunk.toString("latin1")
	c.Assert(conditions, Equals, "((`a` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' > ?)) AND ((`a` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' <= ?))")
	expectArgs = []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}
}

func (*testChunkSuite) TestRangeLimit(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := createConn()
	c.Assert(err, IsNil)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `test`")
	c.Assert(err, IsNil)

	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`test_range`")
	c.Assert(err, IsNil)

	createTableSQL := `CREATE TABLE test.test_range (
		a int NOT NULL,
		b datetime DEFAULT NULL,
		c time DEFAULT NULL,
		d varchar(10) COLLATE latin1_bin DEFAULT NULL,
		e int(10) DEFAULT NULL,
		h year(4) DEFAULT NULL,
		PRIMARY KEY (a))`

	// will generate 0-9 at column `a`
	dataCount := 10
	cfg := &importer.Config{
		TableSQL:    createTableSQL,
		WorkerCount: 5,
		JobCount:    dataCount,
		Batch:       100,
		DBCfg:       dbutil.GetDBConfigFromEnv("test"),
	}

	// generate data for test.test_chunk
	importer.DoProcess(cfg)
	defer conn.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`test_range`")

	// only work on tidb, so don't assert err here
	_, _ = conn.ExecContext(ctx, "ANALYZE TABLE `test`.`test_range`")

	tableInfo, err := dbutil.GetTableInfo(ctx, conn, "test", "test_range")
	c.Assert(err, IsNil)

	tableInstance := &TableInstance{
		Conn:   conn,
		Schema: "test",
		Table:  "test_range",
		info:   tableInfo,
	}

	c.Assert(createCheckpointTable(ctx, conn), IsNil)
	chunks, err := SplitChunks(ctx, tableInstance, "a,d", "a > 7", 1, "", false, conn)
	c.Assert(err, IsNil)
	defer conn.ExecContext(ctx, "DROP DATABASE sync_diff_inspector")
	// a > 7 and chunkSize = 1 should return 2 chunk
	c.Assert(chunks, HasLen, 2)
	count := 0

	for _, chunk := range chunks {
		rows, _, err := getChunkRows(ctx, conn, "test", "test_range", tableInfo, chunk.Where, utils.StringsToInterfaces(chunk.Args), "")
		c.Assert(err, IsNil)
		for rows.Next() {
			count++
		}
	}
	c.Assert(count, Equals, 2)
}
