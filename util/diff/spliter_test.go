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
	"fmt"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/parser"
)

var _ = Suite(&testSpliterSuite{})

type testSpliterSuite struct{}

type chunkResult struct {
	chunkStr string
	args     []string
}

func (s *testSpliterSuite) TestSplitRangeByRandom(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	testCases := []struct {
		createTableSQL string
		splitCount     int
		originChunk    *ChunkRange
		randomValues   [][]interface{}
		expectResult   []chunkResult
	}{
		{
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))",
			3,
			NewChunkRange().copyAndUpdate("a", "0", "10", true, true).copyAndUpdate("b", "a", "z", true, true),
			[][]interface{}{
				{5, 7},
				{"g", "n"},
			},
			[]chunkResult{
				{
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"0", "0", "a", "5", "5", "g"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"5", "5", "g", "7", "7", "n"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"7", "7", "n", "10", "10", "z"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			3,
			NewChunkRange().copyAndUpdate("b", "a", "z", true, true),
			[][]interface{}{
				{"g", "n"},
			},
			[]chunkResult{
				{
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "g"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"g", "n"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"n", "z"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			2,
			NewChunkRange().copyAndUpdate("b", "a", "z", true, true),
			[][]interface{}{
				{"g"},
			},
			[]chunkResult{
				{
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "g"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"g", "z"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			3,
			NewChunkRange().copyAndUpdate("b", "a", "z", true, true),
			[][]interface{}{
				{},
			},
			[]chunkResult{
				{
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "z"},
				},
			},
		},
	}

	for i, testCase := range testCases {
		tableInfo, err := dbutil.GetTableInfoBySQL(testCase.createTableSQL, parser.New())
		c.Assert(err, IsNil)

		splitCols, err := getSplitFields(tableInfo, nil)
		c.Assert(err, IsNil)
		createFakeResultForRandomSplit(mock, 0, testCase.randomValues)

		chunks, err := splitRangeByRandom(db, testCase.originChunk, testCase.splitCount, "test", "test", splitCols, "", "")
		c.Assert(err, IsNil)
		for j, chunk := range chunks {
			chunkStr, args := chunk.toString("")
			c.Log(i, j, chunkStr, args)
			c.Assert(chunkStr, Equals, testCase.expectResult[j].chunkStr)
			c.Assert(args, DeepEquals, testCase.expectResult[j].args)
		}
	}
}

func (s *testSpliterSuite) TestRandomSpliter(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	testCases := []struct {
		createTableSQL string
		count          int
		randomValues   [][]interface{}
		expectResult   []chunkResult
	}{
		{
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))",
			10,
			[][]interface{}{
				{1, 2, 3, 4, 5},
				{"a", "b", "c", "d", "e"},
			},
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"1", "1", "a"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"1", "1", "a", "2", "2", "b"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"2", "2", "b", "3", "3", "c"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"3", "3", "c", "4", "4", "d"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"4", "4", "d", "5", "5", "e"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"5", "5", "e"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			10,
			[][]interface{}{
				{"a", "b", "c", "d", "e"},
			},
			[]chunkResult{
				{
					"(`b` <= ?)",
					[]string{"a"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "b"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"b", "c"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"c", "d"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"d", "e"},
				}, {
					"(`b` > ?)",
					[]string{"e"},
				},
			},
		},
	}

	for i, testCase := range testCases {
		tableInfo, err := dbutil.GetTableInfoBySQL(testCase.createTableSQL, parser.New())
		c.Assert(err, IsNil)

		tableInstance := &TableInstance{
			Conn:   db,
			Schema: "test",
			Table:  "test",
			info:   tableInfo,
		}

		splitCols, err := getSplitFields(tableInfo, nil)
		c.Assert(err, IsNil)

		createFakeResultForRandomSplit(mock, testCase.count, testCase.randomValues)

		rSpliter := new(randomSpliter)
		chunks, err := rSpliter.split(tableInstance, splitCols, 2, "TRUE", "")
		c.Assert(err, IsNil)

		for j, chunk := range chunks {
			chunkStr, args := chunk.toString("")
			c.Log(i, j, chunkStr, args)
			c.Assert(chunkStr, Equals, testCase.expectResult[j].chunkStr)
			c.Assert(args, DeepEquals, testCase.expectResult[j].args)
		}
	}
}

func createFakeResultForRandomSplit(mock sqlmock.Sqlmock, count int, randomValues [][]interface{}) {
	if count > 0 {
		// generate fake result for get the row count of this table
		countRows := sqlmock.NewRows([]string{"cnt"}).AddRow(count)
		mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
	}

	// generate fake result for get random value for column a
	for _, randomVs := range randomValues {
		randomRows := sqlmock.NewRows([]string{"a"})
		for _, value := range randomVs {
			randomRows.AddRow(value)
		}
		mock.ExpectQuery("ORDER BY rand_value").WillReturnRows(randomRows)
	}

	return
}

func (s *testSpliterSuite) TestBucketSpliter(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL, parser.New())
	c.Assert(err, IsNil)

	testCases := []struct {
		chunkSize     int
		aRandomValues []interface{}
		bRandomValues []interface{}
		expectResult  []chunkResult
	}{
		{
			// chunk size less than the count of bucket 64, and the bucket's count 64 >= 32, so will split by random in every bucket
			32,
			[]interface{}{32, 32 * 3, 32 * 5, 32 * 7, 32 * 9},
			[]interface{}{6, 6 * 3, 6 * 5, 6 * 7, 6 * 9},
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"32", "32", "6"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"32", "32", "6", "63", "63", "11"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"63", "63", "11", "96", "96", "18"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"96", "96", "18", "127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "160", "160", "30"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"160", "160", "30", "191", "191", "35"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"191", "191", "35", "224", "224", "42"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"224", "224", "42", "255", "255", "47"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"255", "255", "47", "288", "288", "54"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"288", "288", "54", "319", "319", "59"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"319", "319", "59"},
				},
			},
		}, {
			// chunk size less than the count of bucket 64, but 64 is  less than 2*40, so will not split every bucket
			40,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"63", "63", "11"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"63", "63", "11", "127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "191", "191", "35"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"191", "191", "35", "255", "255", "47"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"255", "255", "47", "319", "319", "59"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"319", "319", "59"},
				},
			},
		}, {
			// chunk size is equal to the count of bucket 64, so every becket will generate a chunk
			64,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"63", "63", "11"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"63", "63", "11", "127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "191", "191", "35"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"191", "191", "35", "255", "255", "47"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"255", "255", "47", "319", "319", "59"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"319", "319", "59"},
				},
			},
		}, {
			// chunk size is greater than the count of bucket 64, will combine two bucket into chunk
			127,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "255", "255", "47"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"255", "255", "47"},
				},
			},
		}, {
			// chunk size is equal to the double count of bucket 64, will combine two bucket into one chunk
			128,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "255", "255", "47"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"255", "255", "47"},
				},
			},
		}, {
			// chunk size is greate than the double count of bucket 64, will combine three bucket into one chunk
			129,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"191", "191", "35"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"191", "191", "35"},
				},
			},
		}, {
			// chunk size is greater than the total count, only generate one chunk
			400,
			nil,
			nil,
			[]chunkResult{
				{
					"TRUE",
					nil,
				},
			},
		},
	}

	tableInstance := &TableInstance{
		Conn:   db,
		Schema: "test",
		Table:  "test",
		info:   tableInfo,
	}

	for i, testCase := range testCases {
		createFakeResultForBucketSplit(mock, testCase.aRandomValues, testCase.bRandomValues)
		bSpliter := new(bucketSpliter)
		chunks, err := bSpliter.split(tableInstance, tableInfo.Columns, testCase.chunkSize, "TRUE", "")
		c.Assert(err, IsNil)
		for j, chunk := range chunks {
			chunkStr, args := chunk.toString("")
			c.Log(i, j, chunkStr, args)
			c.Assert(chunkStr, Equals, testCase.expectResult[j].chunkStr)
			c.Assert(args, DeepEquals, testCase.expectResult[j].args)
		}
	}
}

func createFakeResultForBucketSplit(mock sqlmock.Sqlmock, aRandomValues, bRandomValues []interface{}) {
	/*
		+---------+------------+-------------+----------+-----------+-------+---------+-------------+-------------+
		| Db_name | Table_name | Column_name | Is_index | Bucket_id | Count | Repeats | Lower_Bound | Upper_Bound |
		+---------+------------+-------------+----------+-----------+-------+---------+-------------+-------------+
		| test    | test       | PRIMARY     |        1 |         0 |    64 |       1 | (0, 0)      | (63, 11)    |
		| test    | test       | PRIMARY     |        1 |         1 |   128 |       1 | (64, 12)    | (127, 23)   |
		| test    | test       | PRIMARY     |        1 |         2 |   192 |       1 | (128, 24)   | (191, 35)   |
		| test    | test       | PRIMARY     |        1 |         3 |   256 |       1 | (192, 36)   | (255, 47)   |
		| test    | test       | PRIMARY     |        1 |         4 |   320 |       1 | (256, 48)   | (319, 59)   |
		+---------+------------+-------------+----------+-----------+-------+---------+-------------+-------------+
	*/

	statsRows := sqlmock.NewRows([]string{"Db_name", "Table_name", "Column_name", "Is_index", "Bucket_id", "Count", "Repeats", "Lower_Bound", "Upper_Bound"})
	for i := 0; i < 5; i++ {
		statsRows.AddRow("test", "test", "PRIMARY", 1, (i+1)*64, (i+1)*64, 1, fmt.Sprintf("(%d, %d)", i*64, i*12), fmt.Sprintf("(%d, %d)", (i+1)*64-1, (i+1)*12-1))
	}
	mock.ExpectQuery("SHOW STATS_BUCKETS").WillReturnRows(statsRows)

	for i := 0; i < len(aRandomValues); i++ {
		aRandomRows := sqlmock.NewRows([]string{"a"})
		aRandomRows.AddRow(aRandomValues[i])
		mock.ExpectQuery("ORDER BY rand_value").WillReturnRows(aRandomRows)

		bRandomRows := sqlmock.NewRows([]string{"b"})
		bRandomRows.AddRow(bRandomValues[i])
		mock.ExpectQuery("ORDER BY rand_value").WillReturnRows(bRandomRows)
	}

	return
}
