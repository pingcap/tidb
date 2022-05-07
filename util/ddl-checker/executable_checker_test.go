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

package checker

import (
	"container/list"
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type parseTestData struct {
	sql                 string
	parseSucceeded      bool
	tableNeededExist    []string
	tableNeededNonExist []string
	executeSucceeded    bool
}

func setUpTestData() *list.List {
	testData := list.New()

	testData.PushBack(parseTestData{sql: "drop table if exists t1,t2,t3,t4,t5;", parseSucceeded: true, tableNeededExist: []string{"t1", "t2", "t3", "t4", "t5"}, tableNeededNonExist: []string{}, executeSucceeded: true})
	testData.PushBack(parseTestData{sql: "drop database if exists mysqltest;", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{}, executeSucceeded: true})
	testData.PushBack(parseTestData{sql: "create table t1 (b char(0));", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{"t1"}, executeSucceeded: true})
	testData.PushBack(parseTestData{sql: "insert into t1 values (''),(null);", parseSucceeded: true, tableNeededExist: nil, tableNeededNonExist: nil, executeSucceeded: true})
	testData.PushBack(parseTestData{sql: "select * from t1;", parseSucceeded: true, tableNeededExist: nil, tableNeededNonExist: nil, executeSucceeded: true})
	testData.PushBack(parseTestData{sql: "drop table if exists t1;", parseSucceeded: true, tableNeededExist: []string{"t1"}, tableNeededNonExist: []string{}, executeSucceeded: true})
	testData.PushBack(parseTestData{sql: "create table t1 (b char(0) not null);", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{"t1"}, executeSucceeded: true})
	testData.PushBack(parseTestData{sql: "create table if not exists t1 (b char(0) not null);", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{"t1"}, executeSucceeded: true})
	testData.PushBack(parseTestData{sql: "insert into t1 values (''),(null);", parseSucceeded: true, tableNeededExist: nil, tableNeededNonExist: nil, executeSucceeded: false})
	testData.PushBack(parseTestData{sql: "select * from t1;", parseSucceeded: true, tableNeededExist: nil, tableNeededNonExist: nil, executeSucceeded: true})
	testData.PushBack(parseTestData{sql: "drop table t1;", parseSucceeded: true, tableNeededExist: []string{"t1"}, tableNeededNonExist: []string{}, executeSucceeded: true})
	testData.PushBack(parseTestData{sql: "create table t(a int comment '[[range=1,10]]');", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{"t"}, executeSucceeded: true})
	return testData
}

func TestParse(t *testing.T) {
	ec, err := NewExecutableChecker()
	require.NoError(t, err)
	defer ec.Close()
	testData := setUpTestData()
	for e := testData.Front(); e != nil; e = e.Next() {
		data := e.Value.(parseTestData)
		stmt, err := ec.Parse(data.sql)
		if err != nil {
			require.False(t, data.parseSucceeded)
			continue
		}
		tableNeededExist, _ := GetTablesNeededExist(stmt)
		tableNeededNonExist, _ := GetTablesNeededNonExist(stmt)
		require.True(t, data.parseSucceeded)
		require.Equal(t, data.tableNeededExist, tableNeededExist)
		require.Equal(t, data.tableNeededNonExist, tableNeededNonExist)
	}
}

func TestExecute(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	testData := setUpTestData()
	tk.MustExec("use test;")
	for e := testData.Front(); e != nil; e = e.Next() {
		data := e.Value.(parseTestData)
		res, err := tk.Exec(data.sql)
		require.Equal(t, data.executeSucceeded, err == nil)
		if res != nil {
			res.Close()
		}
	}
}
