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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
)

func TestRuleConstantPropagation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// create table
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, name varchar(10));")
	tk.MustExec("create table s (id int, name varchar(10));")
	testData := core.GetRuleConstantPropagationData()
	var (
		input  []string
		output []struct {
			SQL    string
			Output []string
		}
	)
	testData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		sql = "explain " + sql
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Output = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Output...))
	}
}

func TestDifferentJoinTypeConstantPropagation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// create table
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, name varchar(10));")
	tk.MustExec("create table s (id int, name varchar(10));")
	testData := core.GetRuleConstantPropagationData()
	var (
		input  []string
		output []struct {
			SQL    string
			Output []string
		}
	)
	testData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		sql = "explain " + sql
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Output = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Output...))
	}
}

func TestSelectionThroughPlanNode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// create table
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, name varchar(10));")
	tk.MustExec("create table s (id int, name varchar(10));")
	testData := core.GetRuleConstantPropagationData()
	var (
		input  []string
		output []struct {
			SQL    string
			Output []string
		}
	)
	testData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		sql = "explain " + sql
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Output = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Output...))
	}
}

func TestUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// create table
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, name varchar(10));")
	tk.MustExec("create table s (id int, name varchar(10));")
	testData := core.GetRuleConstantPropagationData()
	var (
		input  []string
		output []struct {
			SQL    string
			Output []string
		}
	)
	testData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		sql = "explain " + sql
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Output = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Output...))
	}
}

func TestMultiSubtreeMatch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// create table
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, name varchar(10));")
	tk.MustExec("create table s (id int, name varchar(10));")
	testData := core.GetRuleConstantPropagationData()
	var (
		input  []string
		output []struct {
			SQL    string
			Output []string
		}
	)
	testData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		sql = "explain " + sql
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Output = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Output...))
	}
}
