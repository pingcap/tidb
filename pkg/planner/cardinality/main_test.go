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

package cardinality

import (
	"flag"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	planutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

var testDataMap = make(testdata.BookKeeper, 3)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()

	if !flag.Parsed() {
		flag.Parse()
	}

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})

	testDataMap.LoadTestSuiteData("testdata", "cardinality_suite")

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}

	callback := func(i int) int {
		testDataMap.GenerateOutputIfNeeded()
		return i
	}
	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

func GetCardinalitySuiteData() testdata.TestData {
	return testDataMap["cardinality_suite"]
}

// MockStatsNode is only used for test.
func MockStatsNode(id int64, m int64, num int) *StatsNode {
	return &StatsNode{ID: id, mask: m, numCols: num}
}

func TestConstantOnlyPredicateDoesNotAffectIndexCoverage(t *testing.T) {
	sctx := mock.NewContext()
	intType := types.NewFieldType(mysql.TypeLonglong)
	boolType := types.NewFieldType(mysql.TypeTiny)
	one := &expression.Constant{Value: types.NewIntDatum(1), RetType: intType}
	newEQ := func(lhs, rhs expression.Expression) expression.Expression {
		expr, err := expression.NewFunctionBase(sctx, ast.EQ, boolType, lhs, rhs)
		require.NoError(t, err)
		return expr
	}

	constantEQ := newEQ(one, one)
	zero := &expression.Constant{Value: types.NewIntDatum(0), RetType: intType}
	uncoveredConstantEQ := newEQ(zero, zero)
	siteEQ := newEQ(&expression.Column{UniqueID: 1, RetType: intType}, one)
	deleteEQ := newEQ(&expression.Column{UniqueID: 2, RetType: intType}, one)
	userEQ := newEQ(&expression.Column{UniqueID: 3, RetType: intType}, one)
	exprs := []expression.Expression{constantEQ, uncoveredConstantEQ, siteEQ, deleteEQ, userEQ}

	shortPath := &planutil.AccessPath{AccessConds: []expression.Expression{constantEQ, siteEQ, deleteEQ}}
	shortMask, shortConstExprMask, _, _, _, err := getMaskAndRanges(sctx.GetPlanCtx(), exprs, ranger.IndexRangeType, nil, shortPath)
	require.NoError(t, err)
	longPath := &planutil.AccessPath{AccessConds: []expression.Expression{userEQ, siteEQ, deleteEQ}}
	longMask, longConstExprMask, _, _, _, err := getMaskAndRanges(sctx.GetPlanCtx(), exprs, ranger.IndexRangeType, nil, longPath)
	require.NoError(t, err)

	require.Equal(t, int64(0b01100), shortMask)
	require.Equal(t, int64(0b00001), shortConstExprMask)
	require.Equal(t, int64(0b11100), longMask)
	require.Zero(t, longConstExprMask)
	nodes := []*StatsNode{
		{ID: 1, Tp: IndexType, mask: shortMask, numCols: 4, Selectivity: 1},
		{ID: 2, Tp: IndexType, mask: longMask, numCols: 6, Selectivity: 0.001},
	}
	usedSets := GetUsableSetsByGreedy(nodes)
	require.Len(t, usedSets, 1)
	require.Equal(t, int64(2), usedSets[0].ID)

	residualMask := (int64(1) << uint(len(exprs))) - 1
	residualMask &^= shortConstExprMask | longConstExprMask
	for _, set := range usedSets {
		residualMask &^= set.mask
	}
	// The covered constant predicate is excluded from residual estimation, while the constant predicate
	// that is not present in any access conditions remains for the existing residual logic to handle.
	require.Equal(t, int64(0b00010), residualMask)
}
