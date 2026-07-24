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

package executor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/stretchr/testify/require"
)

func TestResourceGroupTaggerCache(t *testing.T) {
	sqlNormalized, sqlDigest := parser.NormalizeDigest("select * from t where id = 1")
	planNormalized, planDigest := parser.NormalizeDigest("Point_Get_1")
	sc := &stmtctx.StatementContext{}
	sc.InitSQLDigest(sqlNormalized, sqlDigest)
	sc.SetPlanDigest(planNormalized, planDigest)

	cache := resourceGroupTaggerCache{}
	tagger, changed := cache.get(sc)
	require.True(t, changed)
	require.NotNil(t, tagger)
	require.Equal(t, sc.GetResourceGroupTagger().EncodeTagWithKey(nil), tagger.EncodeTagWithKey(nil))

	sameSQLNormalized, sameSQLDigest := parser.NormalizeDigest("select * from t where id = 2")
	samePlanNormalized, samePlanDigest := parser.NormalizeDigest("Point_Get_1")
	sameDigests := &stmtctx.StatementContext{}
	sameDigests.InitSQLDigest(sameSQLNormalized, sameSQLDigest)
	sameDigests.SetPlanDigest(samePlanNormalized, samePlanDigest)
	tagger, changed = cache.get(sameDigests)
	require.False(t, changed)
	require.Nil(t, tagger)

	changedSQLNormalized, changedSQLDigest := parser.NormalizeDigest("select b from t where id = 2")
	sameDigests = &stmtctx.StatementContext{}
	sameDigests.InitSQLDigest(changedSQLNormalized, changedSQLDigest)
	sameDigests.SetPlanDigest(samePlanNormalized, samePlanDigest)
	tagger, changed = cache.get(sameDigests)
	require.True(t, changed)
	require.NotNil(t, tagger)
	require.Equal(t, sameDigests.GetResourceGroupTagger().EncodeTagWithKey(nil), tagger.EncodeTagWithKey(nil))

	changedPlanNormalized, changedPlanDigest := parser.NormalizeDigest("TableReader_2")
	sameDigests.SetPlanDigest(changedPlanNormalized, changedPlanDigest)
	tagger, changed = cache.get(sameDigests)
	require.True(t, changed)
	require.NotNil(t, tagger)
	require.Equal(t, sameDigests.GetResourceGroupTagger().EncodeTagWithKey(nil), tagger.EncodeTagWithKey(nil))

	emptyCache := resourceGroupTaggerCache{}
	empty := &stmtctx.StatementContext{}
	tagger, changed = emptyCache.get(empty)
	require.True(t, changed)
	require.NotNil(t, tagger)
	require.Equal(t, empty.GetResourceGroupTagger().EncodeTagWithKey(nil), tagger.EncodeTagWithKey(nil))
	tagger, changed = emptyCache.get(empty)
	require.False(t, changed)
	require.Nil(t, tagger)
}
