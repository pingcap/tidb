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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestFix69990NonPreparedSchemaChangeDoesNotBuildParamMatchModes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=ON")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=ON")
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match=ON")
	tk.MustExec("create table nonprep_refresh(id int, v int)")
	tk.MustExec("insert into nonprep_refresh values(1,0)")

	// Warm and reuse a non-prepared UPDATE entry.
	tk.MustExec("update nonprep_refresh set v=1 where id=1")
	tk.MustExec("update nonprep_refresh set v=2 where id=1")
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)

	// Re-preprocess after DDL must preserve non-prepared cache behavior without
	// introducing the prepared-only matcher lifecycle.
	tk.MustExec("alter table nonprep_refresh add column note int")
	tk.MustExec("update nonprep_refresh set v=3 where id=1")
}

func TestFix69990NonPreparedUpdateConversionControlledByMatchingFeature(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, v int)")
	tk.MustExec("insert into t values(1, 0)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=ON")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=ON")

	assertCache := func(sql, expected string) {
		tk.MustExec(sql)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(expected))
	}

	// PointUpdate conversion follows the matching feature, so non-prepared UPDATE
	// cache entries must be isolated when the feature state changes.
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match=OFF")
	assertCache("update t set v=1 where id=1", "0")
	assertCache("update t set v=2 where id=1", "1")
	updateCacheSize := tk.Session().GetSessionPlanCache().Size()
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match=ON")
	assertCache("update t set v=3 where id=1", "0")
	assertCache("update t set v=4 where id=1", "1")
	updateCacheSize++
	if got := tk.Session().GetSessionPlanCache().Size(); got != updateCacheSize {
		t.Fatalf("matching feature did not isolate non-prepared UPDATE cache: want %d, got %d", updateCacheSize, got)
	}

	// Switching back reuses the legacy conversion entry.
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match=OFF")
	assertCache("update t set v=5 where id=1", "1")
	if got := tk.Session().GetSessionPlanCache().Size(); got != updateCacheSize {
		t.Fatalf("switching back created an unexpected UPDATE cache variant: %d -> %d", updateCacheSize, got)
	}
	tk.MustQuery("select v from t where id=1").Check(testkit.Rows("5"))

	// Non-prepared INSERT conversion is unchanged and must not be split by the
	// PointGet UPDATE feature identity.
	tk.MustExec("insert into t values(2, 2)")
	assertCache("insert into t values(3, 3)", "1")
	insertCacheSize := tk.Session().GetSessionPlanCache().Size()
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match=ON")
	assertCache("insert into t values(4, 4)", "1")
	tk.MustExec("set tidb_enable_plan_cache_dml_param_type_match=OFF")
	assertCache("insert into t values(5, 5)", "1")
	if got := tk.Session().GetSessionPlanCache().Size(); got != insertCacheSize {
		t.Fatalf("PointGet UPDATE feature split non-prepared INSERT cache: %d -> %d", insertCacheSize, got)
	}
}
