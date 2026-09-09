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

package planner

import (
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

func TestEngineRestrictedRoundGatesAndCleanup(t *testing.T) {
	sessVars := variable.NewSessionVars(nil)
	sessVars.EnableAlternativeLogicalPlans = true
	sessVars.StmtCtx.AlternativeLogicalPlanMixedStorageEngines = true

	if !shouldTryTiKVOnlyRound(sessVars) {
		t.Fatal("mixed storage plans should enable the TiKV-only round")
	}
	if !shouldTryTiFlashOnlyRound(sessVars) {
		t.Fatal("mixed storage plans should enable the TiFlash-only round")
	}

	sessVars.StmtCtx.AlternativeLogicalPlanHasStoreTypeHint = true
	if shouldTryTiKVOnlyRound(sessVars) || shouldTryTiFlashOnlyRound(sessVars) {
		t.Fatal("an explicit storage hint must disable engine-restricted rounds")
	}
	sessVars.StmtCtx.AlternativeLogicalPlanHasStoreTypeHint = false
	sessVars.StmtCtx.AlternativeLogicalPlanMissingTiFlashPath = true
	if shouldTryTiFlashOnlyRound(sessVars) {
		t.Fatal("a missing TiFlash path must disable the TiFlash-only round")
	}

	sessVars.StmtCtx.AlternativeLogicalPlanMissingTiFlashPath = false
	if err := sessVars.SetSystemVar("tidb_enforce_mpp", "1"); err != nil {
		t.Fatal(err)
	}
	if shouldTryTiKVOnlyRound(sessVars) || shouldTryTiFlashOnlyRound(sessVars) {
		t.Fatal("enforced MPP must disable engine-restricted rounds")
	}

	sessVars.IsolationReadEngines = map[kv.StoreType]struct{}{kv.TiFlash: {}}
	var tiKVOnly alternativeRound
	for _, round := range alternativeRounds {
		if round.name == "tikv-only" {
			tiKVOnly = round
			break
		}
	}
	cleanup := tiKVOnly.setup(sessVars)
	if _, ok := sessVars.IsolationReadEngines[kv.TiFlash]; ok {
		t.Fatal("the TiKV-only setup must remove TiFlash")
	}
	cleanup()
	if _, ok := sessVars.IsolationReadEngines[kv.TiFlash]; !ok {
		t.Fatal("round cleanup must restore the caller's isolation engines")
	}

}
