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
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type mvRefreshObserveStepKind int

const (
	mvRefreshObserveStepMeta mvRefreshObserveStepKind = iota
	mvRefreshObserveStepFastMerge
	mvRefreshObserveStepCompleteDelete
	mvRefreshObserveStepCompleteInsert
	mvRefreshObserveStepCompleteDeltaApply
	mvRefreshObserveStepOutOfPlaceCreateShadow
	mvRefreshObserveStepOutOfPlaceLoadShadow
	mvRefreshObserveStepOutOfPlaceCutover
)

type mvRefreshObserveStep struct {
	ID   string
	Name string
	Kind mvRefreshObserveStepKind
}

type mvRefreshStepObserver interface {
	OnStepStart(step mvRefreshObserveStep, at time.Time)
	OnStepEnd(step mvRefreshObserveStep, at time.Time, err error)
	OnStepPlanRows(step mvRefreshObserveStep, rows [][]string)
}

var (
	mvRefreshObserveStepTxnBegin = mvRefreshObserveStep{
		Name: "TXN_BEGIN",
		Kind: mvRefreshObserveStepMeta,
	}
	mvRefreshObserveStepLockRefreshInfo = mvRefreshObserveStep{
		Name: "LOCK_REFRESH_INFO",
		Kind: mvRefreshObserveStepMeta,
	}
	mvRefreshObserveStepInsertHistRunning = mvRefreshObserveStep{
		Name: "INSERT_HIST_RUNNING",
		Kind: mvRefreshObserveStepMeta,
	}
	mvRefreshObserveStepDataChangeFastMerge = mvRefreshObserveStep{
		Name: "DATA_CHANGE_FAST_MERGE",
		Kind: mvRefreshObserveStepFastMerge,
	}
	mvRefreshObserveStepDataChangeCompleteDelete = mvRefreshObserveStep{
		Name: "DATA_CHANGE_COMPLETE_DELETE",
		Kind: mvRefreshObserveStepCompleteDelete,
	}
	mvRefreshObserveStepDataChangeCompleteInsert = mvRefreshObserveStep{
		Name: "DATA_CHANGE_COMPLETE_INSERT",
		Kind: mvRefreshObserveStepCompleteInsert,
	}
	mvRefreshObserveStepDataChangeCompleteDeltaApply = mvRefreshObserveStep{
		Name: "DATA_CHANGE_COMPLETE_DELTA_APPLY",
		Kind: mvRefreshObserveStepCompleteDeltaApply,
	}
	mvRefreshObserveStepDataChangeOutOfPlaceCreateShadow = mvRefreshObserveStep{
		Name: "OUT_OF_PLACE_CREATE_SHADOW",
		Kind: mvRefreshObserveStepOutOfPlaceCreateShadow,
	}
	mvRefreshObserveStepDataChangeOutOfPlaceLoadShadow = mvRefreshObserveStep{
		Name: "OUT_OF_PLACE_LOAD_SHADOW",
		Kind: mvRefreshObserveStepOutOfPlaceLoadShadow,
	}
	mvRefreshObserveStepDataChangeOutOfPlaceCutover = mvRefreshObserveStep{
		Name: "OUT_OF_PLACE_CUTOVER",
		Kind: mvRefreshObserveStepOutOfPlaceCutover,
	}
	mvRefreshObserveStepPersistRefreshInfo = mvRefreshObserveStep{
		Name: "PERSIST_REFRESH_INFO",
		Kind: mvRefreshObserveStepMeta,
	}
	mvRefreshObserveStepTxnCommit = mvRefreshObserveStep{
		Name: "TXN_COMMIT",
		Kind: mvRefreshObserveStepMeta,
	}
	mvRefreshObserveStepFinalizeHist = mvRefreshObserveStep{
		Name: "FINALIZE_HIST",
		Kind: mvRefreshObserveStepMeta,
	}
)

type mvRefreshStepSet struct {
	txnBegin                         mvRefreshObserveStep
	lockRefreshInfo                  mvRefreshObserveStep
	insertHistRunning                mvRefreshObserveStep
	dataChangeFastMerge              mvRefreshObserveStep
	dataChangeCompleteDelete         mvRefreshObserveStep
	dataChangeCompleteInsert         mvRefreshObserveStep
	dataChangeCompleteDeltaApply     mvRefreshObserveStep
	dataChangeOutOfPlaceCreateShadow mvRefreshObserveStep
	dataChangeOutOfPlaceLoadShadow   mvRefreshObserveStep
	dataChangeOutOfPlaceCutover      mvRefreshObserveStep
	persistRefreshInfo               mvRefreshObserveStep
	txnCommit                        mvRefreshObserveStep
	finalizeHist                     mvRefreshObserveStep
	steps                            []mvRefreshObserveStep
}

func mvRefreshStepID(idx int) string {
	return fmt.Sprintf("S%02d", idx)
}

func newMVRefreshStepSet(mode ast.RefreshMaterializedViewMode) (mvRefreshStepSet, error) {
	var set mvRefreshStepSet
	steps := make([]mvRefreshObserveStep, 0, 8)
	nextID := 1
	appendStep := func(step mvRefreshObserveStep) mvRefreshObserveStep {
		clone := step
		clone.ID = mvRefreshStepID(nextID)
		nextID++
		steps = append(steps, clone)
		return clone
	}

	if mode == ast.RefreshMaterializedViewModeCompleteOutOfPlace {
		set.insertHistRunning = appendStep(mvRefreshObserveStepInsertHistRunning)
		set.dataChangeOutOfPlaceCreateShadow = appendStep(mvRefreshObserveStepDataChangeOutOfPlaceCreateShadow)
		set.dataChangeOutOfPlaceLoadShadow = appendStep(mvRefreshObserveStepDataChangeOutOfPlaceLoadShadow)
		set.dataChangeOutOfPlaceCutover = appendStep(mvRefreshObserveStepDataChangeOutOfPlaceCutover)
		set.finalizeHist = appendStep(mvRefreshObserveStepFinalizeHist)
		set.steps = steps
		return set, nil
	}

	if mode != ast.RefreshMaterializedViewModeCompleteInPlace &&
		mode != ast.RefreshMaterializedViewModeFast &&
		mode != ast.RefreshMaterializedViewModeCompleteDeltaApply {
		return mvRefreshStepSet{}, errors.New("refresh materialized view observe: unknown mode")
	}

	set.txnBegin = appendStep(mvRefreshObserveStepTxnBegin)
	set.lockRefreshInfo = appendStep(mvRefreshObserveStepLockRefreshInfo)
	set.insertHistRunning = appendStep(mvRefreshObserveStepInsertHistRunning)

	if mode == ast.RefreshMaterializedViewModeFast {
		set.dataChangeFastMerge = appendStep(mvRefreshObserveStepDataChangeFastMerge)
	} else if mode == ast.RefreshMaterializedViewModeCompleteDeltaApply {
		set.dataChangeCompleteDeltaApply = appendStep(mvRefreshObserveStepDataChangeCompleteDeltaApply)
	} else {
		set.dataChangeCompleteDelete = appendStep(mvRefreshObserveStepDataChangeCompleteDelete)
		set.dataChangeCompleteInsert = appendStep(mvRefreshObserveStepDataChangeCompleteInsert)
	}

	set.persistRefreshInfo = appendStep(mvRefreshObserveStepPersistRefreshInfo)
	set.txnCommit = appendStep(mvRefreshObserveStepTxnCommit)
	set.finalizeHist = appendStep(mvRefreshObserveStepFinalizeHist)
	set.steps = steps
	return set, nil
}

func clonePlanRows(rows [][]string) [][]string {
	if len(rows) == 0 {
		return nil
	}
	cloned := make([][]string, len(rows))
	for i := range rows {
		cloned[i] = append([]string(nil), rows[i]...)
	}
	return cloned
}
