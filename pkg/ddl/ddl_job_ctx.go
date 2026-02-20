// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
)
func (dc *ddlCtx) attachTopProfilingInfo(jobID int64, jobQuery string) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		ctx = NewReorgContext()
		dc.jobCtx.jobCtxMap[jobID] = ctx
	}
	ctx.attachTopProfilingInfo(jobQuery)
}

func (dc *ddlCtx) setDDLSourceForDiagnosis(jobID int64, jobType model.ActionType) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		ctx = NewReorgContext()
		dc.jobCtx.jobCtxMap[jobID] = ctx
	}
	ctx.setDDLLabelForDiagnosis(jobType)
}

func (dc *ddlCtx) getResourceGroupTaggerForTopSQL(jobID int64) *kv.ResourceGroupTagBuilder {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		return nil
	}
	return ctx.getResourceGroupTaggerForTopSQL()
}

func (dc *ddlCtx) setAnalyzeDoneCh(jobID int64, ch chan error) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		ctx = NewReorgContext()
		dc.jobCtx.jobCtxMap[jobID] = ctx
	}
	ctx.analyzeDone = ch
}

func (dc *ddlCtx) getAnalyzeDoneCh(jobID int64) chan error {
	dc.jobCtx.RLock()
	defer dc.jobCtx.RUnlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		return nil
	}
	return ctx.analyzeDone
}

// setAnalyzeStartTime sets the analyze start time for a given job ID.
func (dc *ddlCtx) setAnalyzeStartTime(jobID int64, t time.Time) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		ctx = NewReorgContext()
		dc.jobCtx.jobCtxMap[jobID] = ctx
	}
	ctx.analyzeStartTime = t
}

// getAnalyzeStartTime returns the analyze start time for a given job ID. If not set, returns zero time and false.
func (dc *ddlCtx) getAnalyzeStartTime(jobID int64) (time.Time, bool) {
	dc.jobCtx.RLock()
	defer dc.jobCtx.RUnlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		return time.Time{}, false
	}
	t := ctx.analyzeStartTime
	return t, !t.IsZero()
}

// clearAnalyzeStartTime clears the analyze start time for a given job ID.
func (dc *ddlCtx) clearAnalyzeStartTime(jobID int64) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	if ctx, exists := dc.jobCtx.jobCtxMap[jobID]; exists {
		ctx.analyzeStartTime = time.Time{}
	}
}

// setAnalyzeCumulativeTimeout sets the computed cumulative timeout for analyze for a given job ID.
func (dc *ddlCtx) setAnalyzeCumulativeTimeout(jobID int64, dur time.Duration) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		ctx = NewReorgContext()
		dc.jobCtx.jobCtxMap[jobID] = ctx
	}
	ctx.analyzeCumulativeTimeout = dur
}

// getAnalyzeCumulativeTimeout returns the stored analyze cumulative timeout and true if set.
func (dc *ddlCtx) getAnalyzeCumulativeTimeout(jobID int64) (time.Duration, bool) {
	dc.jobCtx.RLock()
	defer dc.jobCtx.RUnlock()
	ctx, exists := dc.jobCtx.jobCtxMap[jobID]
	if !exists {
		return 0, false
	}
	if ctx.analyzeCumulativeTimeout == 0 {
		return 0, false
	}
	return ctx.analyzeCumulativeTimeout, true
}

// clearAnalyzeCumulativeTimeout clears the stored analyze cumulative timeout for a given job ID.
func (dc *ddlCtx) clearAnalyzeCumulativeTimeout(jobID int64) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	if ctx, exists := dc.jobCtx.jobCtxMap[jobID]; exists {
		ctx.analyzeCumulativeTimeout = 0
	}
}

func (dc *ddlCtx) removeJobCtx(job *model.Job) {
	dc.jobCtx.Lock()
	defer dc.jobCtx.Unlock()
	delete(dc.jobCtx.jobCtxMap, job.ID)
}

func (dc *ddlCtx) jobContext(jobID int64, reorgMeta *model.DDLReorgMeta) *ReorgContext {
	dc.jobCtx.RLock()
	defer dc.jobCtx.RUnlock()
	var ctx *ReorgContext
	if jobContext, exists := dc.jobCtx.jobCtxMap[jobID]; exists {
		ctx = jobContext
	} else {
		ctx = NewReorgContext()
	}
	if reorgMeta != nil && len(ctx.resourceGroupName) == 0 {
		ctx.resourceGroupName = reorgMeta.ResourceGroupName
	}
	return ctx
}

type reorgContexts struct {
	sync.RWMutex
	// reorgCtxMap maps job ID to reorg context.
	reorgCtxMap map[int64]*reorgCtx
	beOwnerTS   int64
}

func (r *reorgContexts) getOwnerTS() int64 {
	r.RLock()
	defer r.RUnlock()
	return r.beOwnerTS
}

func (r *reorgContexts) setOwnerTS(ts int64) {
	r.Lock()
	r.beOwnerTS = ts
	r.Unlock()
}

func (dc *ddlCtx) getReorgCtx(jobID int64) *reorgCtx {
	dc.reorgCtx.RLock()
	defer dc.reorgCtx.RUnlock()
	return dc.reorgCtx.reorgCtxMap[jobID]
}

func (dc *ddlCtx) newReorgCtx(jobID int64, rowCount int64) *reorgCtx {
	dc.reorgCtx.Lock()
	defer dc.reorgCtx.Unlock()
	existedRC, ok := dc.reorgCtx.reorgCtxMap[jobID]
	if ok {
		existedRC.references.Add(1)
		return existedRC
	}
	rc := &reorgCtx{}
	rc.doneCh = make(chan reorgFnResult, 1)
	// initial reorgCtx
	rc.setRowCount(rowCount)
	rc.mu.warnings = make(map[errors.ErrorID]*terror.Error)
	rc.mu.warningsCount = make(map[errors.ErrorID]int64)
	rc.references.Add(1)
	dc.reorgCtx.reorgCtxMap[jobID] = rc
	return rc
}

func (dc *ddlCtx) removeReorgCtx(jobID int64) {
	dc.reorgCtx.Lock()
	defer dc.reorgCtx.Unlock()
	ctx, ok := dc.reorgCtx.reorgCtxMap[jobID]
	if ok {
		ctx.references.Sub(1)
		if ctx.references.Load() == 0 {
			delete(dc.reorgCtx.reorgCtxMap, jobID)
		}
	}
}
