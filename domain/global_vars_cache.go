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

package domain

import (
	"sync"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stmtsummary"
)

// GlobalVariableCache caches global variables.
type GlobalVariableCache struct {
	sync.RWMutex
	lastModify time.Time
	rows       []chunk.Row
	fields     []*ast.ResultField

	// Unit test may like to disable it.
	disable bool
}

const globalVariableCacheExpiry time.Duration = 2 * time.Second

// Update updates the global variable cache.
func (gvc *GlobalVariableCache) Update(rows []chunk.Row, fields []*ast.ResultField) {
	gvc.Lock()
	gvc.lastModify = time.Now()
	gvc.rows = rows
	gvc.fields = fields
	gvc.Unlock()

	checkEnableServerGlobalVar(rows)
}

// Get gets the global variables from cache.
func (gvc *GlobalVariableCache) Get() (succ bool, rows []chunk.Row, fields []*ast.ResultField) {
	gvc.RLock()
	defer gvc.RUnlock()
	if time.Since(gvc.lastModify) < globalVariableCacheExpiry {
		succ, rows, fields = !gvc.disable, gvc.rows, gvc.fields
		return
	}
	succ = false
	return
}

// Disable disables the global variable cache, used in test only.
func (gvc *GlobalVariableCache) Disable() {
	gvc.Lock()
	defer gvc.Unlock()
	gvc.disable = true
}

// checkEnableServerGlobalVar processes variables that acts in server and global level.
func checkEnableServerGlobalVar(rows []chunk.Row) {
	for _, row := range rows {
		sVal := ""
		if !row.IsNull(1) {
			sVal = row.GetString(1)
		}
		switch row.GetString(0) {
		case variable.TiDBEnableStmtSummary:
			stmtsummary.StmtSummaryByDigestMap.SetEnabled(sVal, false)
		case variable.TiDBStmtSummaryInternalQuery:
			stmtsummary.StmtSummaryByDigestMap.SetEnabledInternalQuery(sVal, false)
		case variable.TiDBStmtSummaryRefreshInterval:
			stmtsummary.StmtSummaryByDigestMap.SetRefreshInterval(sVal, false)
		case variable.TiDBStmtSummaryHistorySize:
			stmtsummary.StmtSummaryByDigestMap.SetHistorySize(sVal, false)
		case variable.TiDBStmtSummaryMaxStmtCount:
			stmtsummary.StmtSummaryByDigestMap.SetMaxStmtCount(sVal, false)
		case variable.TiDBStmtSummaryMaxSQLLength:
			stmtsummary.StmtSummaryByDigestMap.SetMaxSQLLength(sVal, false)
		case variable.TiDBCapturePlanBaseline:
			variable.CapturePlanBaseline.Set(sVal, false)
		}
	}
}

// GetGlobalVarsCache gets the global variable cache.
func (do *Domain) GetGlobalVarsCache() *GlobalVariableCache {
	return &do.gvc
}
