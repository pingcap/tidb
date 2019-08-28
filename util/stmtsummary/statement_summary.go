// Copyright 2016 PingCAP, Inc.
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

package stmtsummary

import (
	"sync"

	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/types"
)

type StmtSummaryByDigest struct {
	sync.RWMutex
	summaryMap map[string]*stmtSummary
}

// Summary of each type of statements.
type stmtSummary struct {
	schemaName      string
	normalizedSql   string
	sampleSql       string
	sumTotalLatency uint64
	maxLatency      uint64
	minLatency      uint64
	sumRowsAffected uint64
	sumRowsSent     uint64
}

// Used for recording execution status of each statement.
type StmtExecInfo struct {
	schemaName   string
	originalSql  string
	totalLatency uint64
	rowsAffected uint64
	rowsSent     uint64
}

// Convert StmtExecInfo to stmtSummary
func convertStmtExecInfoToSummary(sei *StmtExecInfo, normalizedSql string) *stmtSummary {
	return nil
}

// Add a new StmtExecInfo to stmtSummary
func addStmtExecInfoToSummary(sei *StmtExecInfo, ss *stmtSummary) {
	return
}

func (ss *StmtSummaryByDigest) AddStatement(sei *StmtExecInfo) {
	normalizeSQL := parser.Normalize(sei.originalSql)
	digest := parser.DigestHash(normalizeSQL)

	ss.Lock()
	summary, ok := ss.summaryMap[digest]
	if ok {
		addStmtExecInfoToSummary(sei, summary)
	} else {
		maxStmtCount := config.GetGlobalConfig().StmtSummary.MaxStmtCount
		if len(ss.summaryMap) >= int(maxStmtCount) {
			ss.removeLeastUsed()
		}

		summary = convertStmtExecInfoToSummary(sei, normalizeSQL)
		ss.summaryMap[digest] = summary
	}
	ss.Unlock()
}

func (ss *StmtSummaryByDigest) removeLeastUsed() {

}

func (ss *StmtSummaryByDigest) ToDatum() [][]types.Datum {
	var rows [][]types.Datum

	ss.RLock()
	// TODO
	ss.RUnlock()

	return rows
}
