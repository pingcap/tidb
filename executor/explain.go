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

package executor

import (
	"strings"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/types"
)

type explainEntry struct {
	ID           int64
	selectType   string
	table        string
	joinType     string
	possibleKeys string
	key          string
	keyLen       string
	ref          string
	rows         int64
	extra        []string
}

// ExplainExec represents an explain executor.
// See https://dev.mysql.com/doc/refman/5.7/en/explain-output.html
type ExplainExec struct {
	StmtPlan plan.Plan
	fields   []*ast.ResultField
	rows     []*Row
	cursor   int
}

// Schema implements Executor Schema interface.
func (e *ExplainExec) Schema() expression.Schema {
	return nil
}

// Fields implements Executor Fields interface.
func (e *ExplainExec) Fields() []*ast.ResultField {
	return e.fields
}

// Next implements Execution Next interface.
func (e *ExplainExec) Next() (*Row, error) {
	if e.rows == nil {
		e.fetchRows()
	}
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	row := e.rows[e.cursor]
	e.cursor++
	return row, nil
}

func (e *ExplainExec) fetchRows() {
	visitor := &explainVisitor{id: 1}
	visitor.explain(e.StmtPlan)
	for _, entry := range visitor.entries {
		row := &Row{}
		row.Data = types.MakeDatums(
			entry.ID,
			entry.selectType,
			entry.table,
			entry.joinType,
			entry.key,
			entry.key,
			entry.keyLen,
			entry.ref,
			entry.rows,
			strings.Join(entry.extra, "; "),
		)
		for i := range row.Data {
			if row.Data[i].Kind() == types.KindString && row.Data[i].GetString() == "" {
				row.Data[i].SetNull()
			}
		}
		e.rows = append(e.rows, row)
	}
}

// Close implements Executor Close interface.
func (e *ExplainExec) Close() error {
	return nil
}

type explainVisitor struct {
	id int64

	// Sort extra should be appended in the first table in a join.
	sort    bool
	entries []*explainEntry
}

func (v *explainVisitor) explain(p plan.Plan) {
	switch x := p.(type) {
	case *plan.PhysicalTableScan:
		v.entries = append(v.entries, v.newEntryForTableScan(x))
	case *plan.PhysicalIndexScan:
		v.entries = append(v.entries, v.newEntryForIndexScan(x))
	case *plan.NewSort:
		v.sort = true
	}

	for _, c := range p.GetChildren() {
		v.explain(c)
	}
}

func (v *explainVisitor) newEntryForTableScan(p *plan.PhysicalTableScan) *explainEntry {
	entry := &explainEntry{
		ID:         v.id,
		selectType: "SIMPLE",
		table:      p.Table.Name.O,
	}
	if entry.joinType != "ALL" {
		entry.key = "PRIMARY"
		entry.keyLen = "8"
	}
	if len(p.AccessCondition) > 0 {
		entry.extra = append(entry.extra, "Using where")
	}

	v.setSortExtra(entry)
	return entry
}

func (v *explainVisitor) newEntryForIndexScan(p *plan.PhysicalIndexScan) *explainEntry {
	entry := &explainEntry{
		ID:         v.id,
		selectType: "SIMPLE",
		table:      p.Table.Name.O,
		key:        p.Index.Name.O,
	}
	if len(p.AccessCondition) > 0 {
		entry.extra = append(entry.extra, "Using where")
	}

	v.setSortExtra(entry)
	return entry
}

func (v *explainVisitor) setSortExtra(entry *explainEntry) {
	if v.sort {
		entry.extra = append(entry.extra, "Using filesort")
		v.sort = false
	}
}
