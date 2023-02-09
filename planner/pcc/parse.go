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

package pcc

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
)

// ParseText indicates ParseText
func ParseText(sql, explainText, version string) (Plan, error) {
	explainLines, err := trimAndSplitExplainResult(explainText)
	if err != nil {
		return Plan{}, err
	}
	sql = strings.TrimSpace(sql)
	sql = strings.TrimSuffix(sql, ";")
	ver := formatVersion(version)
	rows := splitRows(explainLines[3 : len(explainLines)-1])
	return Parse(ver, sql, rows)
}

// Parse indicates Parse
func Parse(version, sql string, explainRows [][]string) (_ Plan, err error) {
	defer func() {
		if r := recover(); r != nil {
			explainContent := ""
			for _, row := range explainRows {
				explainContent += strings.Join(row, "\t") + "\n"
			}
			fmt.Printf("parse sql=%v ver=%v panic\n, explain: %v\n\n", sql, version, explainContent)
			panic(r)
		}
	}()

	switch formatVersion(version) {
	case V6:
		return ParseV6(sql, explainRows)
	}
	return Plan{}, errors.Errorf("unsupported TiDB version %v", version)
}

// Compare indicates Compare
func Compare(p1, p2 Plan) (reason string, same bool) {
	if p1.SQL != p2.SQL {
		return "differentiate SQLs", false
	}
	p1.Root = removeProj(p1.Root)
	p2.Root = removeProj(p2.Root)
	return compare(p1.Root, p2.Root, fillInAlias(p1.SQL))
}

func removeProj(node Operator) Operator {
	if node.Type() == OpTypeProjection {
		return node.Children()[0]
	}
	for i, child := range node.Children() {
		node.SetChild(i, removeProj(child))
	}
	return node
}

type aliasVisitor struct {
	alias map[string]string
}

func (vis *aliasVisitor) Enter(n ast.Node) (ast.Node, bool) {
	tbl, ok := n.(*ast.TableSource)
	if !ok {
		return n, false
	}

	switch v := tbl.Source.(type) {
	case *ast.TableName:
		if tbl.AsName.L != "" {
			vis.alias[v.Name.L] = tbl.AsName.L
		}
	}
	return n, false
}

func (vis *aliasVisitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

func fillInAlias(sql string) (alias map[string]string) {
	alias = make(map[string]string)
	node, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return
	}
	vis := &aliasVisitor{alias}
	node.Accept(vis)
	return
}

// a simple DS has no Selection/Agg/Join under it
func extractDataSourceInfo(op Operator) (isSimple bool, tbl string) {
	switch op.Type() {
	case OpTypeTableScan:
		return true, op.(TableScanOp).Table
	case OpTypeIndexScan:
		return true, op.(IndexScanOp).Table
	case OpTypeTableReader, OpTypeIndexReader:
		return extractDataSourceInfo(op.Children()[0])
	case OpTypeIndexLookup:
		simpleIdx, tbl := extractDataSourceInfo(op.Children()[0])
		simpleTbl, _ := extractDataSourceInfo(op.Children()[1])
		return simpleIdx && simpleTbl, tbl
	}
	return false, ""
}

// PointGet is always better than Table/IndexReader and IndexLookUp when accessing the same table.
func specialHandlePointGet(op1, op2 Operator, tblAlias map[string]string) bool {
	if !OpTypeIsDataSource(op1.Type()) || op2.Type() != OpTypePointGet {
		return false
	}
	isSimple, tbl := extractDataSourceInfo(op1)
	point := op2.(PointGetOp)
	if isSimple && sameTable(point.Table, tbl, tblAlias) {
		return true
	}
	return false
}

func compare(op1, op2 Operator, tblAlias map[string]string) (reason string, same bool) {
	if specialHandlePointGet(op1, op2, tblAlias) {
		return "", true
	}

	if op1.Type() != op2.Type() || op1.Task() != op2.Task() {
		return fmt.Sprintf("different operators %v and %v", op1.ID(), op2.ID()), false
	}
	c1, c2 := op1.Children(), op2.Children()
	if len(c1) != len(c2) {
		return fmt.Sprintf("%v and %v have different children lengths", op1.ID(), op2.ID()), false
	}
	same = true
	switch op1.Type() {
	case OpTypeTableScan:
		t1, t2 := op1.(TableScanOp), op2.(TableScanOp)
		if !sameTable(t1.Table, t2.Table, tblAlias) {
			same = false
			reason = fmt.Sprintf("different table scan %v:%v, %v:%v", t1.ID(), t1.Table, t2.ID(), t2.Table)
		}
	case OpTypeIndexScan:
		t1, t2 := op1.(IndexScanOp), op2.(IndexScanOp)
		if !sameTable(t1.Table, t2.Table, tblAlias) || t1.Index != t2.Index {
			same = false
			reason = fmt.Sprintf("different index scan %v:%v:%v, %v:%v:%v", t1.ID(), t1.Table, t1.Index, t2.ID(), t2.Table, t2.Index)
		}
	}
	if !same {
		return reason, false
	}
	for i := range c1 {
		if reason, same = compare(c1[i], c2[i], tblAlias); !same {
			return reason, same
		}
	}
	return "", true
}

func sameTable(t1, t2 string, alias map[string]string) bool {
	if alias == nil {
		alias = make(map[string]string)
	}
	t1 = strings.ToLower(t1)
	t2 = strings.ToLower(t2)
	if t1 == t2 {
		return true
	}
	if alias[t1] != "" && alias[t1] == t2 {
		return true
	}
	if alias[t2] != "" && alias[t2] == t1 {
		return true
	}
	return false
}

func trimAndSplitExplainResult(explainResult string) ([]string, error) {
	lines := strings.Split(explainResult, "\n")
	var idx [3]int
	p := 0
	for i := range lines {
		if isSeparateLine(lines[i]) {
			idx[p] = i
			p++
			if p == 3 {
				break
			}
		}
	}
	if p != 3 {
		return nil, errors.Errorf("invalid explain result")
	}
	return lines[idx[0] : idx[2]+1], nil
}

func isSeparateLine(line string) bool {
	line = strings.TrimSpace(line)
	if len(line) == 0 {
		return false
	}
	for _, c := range line {
		if c != '+' && c != '-' {
			return false
		}
	}
	return true
}

func formatVersion(version string) string {
	version = strings.ToLower(version)
	if strings.HasPrefix(version, V6) {
		return V6
	}
	return VUnknown
}

func splitRows(rows []string) [][]string {
	results := make([][]string, 0, len(rows))
	for _, row := range rows {
		cols := strings.Split(row, "|")
		cols = cols[1 : len(cols)-1]
		results = append(results, cols)
	}
	return results
}

func findChildRowNo(rows [][]string, parentRowNo, idColNo int) []int {
	parent := []rune(rows[parentRowNo][idColNo])
	col := 0
	for col = range parent {
		c := parent[col]
		if c >= 'A' && c <= 'Z' {
			break
		}
	}
	if col >= len(parent) {
		return nil
	}
	childRowNo := make([]int, 0, 2)
	for i := parentRowNo + 1; i < len(rows); i++ {
		field := rows[i][idColNo]
		if col >= len([]rune(field)) {
			break
		}
		c := []rune(field)[col]
		if c == '├' || c == '└' {
			childRowNo = append(childRowNo, i)
		} else if c != '│' {
			break
		}
	}
	return childRowNo
}

func extractOperatorID(field string) string {
	return strings.TrimFunc(field, func(c rune) bool {
		return c == '└' || c == '─' || c == '│' || c == '├' || c == ' '
	})
}

func splitKVs(kvStr string) map[string]string {
	kvMap := make(map[string]string)
	kvs := strings.Split(kvStr, ",")
	for _, kv := range kvs {
		fields := strings.Split(kv, ":")
		if len(fields) == 2 {
			kvMap[strings.TrimSpace(fields[0])] = strings.TrimSpace(fields[1])
		}
	}
	return kvMap
}

func extractIndexColumns(indexStr string) string {
	be := strings.Index(indexStr, "(")
	ed := strings.Index(indexStr, ")")
	if be != -1 && ed != -1 {
		indexStr = indexStr[be+1 : ed]
	}
	return indexStr
}

func parseTaskType(taskStr string) TaskType {
	task := strings.TrimSpace(strings.ToLower(taskStr))
	if task == "root" {
		return TaskTypeRoot
	}
	if strings.Contains(task, "tiflash") {
		return TaskTypeTiFlash
	}
	return TaskTypeTiKV
}

// MatchOpType indicates MatchOpType
func MatchOpType(opID string) OpType {
	x := strings.ToLower(opID)
	if strings.Contains(x, "agg") {
		if strings.Contains(x, "hash") {
			return OpTypeHashAgg
		} else if strings.Contains(x, "stream") {
			return OpTypeStreamAgg
		}
		return OpTypeUnknown
	}
	if strings.Contains(x, "join") {
		if strings.Contains(x, "hash") {
			return OpTypeHashJoin
		} else if strings.Contains(x, "merge") {
			return OpTypeMergeJoin
		} else if strings.Contains(x, "index") {
			return OpTypeIndexJoin
		}
		return OpTypeUnknown
	}
	if strings.Contains(x, "table") {
		if strings.Contains(x, "reader") {
			return OpTypeTableReader
		} else if strings.Contains(x, "scan") {
			return OpTypeTableScan
		} else if strings.Contains(x, "dual") {
			return OpTypeTableDual
		}
		return OpTypeUnknown
	}
	if strings.Contains(x, "index") {
		if strings.Contains(x, "reader") {
			return OpTypeIndexReader
		} else if strings.Contains(x, "scan") {
			return OpTypeIndexScan
		} else if strings.Contains(x, "lookup") {
			return OpTypeIndexLookup
		}
		return OpTypeUnknown
	}
	if strings.Contains(x, "selection") {
		return OpTypeSelection
	}
	if strings.Contains(x, "projection") {
		return OpTypeProjection
	}
	if strings.Contains(x, "point") {
		return OpTypePointGet
	}
	if strings.Contains(x, "maxonerow") {
		return OpTypeMaxOneRow
	}
	if strings.Contains(x, "apply") {
		return OpTypeApply
	}
	if strings.Contains(x, "limit") {
		return OpTypeLimit
	}
	if strings.Contains(x, "sort") {
		return OpTypeSort
	}
	if strings.Contains(x, "topn") {
		return OpTypeTopN
	}
	if strings.Contains(x, "selectlock") {
		return OpTypeSelectLock
	}
	return OpTypeUnknown
}

// FormatExplainRows indicates FormatExplainRows
func FormatExplainRows(rows [][]string) string {
	if len(rows) == 0 {
		return ""
	}
	nRows := len(rows)
	nCols := len(rows[0])
	fmtRows := make([]string, nRows)
	for col := 0; col < nCols; col++ {
		lengest := 0
		for i := 0; i < nRows; i++ {
			if len(fmtRows[i]) > lengest {
				lengest = len(fmtRows[i])
			}
		}
		for i := 0; i < nRows; i++ {
			gap := lengest - len(fmtRows[i])
			fmtRows[i] += strings.Repeat(" ", gap)
			if col != nCols-1 && col != 0 {
				fmtRows[i] += "  |  "
			}
			fmtRows[i] += rows[i][col]
		}
	}
	return strings.Join(fmtRows, "\n")
}
