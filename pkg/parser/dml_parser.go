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
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// markIsInBraces sets IsInBraces=true on a SelectStmt or SetOprStmt node.
func markIsInBraces(node ast.Node) {
	if s, ok := node.(*ast.SelectStmt); ok {
		s.IsInBraces = true
	} else if s, ok := node.(*ast.SetOprStmt); ok {
		s.IsInBraces = true
	}
}

// parsePriority parses optional LOW_PRIORITY / HIGH_PRIORITY / DELAYED modifiers.
func (p *HandParser) parsePriority() mysql.PriorityEnum {
	switch p.peek().Tp {
	case lowPriority:
		p.next()
		return mysql.LowPriority
	case highPriority:
		p.next()
		return mysql.HighPriority
	case delayed:
		p.next()
		return mysql.DelayedPriority
	}
	return mysql.NoPriority
}

// wrapTableNameInRefs wraps a TableName in the standard TableSource → Join → TableRefsClause chain.
func (p *HandParser) wrapTableNameInRefs(tn *ast.TableName) *ast.TableRefsClause {
	ts := Alloc[ast.TableSource](p.arena)
	ts.Source = tn
	join := p.arena.AllocJoin()
	join.Left = ts
	clause := Alloc[ast.TableRefsClause](p.arena)
	clause.TableRefs = join
	return clause
}

// parseInsertStmt parses INSERT [INTO] table [(col_list)] VALUES (val_list), ... | SELECT ...
// Also handles REPLACE which uses the same AST structure.
func (p *HandParser) parseInsertStmt(isReplace bool) *ast.InsertStmt {
	stmt := Alloc[ast.InsertStmt](p.arena)
	stmt.IsReplace = isReplace
	stmt.PartitionNames = make([]ast.CIStr, 0)

	// Consume INSERT or REPLACE.
	p.next()

	// Optional optimizer hints (/*+ ... */).
	stmt.TableHints = p.parseOptHints()

	// Optional priority modifiers (INSERT and REPLACE).
	stmt.Priority = p.parsePriority()

	// Optional IGNORE.
	if _, ok := p.accept(ignore); ok {
		stmt.IgnoreErr = true
	}

	// Optional INTO.
	p.accept(into)

	// Table reference. For INSERT the parser uses TableName + PartitionNameListOpt.
	tn := p.parseTableName()
	if tn == nil {
		return nil
	}

	// Optional PARTITION clause.
	if _, ok := p.accept(partition); ok {
		p.expect('(')
		var names []ast.CIStr
		for {
			tok := p.next()
			names = append(names, ast.NewCIStr(tok.Lit))
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
		tn.PartitionNames = names
	}

	// Wrap TableName in standard TableSource -> Join -> TableRefsClause.
	stmt.Table = p.wrapTableNameInRefs(tn)

	// Optional column list.
	var hasColumnList bool
	if _, ok := p.accept('('); ok {
		hasColumnList = true
		// Could be: empty (), column list, or (SELECT ...).
		if p.peek().Tp == ')' {
			// Empty column list: INSERT INTO foo () VALUES ()
			p.next() // consume ')'
			stmt.Columns = make([]*ast.ColumnName, 0)
		} else if p.peek().Tp == selectKwd || p.peek().Tp == with {
			// Subquery in parens: INSERT INTO t1 (SELECT ...).
			query := p.parseSelectStmt()
			res := p.maybeParseUnion(query)
			p.expect(')')
			// Mark the select as "in braces" so Restore() preserves the parens.
			markIsInBraces(res)
			stmt.Select = res
		} else {
			// Column list: (c1, c2, ...)
			for {
				stmt.Columns = append(stmt.Columns, p.parseColumnName())
				if _, ok := p.accept(','); !ok {
					break
				}
			}
			p.expect(')')
		}
	}

	// VALUES or SELECT
	switch p.peek().Tp {
	case values, value: // VALUES
		stmt.Lists = p.parseValueList(isReplace, false)
	case selectKwd: // SELECT
		sel := p.parseSelectStmt()
		stmt.Select = p.maybeParseUnion(sel)
	case with: // WITH [RECURSIVE] cte_list SELECT ...
		withStmt := p.parseWithStmt()
		if rs, ok := withStmt.(ast.ResultSetNode); ok {
			stmt.Select = rs
		}
	case tableKwd: // TABLE
		// INSERT INTO ... TABLE ...
		stmt.Select = p.parseTableStmt().(*ast.SelectStmt)
	case '(': // (SELECT ...) or (VALUES ...)
		p.next()
		if p.peek().Tp == selectKwd {
			sel := p.parseSelectStmt()
			stmt.Select = p.maybeParseUnion(sel)
			p.expect(')')
			markIsInBraces(stmt.Select)
		} else if p.peek().Tp == values { // (VALUES ...)
			stmt.Lists = p.parseValueList(isReplace, false) // subquery VALUES? Assuming false for now.
			p.expect(')')
		} else { //revive:disable-line
			// Assuming recursive calls handle complex cases.
		}
	case set: // SET c1=v1
		if hasColumnList {
			p.syntaxErrorAt(p.peek().Offset)
			return nil
		}
		// INSERT INTO t SET c1=v1
		p.next()
		stmt.Setlist = true
		// We need to populate Lists with one row.
		var row []ast.ExprNode
		for {
			assign := p.parseAssignment()
			if assign == nil {
				return nil
			}
			stmt.Columns = append(stmt.Columns, assign.Column)
			row = append(row, assign.Expr)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		stmt.Lists = append(stmt.Lists, row)
	}

	// ON DUPLICATE KEY UPDATE
	if _, ok := p.accept(on); ok {
		p.expect(duplicate) // duplicate
		p.expect(key)
		p.expect(update)
		for {
			assign := p.parseAssignment()
			stmt.OnDuplicate = append(stmt.OnDuplicate, assign)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}

	return stmt
}

// parseValueList parses VALUES (v1, v2), (v3, v4), ...
func (p *HandParser) parseValueList(isReplace, enforceRow bool) [][]ast.ExprNode { //revive:disable-line
	p.expectAny(values, value)

	var lists [][]ast.ExprNode
	for {
		// (val1, val2, ...) || ROW(val1, val2, ...)
		if _, ok := p.accept(row); ok { //revive:disable-line
			// consume ROW
		} else if enforceRow {
			// Report error at the current position (before consuming),
			// matching the yacc grammar's error offset.
			tok := p.peek()
			p.errorNear(tok.Offset+1, tok.Offset)
			return nil
		}
		p.expect('(')
		list := make([]ast.ExprNode, 0)
		if p.peek().Tp != ')' {
			for {
				list = append(list, p.parseExpression(precNone))
				if _, ok := p.accept(','); !ok {
					break
				}
			}
		}
		p.expect(')')
		lists = append(lists, list)

		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return lists
}

// parseAssignment parses col = expr | col = DEFAULT
func (p *HandParser) parseAssignment() *ast.Assignment {
	col := p.parseColumnName()
	if col == nil {
		return nil
	}
	p.expectAny(eq, assignmentEq)

	node := Alloc[ast.Assignment](p.arena)
	node.Column = col

	if _, ok := p.accept(defaultKwd); ok {
		node.Expr = p.arena.AllocDefaultExpr()
	} else {
		node.Expr = p.parseExpression(precNone)
	}
	return node
}

// parseUpdateStmt parses UPDATE [LOW_PRIORITY] [IGNORE] table_reference SET col_name1={expr1|DEFAULT} [,
// col_name2={expr2|DEFAULT}] ... [WHERE where_condition] [ORDER BY ...] [LIMIT row_count]
func (p *HandParser) parseUpdateStmt() ast.StmtNode {
	stmt := Alloc[ast.UpdateStmt](p.arena)
	p.expect(update)

	// Optional optimizer hints (/*+ ... */).
	stmt.TableHints = p.parseOptHints()

	// [LOW_PRIORITY | HIGH_PRIORITY | DELAYED]
	stmt.Priority = p.parsePriority()

	// [IGNORE]
	if _, ok := p.accept(ignore); ok {
		stmt.IgnoreErr = true
	}

	// Table reference
	stmt.TableRefs = p.parseTableRefs()

	// SET
	p.expect(set)
	for {
		assign := p.parseAssignment()
		stmt.List = append(stmt.List, assign)
		if _, ok := p.accept(','); !ok {
			break
		}
	}

	// [WHERE]
	if _, ok := p.accept(where); ok {
		stmt.Where = p.parseExpression(precNone)
	}

	// [ORDER BY] and [LIMIT]. Multi-table UPDATE does not support them.
	isMultiTable := false
	if stmt.TableRefs != nil && stmt.TableRefs.TableRefs != nil {
		if stmt.TableRefs.TableRefs.Right != nil {
			isMultiTable = true
		}
	}
	// Propagate multipleTable flag to the AST node so downstream consumers
	// (planner, executor) can distinguish single-table vs multi-table updates.
	stmt.MultipleTable = isMultiTable

	if p.peek().Tp == order {
		if isMultiTable {
			p.error(p.peek().Offset, "Incorrect usage of UPDATE and ORDER BY")
			return nil
		}
		stmt.Order = p.parseOrderByClause()
	}

	// [LIMIT]
	if p.peek().Tp == limit {
		if isMultiTable {
			p.error(p.peek().Offset, "Incorrect usage of UPDATE and LIMIT")
			return nil
		}
		stmt.Limit = p.parseLimitClause()
	}

	return stmt
}

// parseDeleteStmt parses DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
// [PARTITION (partition_name_list)] [WHERE where_condition] [ORDER BY ...] [LIMIT row_count]
// OR Multi-table syntax.
func (p *HandParser) parseDeleteStmt() ast.StmtNode {
	stmt := Alloc[ast.DeleteStmt](p.arena)
	stmt.IsMultiTable = false
	stmt.BeforeFrom = false
	p.expect(deleteKwd)

	// Optional optimizer hints (/*+ ... */).
	stmt.TableHints = p.parseOptHints()

	// [LOW_PRIORITY | HIGH_PRIORITY | DELAYED]
	stmt.Priority = p.parsePriority()

	// [QUICK]
	if _, ok := p.accept(quick); ok {
		stmt.Quick = true
	}

	// [IGNORE]
	if _, ok := p.accept(ignore); ok {
		stmt.IgnoreErr = true
	}

	// DELETE FROM ...
	// Multi-table: DELETE t1, t2 FROM t1, t2, t3 ...
	// Single-table: DELETE FROM t1 ...

	if p.peek().Tp != from {
		stmt.IsMultiTable = true
		stmt.BeforeFrom = true
		stmt.Tables = p.parseDeleteTableList()
	}

	p.expect(from)

	// Table refs
	stmt.TableRefs = p.parseTableRefs()

	// [USING]
	// Only accept USING in "DELETE FROM t1, t2 USING ..." form (BeforeFrom==false).
	// When BeforeFrom==true ("DELETE t1, t2 FROM t JOIN t1 ON ..."), the USING
	// keyword does not belong to DELETE; it may be part of an outer statement
	// (e.g., CREATE BINDING ... FOR ... USING ...).
	if !stmt.BeforeFrom {
		if _, ok := p.accept(using); ok {
			// DELETE FROM t1, t2 USING t1, t2, t3 ...
			if stmt.IsMultiTable { //revive:disable-line
				// Already is multi-table. e.g. DELETE t1 FROM t2 USING t3
			} else {
				// Was DELETE FROM t1 ... USING ...
				// Convert single-table DELETE FROM t1 to Multi-table.
				stmt.IsMultiTable = true
				stmt.BeforeFrom = false
				stmt.Tables = p.convertToTableList(stmt.TableRefs)
				if stmt.Tables == nil { //revive:disable-line
					// Failed to convert
				}
				// Parse the USING tables as new TableRefs (sources).
				stmt.TableRefs = p.parseTableRefs()
			}
		} else {
			// No USING clause.
			if stmt.TableRefs != nil {
				var validateAndClear func(node ast.ResultSetNode) bool
				validateAndClear = func(node ast.ResultSetNode) bool {
					if j, ok := node.(*ast.Join); ok {
						left := validateAndClear(j.Left)
						right := false
						if j.Right != nil {
							right = validateAndClear(j.Right)
						}
						return left || right
					}
					if ts, ok := node.(*ast.TableSource); ok {
						if tn, ok := ts.Source.(*ast.TableName); ok {
							has := tn.HasWildcard
							tn.HasWildcard = false
							return has
						}
					}
					return false
				}

				var hasWildcard bool
				if stmt.TableRefs.TableRefs != nil {
					hasWildcard = validateAndClear(stmt.TableRefs.TableRefs)
				}

				if hasWildcard && !stmt.IsMultiTable {
					p.syntaxErrorAt(p.peek().Offset)
					return nil
				}
			}
		}
	} else {
		// BeforeFrom==true: "DELETE t1, t2 FROM ..."
		// Clear wildcards in table refs for AST normalization.
		if stmt.TableRefs != nil {
			var clearWildcard func(node ast.ResultSetNode)
			clearWildcard = func(node ast.ResultSetNode) {
				if j, ok := node.(*ast.Join); ok {
					clearWildcard(j.Left)
					if j.Right != nil {
						clearWildcard(j.Right)
					}
				}
				if ts, ok := node.(*ast.TableSource); ok {
					if tn, ok := ts.Source.(*ast.TableName); ok {
						tn.HasWildcard = false
					}
				}
			}
			if stmt.TableRefs.TableRefs != nil {
				clearWildcard(stmt.TableRefs.TableRefs)
			}
		}
	}

	// [WHERE]
	if _, ok := p.accept(where); ok {
		stmt.Where = p.parseExpression(precNone)
	}

	// [ORDER BY] and [LIMIT] are only valid for single-table DELETE.
	if !stmt.IsMultiTable {
		if p.peek().Tp == order {
			stmt.Order = p.parseOrderByClause()
		}

		if p.peek().Tp == limit {
			stmt.Limit = p.parseLimitClause()
		}
	}

	// Clear wildcard in Tables (targets) to ensure AST normalization.
	if stmt.Tables != nil {
		for _, t := range stmt.Tables.Tables {
			t.HasWildcard = false
		}
	}

	return stmt
}

// convertToTableList converts TableRefsClause (from parsing FROM ...) into DeleteTableList.
// Used when we encounter USING and need to treat FROM clause as Targets.
func (p *HandParser) convertToTableList(refs *ast.TableRefsClause) *ast.DeleteTableList {
	if refs == nil || refs.TableRefs == nil {
		return nil
	}
	list := Alloc[ast.DeleteTableList](p.arena)

	// Helper to extract TableName from Join tree.
	var visit func(node ast.ResultSetNode) bool
	visit = func(node ast.ResultSetNode) bool {
		if j, ok := node.(*ast.Join); ok {
			// Recurse left
			if !visit(j.Left) {
				return false
			}
			// Recurse right if present
			if j.Right != nil {
				return visit(j.Right)
			}
			return true
		}
		if ts, ok := node.(*ast.TableSource); ok {
			if tn, ok := ts.Source.(*ast.TableName); ok {
				list.Tables = append(list.Tables, tn)
				return true
			}
		}
		// Subquery or other source cannot be deleted from in this manner?
		// MySQL allows deleting from aliases.
		return false
	}

	if !visit(refs.TableRefs) {
		return nil
	}
	return list
}

// parseDeleteTableList parses t1, t2 for DELETE t1, t2 FROM ...
func (p *HandParser) parseDeleteTableList() *ast.DeleteTableList {
	list := Alloc[ast.DeleteTableList](p.arena)
	for {
		tn := p.parseTableName()
		// .*.
		if p.peek().Tp == '.' && p.peekN(1).Tp == '*' {
			p.next() // .
			p.next() // *
		}
		list.Tables = append(list.Tables, tn)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return list
}

// parseTableStmt parses TABLE t1 [ORDER BY ...] [LIMIT ...]
func (p *HandParser) parseTableStmt() ast.StmtNode {
	// TABLE t1 -> SELECT * FROM t1
	stmt := p.arena.AllocSelectStmt()
	stmt.Kind = ast.SelectStmtKindTable
	p.expect(tableKwd)

	tn := p.parseTableName()
	stmt.From = p.wrapTableNameInRefs(tn)

	p.parseSelectStmtSuffix(stmt)

	// Set * as Fields
	wildCard := Alloc[ast.SelectField](p.arena)
	wildCard.WildCard = &ast.WildCardField{}
	stmt.Fields = &ast.FieldList{Fields: []*ast.SelectField{wildCard}}

	return stmt
}

// parseValuesStmt parses VALUES (1,2), (3,4) ...
func (p *HandParser) parseValuesStmt() ast.StmtNode {
	stmt := p.arena.AllocSelectStmt()
	stmt.Kind = ast.SelectStmtKindValues

	rawLists := p.parseValueList(false, true)
	// Convert [][]ExprNode to []*RowExpr
	for _, rawRow := range rawLists {
		rowExpr := Alloc[ast.RowExpr](p.arena)
		rowExpr.Values = rawRow
		stmt.Lists = append(stmt.Lists, rowExpr)
	}

	p.parseSelectStmtSuffix(stmt)

	return stmt
}

// parseNonTransactionalDMLStmt parses: BATCH [ON column] [LIMIT N] [DRY RUN [QUERY]] <DML>
func (p *HandParser) parseNonTransactionalDMLStmt() ast.StmtNode {
	stmt := Alloc[ast.NonTransactionalDMLStmt](p.arena)
	p.expect(batch)

	if _, ok := p.accept(on); ok {
		stmt.ShardColumn = p.parseColumnName()
	}

	if _, ok := p.accept(limit); ok {
		if tok, ok := p.expect(intLit); ok {
			stmt.Limit = tokenItemToUint64(tok.Item)
		}
	}

	// DRY RUN
	if _, ok := p.accept(dry); ok {
		p.expect(run)
		stmt.DryRun = ast.DryRunSplitDml
		if ok := p.peek().Tp == query; ok {
			p.next()
			stmt.DryRun = ast.DryRunQuery
		} else if p.peek().IsKeyword("QUERY") {
			p.next()
			stmt.DryRun = ast.DryRunQuery
		}
	}

	var dml ast.StmtNode
	switch p.peek().Tp {
	case insert:
		dml = p.parseInsertStmt(false)
	case replace:
		dml = p.parseInsertStmt(true)
	case update:
		dml = p.parseUpdateStmt()
	case deleteKwd:
		dml = p.parseDeleteStmt()
	default:
		p.syntaxErrorAt(p.peek().Offset)
		return nil
	}

	if shardable, ok := dml.(ast.ShardableDMLStmt); ok { //revive:disable-line
		stmt.DMLStmt = shardable
	} else {
		p.syntaxErrorAt(p.peek().Offset)
		return nil
	}

	return stmt
}

// parseSelectStmtSuffix parses the common suffix for TABLE/VALUES statements:
// [ORDER BY ...] [LIMIT ...] [INTO OUTFILE ...]
func (p *HandParser) parseSelectStmtSuffix(stmt *ast.SelectStmt) {
	if p.peek().Tp == order {
		stmt.OrderBy = p.parseOrderByClause()
	}
	if p.peek().Tp == limit {
		stmt.Limit = p.parseLimitClause()
	}
	if p.peek().Tp == forKwd || p.peek().Tp == lock {
		stmt.LockInfo = p.parseSelectLock()
	}
	if p.peek().Tp == into {
		stmt.SelectIntoOpt = p.parseSelectIntoOption()
	}
}
