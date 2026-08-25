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

// Package visitor_codegen generates AST traversal methods for InPlaceVisitor.
package visitor_codegen

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

var defaultTraversalFiles = []string{
	"ddl.go",
	"dml.go",
	"expressions.go",
	"functions.go",
	"misc.go",
	"procedure.go",
	"stats.go",
}

// defaultExpectedReceivers is deliberately explicit. A new or removed Accept
// method must update the generator contract rather than silently changing the
// generated receiver set.
var defaultExpectedReceivers = []string{
	"AddQueryWatchStmt",
	"AdminStmt",
	"AggregateFuncExpr",
	"AlterDatabaseStmt",
	"AlterInstanceStmt",
	"AlterPlacementPolicyStmt",
	"AlterRangeStmt",
	"AlterResourceGroupStmt",
	"AlterSequenceStmt",
	"AlterTableSpec",
	"AlterTableStmt",
	"AlterUserStmt",
	"AnalyzeTableStmt",
	"AsOfClause",
	"Assignment",
	"AttributesSpec",
	"BRIEStmt",
	"BeginStmt",
	"BetweenExpr",
	"BinaryOperationExpr",
	"BinlogStmt",
	"ByItem",
	"CalibrateResourceStmt",
	"CallStmt",
	"CancelDistributionJobStmt",
	"CaseExpr",
	"CleanupTableLockStmt",
	"ColumnDef",
	"ColumnName",
	"ColumnNameExpr",
	"ColumnNameOrUserVar",
	"ColumnOption",
	"ColumnPosition",
	"CommitStmt",
	"CommonTableExpression",
	"CompactTableStmt",
	"CompareSubqueryExpr",
	"Constraint",
	"CreateBindingStmt",
	"CreateDatabaseStmt",
	"CreateIndexStmt",
	"CreateMaskingPolicyStmt",
	"CreatePlacementPolicyStmt",
	"CreateResourceGroupStmt",
	"CreateSequenceStmt",
	"CreateStatisticsStmt",
	"CreateTableStmt",
	"CreateUserStmt",
	"CreateViewStmt",
	"DeallocateStmt",
	"DefaultExpr",
	"DeleteStmt",
	"DeleteTableList",
	"DistributeTableStmt",
	"DoStmt",
	"DropBindingStmt",
	"DropDatabaseStmt",
	"DropIndexStmt",
	"DropPlacementPolicyStmt",
	"DropProcedureStmt",
	"DropQueryWatchStmt",
	"DropResourceGroupStmt",
	"DropSequenceStmt",
	"DropStatisticsStmt",
	"DropStatsStmt",
	"DropTableStmt",
	"DropUserStmt",
	"DynamicCalibrateResourceOption",
	"ExecuteStmt",
	"ExistsSubqueryExpr",
	"ExplainForStmt",
	"ExplainStmt",
	"FieldList",
	"FlashBackDatabaseStmt",
	"FlashBackTableStmt",
	"FlashBackToTimestampStmt",
	"FlushStmt",
	"FrameBound",
	"FrameClause",
	"FuncCallExpr",
	"FuncCastExpr",
	"GetFormatSelectorExpr",
	"GrantProxyStmt",
	"GrantRoleStmt",
	"GrantStmt",
	"GroupByClause",
	"HavingClause",
	"HelpStmt",
	"ImportIntoActionStmt",
	"ImportIntoStmt",
	"IndexLockAndAlgorithm",
	"IndexOption",
	"IndexPartSpecification",
	"InsertStmt",
	"IsNullExpr",
	"IsTruthExpr",
	"JSONSumCrc32Expr",
	"Join",
	"KillStmt",
	"Limit",
	"LoadDataStmt",
	"LoadStatsStmt",
	"LockStatsStmt",
	"LockTablesStmt",
	"MatchAgainst",
	"MaxValueExpr",
	"NonTransactionalDMLStmt",
	"OnCondition",
	"OnDeleteOpt",
	"OnUpdateOpt",
	"OptimizeTableStmt",
	"OrderByClause",
	"ParenthesesExpr",
	"PartitionByClause",
	"PartitionOptions",
	"PatternInExpr",
	"PatternLikeOrIlikeExpr",
	"PatternRegexpExpr",
	"PlanReplayerStmt",
	"PositionExpr",
	"PrepareStmt",
	"PrivElem",
	"ProcedureBlock",
	"ProcedureCloseCur",
	"ProcedureCursor",
	"ProcedureDecl",
	"ProcedureElseBlock",
	"ProcedureElseIfBlock",
	"ProcedureErrorCon",
	"ProcedureErrorControl",
	"ProcedureErrorState",
	"ProcedureErrorVal",
	"ProcedureFetchInto",
	"ProcedureIfBlock",
	"ProcedureIfInfo",
	"ProcedureInfo",
	"ProcedureJump",
	"ProcedureLabelBlock",
	"ProcedureLabelLoop",
	"ProcedureOpenCur",
	"ProcedureRepeatStmt",
	"ProcedureWhileStmt",
	"QueryWatchOption",
	"QueryWatchTextOption",
	"RecommendIndexStmt",
	"RecoverTableStmt",
	"ReferenceDef",
	"RefreshStatsStmt",
	"ReleaseSavepointStmt",
	"RenameTableStmt",
	"RenameUserStmt",
	"RepairTableStmt",
	"ResourceGroupRunawayActionOption",
	"RestartStmt",
	"RevokeRoleStmt",
	"RevokeStmt",
	"RollbackStmt",
	"RowExpr",
	"SavepointStmt",
	"SearchCaseStmt",
	"SearchWhenThenStmt",
	"SelectField",
	"SelectIntoOption",
	"SelectStmt",
	"SetBindingStmt",
	"SetCollationExpr",
	"SetConfigStmt",
	"SetDefaultRoleStmt",
	"SetOprSelectList",
	"SetOprStmt",
	"SetPwdStmt",
	"SetResourceGroupStmt",
	"SetRoleStmt",
	"SetSessionStatesStmt",
	"SetStmt",
	"ShowStmt",
	"ShutdownStmt",
	"SimpleCaseStmt",
	"SimpleWhenThenStmt",
	"SplitIndexOption",
	"SplitOption",
	"SplitRegionStmt",
	"StatsOptionsSpec",
	"StoreParameter",
	"StringOrUserVar",
	"SubqueryExpr",
	"TableName",
	"TableNameExpr",
	"TableOptimizerHint",
	"TableOption",
	"TableRefsClause",
	"TableSample",
	"TableSource",
	"TableToTable",
	"TimeUnitExpr",
	"TraceStmt",
	"TrafficStmt",
	"TrimDirectionExpr",
	"TruncateTableStmt",
	"UnaryOperationExpr",
	"UnlockStatsStmt",
	"UnlockTablesStmt",
	"UpdateStmt",
	"UseStmt",
	"UserToUser",
	"ValuesExpr",
	"VariableAssignment",
	"VariableExpr",
	"WhenClause",
	"WildCardField",
	"WindowFuncExpr",
	"WindowSpec",
	"WithClause",
}

// GenerateRequest describes the source package and the receiver inventory that
// generation must cover completely.
type GenerateRequest struct {
	SourceDir         string
	FS                fs.FS
	TraversalFiles    []string
	ExpectedReceivers []string
}

// GenerateResult contains deterministic formatted source and its sorted
// receiver inventory.
type GenerateResult struct {
	Source    []byte
	Receivers []string
}

type parsedPackage struct {
	fset      *token.FileSet
	name      string
	files     map[string]*ast.File
	types     map[string]*ast.TypeSpec
	methods   map[string]map[string]string
	receivers map[string]struct{}
}

type localBinding struct {
	typ           ast.Expr
	storage       ast.Expr
	rangeCopy     bool
	replacementOf ast.Expr
}

type methodTransformer struct {
	pkg          *parsedPackage
	filename     string
	receiverName string
	receiverVar  string
	visitorVar   string
	locals       map[string]localBinding
	enterResult  string
}

// Generate parses legacy Accept methods and emits their write-free in-place
// traversal counterparts. It returns no source unless the complete expected
// receiver set is present and every method is recognized.
func Generate(request GenerateRequest) (GenerateResult, error) {
	pkg, traversalFiles, err := parsePackage(request)
	if err != nil {
		return GenerateResult{}, err
	}

	receivers := make([]string, 0, len(pkg.receivers))
	for receiver := range pkg.receivers {
		receivers = append(receivers, receiver)
	}
	sort.Strings(receivers)
	expected := request.ExpectedReceivers
	if expected == nil {
		expected = defaultExpectedReceivers
	}
	expected = append([]string(nil), expected...)
	sort.Strings(expected)
	if !equalStrings(receivers, expected) {
		return GenerateResult{}, fmt.Errorf("receiver inventory mismatch: got %v, expected %v", receivers, expected)
	}

	generated := &ast.File{Name: ast.NewIdent(pkg.name)}
	for _, filename := range traversalFiles {
		file := pkg.files[filename]
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !isAcceptMethod(fn) {
				continue
			}
			transformed, transformErr := transformAccept(pkg, filename, fn)
			if transformErr != nil {
				return GenerateResult{}, transformErr
			}
			generated.Decls = append(generated.Decls, transformed)
		}
	}
	clearTokenPositions(reflect.ValueOf(generated), make(map[uintptr]bool))

	var body bytes.Buffer
	if err := format.Node(&body, pkg.fset, generated); err != nil {
		return GenerateResult{}, fmt.Errorf("format generated AST: %w", err)
	}
	const header = `// Copyright 2026 PingCAP, Inc.
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
//
// Code generated by visitor_codegen. DO NOT EDIT.

`
	source, err := format.Source(append([]byte(header), body.Bytes()...))
	if err != nil {
		return GenerateResult{}, fmt.Errorf("format generated source: %w", err)
	}
	return GenerateResult{Source: source, Receivers: receivers}, nil
}

var tokenPositionType = reflect.TypeOf(token.Pos(0))

func clearTokenPositions(value reflect.Value, seen map[uintptr]bool) {
	if !value.IsValid() {
		return
	}
	if value.Type() == tokenPositionType {
		if value.CanSet() {
			value.SetInt(0)
		}
		return
	}
	switch value.Kind() {
	case reflect.Interface:
		if !value.IsNil() {
			clearTokenPositions(value.Elem(), seen)
		}
	case reflect.Pointer:
		if value.IsNil() {
			return
		}
		pointer := value.Pointer()
		if seen[pointer] {
			return
		}
		seen[pointer] = true
		clearTokenPositions(value.Elem(), seen)
	case reflect.Struct:
		typeInfo := value.Type()
		for i := range value.NumField() {
			// Parser object links are not needed for generation and may be cyclic.
			if typeInfo.Field(i).Name == "Obj" {
				continue
			}
			clearTokenPositions(value.Field(i), seen)
		}
	case reflect.Slice:
		for i := range value.Len() {
			clearTokenPositions(value.Index(i), seen)
		}
	}
}

func parsePackage(request GenerateRequest) (*parsedPackage, []string, error) {
	sourceFS := request.FS
	if sourceFS == nil {
		if request.SourceDir == "" {
			return nil, nil, errors.New("source directory is required")
		}
		sourceFS = os.DirFS(request.SourceDir)
	}
	entries, err := fs.ReadDir(sourceFS, ".")
	if err != nil {
		return nil, nil, fmt.Errorf("read source directory: %w", err)
	}
	allFiles := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "visitor_inplace_generated.go" {
			continue
		}
		allFiles = append(allFiles, name)
	}
	sort.Strings(allFiles)

	traversalFiles := append([]string(nil), request.TraversalFiles...)
	if len(traversalFiles) == 0 {
		traversalFiles = append([]string(nil), defaultTraversalFiles...)
	}
	traversalSet := make(map[string]struct{}, len(traversalFiles))
	for _, filename := range traversalFiles {
		traversalSet[filename] = struct{}{}
	}
	pkg := &parsedPackage{
		fset:      token.NewFileSet(),
		files:     make(map[string]*ast.File, len(allFiles)),
		types:     make(map[string]*ast.TypeSpec),
		methods:   make(map[string]map[string]string),
		receivers: make(map[string]struct{}),
	}
	for _, filename := range allFiles {
		contents, readErr := fs.ReadFile(sourceFS, filename)
		if readErr != nil {
			return nil, nil, fmt.Errorf("read %s: %w", filename, readErr)
		}
		file, parseErr := parser.ParseFile(pkg.fset, filename, contents, parser.ParseComments)
		if parseErr != nil {
			return nil, nil, fmt.Errorf("parse %s: %w", filename, parseErr)
		}
		if pkg.name == "" {
			pkg.name = file.Name.Name
		} else if file.Name.Name != pkg.name {
			return nil, nil, fmt.Errorf("%s: package %s does not match %s", filename, file.Name.Name, pkg.name)
		}
		pkg.files[filename] = file
		for _, decl := range file.Decls {
			if method, ok := decl.(*ast.FuncDecl); ok && method.Recv != nil {
				receiver, receiverOK := anyReceiverTypeName(method)
				if receiverOK && method.Type.Params != nil && len(method.Type.Params.List) == 1 {
					parameter, parameterOK := method.Type.Params.List[0].Type.(*ast.Ident)
					if parameterOK {
						if pkg.methods[receiver] == nil {
							pkg.methods[receiver] = make(map[string]string)
						}
						pkg.methods[receiver][method.Name.Name] = parameter.Name
					}
				}
			}
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				typeSpec := spec.(*ast.TypeSpec)
				pkg.types[typeSpec.Name.Name] = typeSpec
			}
		}
	}
	for _, filename := range allFiles {
		file := pkg.files[filename]
		if _, selected := traversalSet[filename]; selected {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !isAcceptMethod(fn) {
				continue
			}
			receiver, _ := receiverTypeName(fn)
			return nil, nil, fmt.Errorf("%s: Accept receiver %s at %s is outside traversal files", filename, receiver, pkg.fset.Position(fn.Pos()))
		}
	}
	for _, filename := range traversalFiles {
		file, ok := pkg.files[filename]
		if !ok {
			return nil, nil, fmt.Errorf("traversal file %s not found", filename)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !isAcceptMethod(fn) {
				continue
			}
			receiver, ok := receiverTypeName(fn)
			if !ok {
				return nil, nil, fmt.Errorf("%s: unsupported non-pointer Accept receiver at %s", filename, pkg.fset.Position(fn.Pos()))
			}
			if _, duplicate := pkg.receivers[receiver]; duplicate {
				return nil, nil, fmt.Errorf("%s: duplicate Accept receiver %s", filename, receiver)
			}
			pkg.receivers[receiver] = struct{}{}
		}
	}
	return pkg, traversalFiles, nil
}

func isAcceptMethod(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || fn.Name.Name != "Accept" || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 || fn.Type.Results == nil || len(fn.Type.Results.List) != 2 {
		return false
	}
	visitor, ok := fn.Type.Params.List[0].Type.(*ast.Ident)
	if !ok || visitor.Name != "Visitor" {
		return false
	}
	node, nodeOK := fn.Type.Results.List[0].Type.(*ast.Ident)
	boolean, boolOK := fn.Type.Results.List[1].Type.(*ast.Ident)
	return nodeOK && boolOK && node.Name == "Node" && boolean.Name == "bool"
}

func receiverTypeName(fn *ast.FuncDecl) (string, bool) {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return "", false
	}
	pointer, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return "", false
	}
	name, ok := pointer.X.(*ast.Ident)
	return name.Name, ok
}

func anyReceiverTypeName(fn *ast.FuncDecl) (string, bool) {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return "", false
	}
	receiver := fn.Recv.List[0].Type
	if pointer, ok := receiver.(*ast.StarExpr); ok {
		receiver = pointer.X
	}
	return namedType(receiver)
}

func transformAccept(pkg *parsedPackage, filename string, fn *ast.FuncDecl) (*ast.FuncDecl, error) {
	receiverName, _ := receiverTypeName(fn)
	receiverField := fn.Recv.List[0]
	if len(receiverField.Names) != 1 || len(fn.Type.Params.List[0].Names) != 1 {
		return nil, fmt.Errorf("%s: %s: unnamed receiver or visitor at %s", filename, receiverName, pkg.fset.Position(fn.Pos()))
	}
	receiverVar := receiverField.Names[0].Name
	visitorVar := fn.Type.Params.List[0].Names[0].Name
	transformer := &methodTransformer{
		pkg:          pkg,
		filename:     filename,
		receiverName: receiverName,
		receiverVar:  receiverVar,
		visitorVar:   visitorVar,
		locals: map[string]localBinding{
			receiverVar: {typ: &ast.StarExpr{X: ast.NewIdent(receiverName)}},
		},
	}
	standardLeaf := isStandardLeafAccept(fn)
	body, err := transformer.transformBlock(fn.Body)
	if err != nil {
		return nil, err
	}
	if err := transformer.validateTransformedBody(body); err != nil {
		return nil, err
	}
	// A leaf calls Leave for either Enter result, so direct traversal does not need the skip branch.
	if standardLeaf {
		body = &ast.BlockStmt{List: []ast.Stmt{
			&ast.ExprStmt{X: &ast.CallExpr{
				Fun:  &ast.SelectorExpr{X: ast.NewIdent(visitorVar), Sel: ast.NewIdent("Enter")},
				Args: []ast.Expr{ast.NewIdent(receiverVar)},
			}},
			&ast.ReturnStmt{Results: []ast.Expr{&ast.CallExpr{
				Fun:  &ast.SelectorExpr{X: ast.NewIdent(visitorVar), Sel: ast.NewIdent("Leave")},
				Args: []ast.Expr{ast.NewIdent(receiverVar)},
			}}},
		}}
	}
	return &ast.FuncDecl{
		Recv: fn.Recv,
		Name: ast.NewIdent("AcceptInPlace"),
		Type: &ast.FuncType{
			Params:  &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent(visitorVar)}, Type: ast.NewIdent("InPlaceVisitor")}}},
			Results: &ast.FieldList{List: []*ast.Field{{Type: ast.NewIdent("bool")}}},
		},
		Body: body,
	}, nil
}

func (t *methodTransformer) transformBlock(block *ast.BlockStmt) (*ast.BlockStmt, error) {
	if block == nil {
		return nil, nil
	}
	statements := block.List
	result := make([]ast.Stmt, 0, len(statements))
	for i := 0; i < len(statements); i++ {
		statement := statements[i]
		enter, isEnter, err := t.enterAssignment(statement)
		if err != nil {
			return nil, err
		}
		if isEnter {
			if t.enterResult != "" {
				return nil, t.errorAt(statement.Pos(), "multiple Visitor.Enter assignments")
			}
			t.enterResult = enter.nodeName
			if enter.skipName != "" && i+1 < len(statements) {
				if check, ok := statements[i+1].(*ast.IfStmt); ok && check.Init == nil && isIdent(check.Cond, enter.skipName) {
					if check.Else != nil {
						return nil, t.errorAt(check.Else.Pos(), "Enter skip check has an unsupported else branch")
					}
					body, err := t.transformBlock(check.Body)
					if err != nil {
						return nil, err
					}
					result = append(result, &ast.IfStmt{
						Init: &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(enter.skipName)}, Tok: token.DEFINE, Rhs: []ast.Expr{enter.call}},
						Cond: ast.NewIdent(enter.skipName),
						Body: body,
					})
					i++
					continue
				}
			}
			result = append(result, &ast.ExprStmt{X: enter.call})
			continue
		}
		rebind, err := t.isReceiverRebind(statement)
		if err != nil {
			return nil, err
		}
		if rebind || isReplaceModeDeclaration(statement) {
			continue
		}
		if t.isReplacementTempCommit(statement) {
			continue
		}
		if isReplacementGuard(statement) {
			if err := t.validateReplacementGuard(statement.(*ast.IfStmt)); err != nil {
				return nil, err
			}
			continue
		}
		if child, ok := childAssignment(statement, t.visitorVar); ok {
			if i+1 >= len(statements) {
				return nil, t.errorAt(child.call.Pos(), "child call %s.Accept has no boolean check", expressionString(child.receiver))
			}
			check, ok := statements[i+1].(*ast.IfStmt)
			if !ok || check.Init != nil {
				return nil, t.errorAt(child.call.Pos(), "child call %s.Accept is not followed by a boolean check", expressionString(child.receiver))
			}
			if check.Else != nil {
				return nil, t.errorAt(check.Else.Pos(), "child check for %s.Accept has an unsupported else branch", expressionString(child.receiver))
			}
			positive, ok := boolCondition(check.Cond, child.okName)
			if !ok {
				return nil, t.errorAt(check.Cond.Pos(), "child call %s.Accept has an unknown boolean check", expressionString(child.receiver))
			}
			call, err := t.inPlaceCall(child.receiver)
			if err != nil {
				return nil, err
			}
			checkBody, err := t.transformBlock(check.Body)
			if err != nil {
				return nil, err
			}
			condition := ast.Expr(call)
			if !positive {
				condition = &ast.UnaryExpr{Op: token.NOT, X: call}
			}
			result = append(result, &ast.IfStmt{Cond: condition, Body: checkBody})
			i++
			if i+1 < len(statements) && !isReplacementGuard(statements[i+1]) {
				writeback, writebackErr := t.validateDirectChildWriteback(statements[i+1], child)
				if writebackErr != nil {
					return nil, writebackErr
				}
				if writeback {
					i++
				}
			}
			if err := t.ensureNoDelayedChildResultUse(statements[i+1:], child); err != nil {
				return nil, err
			}
			continue
		}

		transformed, err := t.transformStatement(statement)
		if err != nil {
			return nil, err
		}
		if transformed != nil {
			result = append(result, transformed)
			t.recordBindings(transformed)
		}
	}
	result = removeUnusedMakeAssignments(result)
	return &ast.BlockStmt{List: result}, nil
}

type enterInfo struct {
	call     *ast.CallExpr
	nodeName string
	skipName string
}

func (t *methodTransformer) enterAssignment(statement ast.Stmt) (enterInfo, bool, error) {
	assignment, ok := statement.(*ast.AssignStmt)
	if !ok || len(assignment.Rhs) != 1 {
		return enterInfo{}, false, nil
	}
	call, ok := assignment.Rhs[0].(*ast.CallExpr)
	if !ok || !isMethodCall(call, t.visitorVar, "Enter") {
		return enterInfo{}, false, nil
	}
	if len(assignment.Lhs) != 2 || len(call.Args) != 1 || !isIdent(call.Args[0], t.receiverVar) {
		return enterInfo{}, false, t.errorAt(call.Pos(), "Visitor.Enter must bind two results from receiver %s", t.receiverVar)
	}
	node, ok := assignment.Lhs[0].(*ast.Ident)
	if !ok {
		return enterInfo{}, false, t.errorAt(assignment.Lhs[0].Pos(), "Visitor.Enter node result must be an identifier")
	}
	skip, ok := assignment.Lhs[1].(*ast.Ident)
	if !ok {
		return enterInfo{}, false, t.errorAt(assignment.Lhs[1].Pos(), "Visitor.Enter skip result must be an identifier")
	}
	name := skip.Name
	if name == "_" {
		name = ""
	}
	return enterInfo{call: call, nodeName: node.Name, skipName: name}, true, nil
}

func (t *methodTransformer) isReceiverRebind(statement ast.Stmt) (bool, error) {
	assignment, ok := statement.(*ast.AssignStmt)
	if !ok || len(assignment.Lhs) != 1 || !isIdent(assignment.Lhs[0], t.receiverVar) || len(assignment.Rhs) != 1 {
		return false, nil
	}
	assertion, ok := assignment.Rhs[0].(*ast.TypeAssertExpr)
	if !ok {
		return false, nil
	}
	pointer, ok := assertion.Type.(*ast.StarExpr)
	if !ok || !isIdent(pointer.X, t.receiverName) {
		return false, nil
	}
	if t.enterResult == "" || !isIdent(assertion.X, t.enterResult) {
		return false, t.errorAt(statement.Pos(), "receiver rebind must assert the Visitor.Enter result %s", t.enterResult)
	}
	return true, nil
}

func (t *methodTransformer) transformStatement(statement ast.Stmt) (ast.Stmt, error) {
	switch stmt := statement.(type) {
	case *ast.ReturnStmt:
		return t.transformReturn(stmt), nil
	case *ast.IfStmt:
		if child, ok := childAssignmentFromIf(stmt, t.visitorVar); ok {
			if stmt.Else != nil {
				return nil, t.errorAt(stmt.Else.Pos(), "inline child check for %s.Accept has an unsupported else branch", expressionString(child.receiver))
			}
			positive, ok := boolCondition(stmt.Cond, child.okName)
			if !ok {
				return nil, t.errorAt(stmt.Cond.Pos(), "inline child call %s.Accept has an unknown boolean check", expressionString(child.receiver))
			}
			call, err := t.inPlaceCall(child.receiver)
			if err != nil {
				return nil, err
			}
			body, err := t.transformBlock(stmt.Body)
			if err != nil {
				return nil, err
			}
			condition := ast.Expr(call)
			if !positive {
				condition = &ast.UnaryExpr{Op: token.NOT, X: call}
			}
			return &ast.IfStmt{Cond: condition, Body: body}, nil
		}
		body, err := t.withScope(func() (*ast.BlockStmt, error) { return t.transformBlock(stmt.Body) })
		if err != nil {
			return nil, err
		}
		var elseStmt ast.Stmt
		if stmt.Else != nil {
			elseStmt, err = t.transformStatement(stmt.Else)
			if err != nil {
				return nil, err
			}
		}
		stmt.Body = body
		stmt.Else = elseStmt
		if err := t.rewriteHelperCalls(stmt.Cond); err != nil {
			return nil, err
		}
		return stmt, nil
	case *ast.RangeStmt:
		return t.transformRange(stmt)
	case *ast.BlockStmt:
		return t.withScope(func() (*ast.BlockStmt, error) { return t.transformBlock(stmt) })
	case *ast.AssignStmt:
		if err := t.rewriteHelperCalls(stmt); err != nil {
			return nil, err
		}
		return stmt, nil
	case *ast.ExprStmt:
		if err := t.rewriteHelperCalls(stmt); err != nil {
			return nil, err
		}
		return stmt, nil
	default:
		if err := t.rewriteHelperCalls(stmt); err != nil {
			return nil, err
		}
		return stmt, nil
	}
}

func (t *methodTransformer) transformReturn(stmt *ast.ReturnStmt) ast.Stmt {
	if len(stmt.Results) == 2 {
		_, inertNodeResult := stmt.Results[0].(*ast.Ident)
		if boolean, ok := stmt.Results[1].(*ast.Ident); inertNodeResult && ok && (boolean.Name == "false" || boolean.Name == "true") {
			return &ast.ReturnStmt{Results: []ast.Expr{ast.NewIdent(boolean.Name)}}
		}
	}
	if len(stmt.Results) == 1 {
		if call, ok := stmt.Results[0].(*ast.CallExpr); ok && isMethodCall(call, t.visitorVar, "Leave") {
			call.Args = []ast.Expr{ast.NewIdent(t.receiverVar)}
			return stmt
		}
	}
	return stmt
}

func (t *methodTransformer) transformRange(stmt *ast.RangeStmt) (ast.Stmt, error) {
	rangeType, _, err := t.resolveExprType(stmt.X)
	if err != nil {
		return nil, err
	}
	elementType, ok := t.rangeElementType(rangeType)
	if !ok {
		return nil, t.errorAt(stmt.X.Pos(), "cannot determine range element type for %s", expressionString(stmt.X))
	}
	oldLocals := t.locals
	t.locals = cloneLocals(oldLocals)
	defer func() { t.locals = oldLocals }()

	keyName := identName(stmt.Key)
	valueName := identName(stmt.Value)
	if keyName != "" && keyName != "_" {
		t.locals[keyName] = localBinding{typ: ast.NewIdent("int")}
	}
	if valueName != "" && valueName != "_" {
		binding := localBinding{typ: elementType, rangeCopy: true}
		if keyName != "" && keyName != "_" {
			binding.storage = &ast.IndexExpr{X: stmt.X, Index: ast.NewIdent(keyName)}
		}
		t.locals[valueName] = binding
	}
	body, err := t.transformBlock(stmt.Body)
	if err != nil {
		return nil, err
	}
	stmt.Body = body
	if keyName != "" && keyName != "_" && !identifierUsed(body, keyName) {
		if valueName != "" && valueName != "_" && identifierUsed(body, valueName) {
			stmt.Key = ast.NewIdent("_")
		} else {
			stmt.Key = nil
			stmt.Value = nil
			stmt.Tok = token.ILLEGAL
		}
	}
	if valueName != "" && valueName != "_" && !identifierUsed(body, valueName) {
		stmt.Value = nil
		if stmt.Key != nil {
			stmt.Tok = token.DEFINE
		}
	}
	return stmt, nil
}

func (t *methodTransformer) inPlaceCall(receiver ast.Expr) (*ast.CallExpr, error) {
	typeExpr, addressable, err := t.resolveExprType(receiver)
	if err != nil {
		return nil, t.errorAt(receiver.Pos(), "cannot classify %s.Accept: %v", expressionString(receiver), err)
	}
	resolved := t.classifyType(typeExpr)
	switch typ := resolved.(type) {
	case *ast.StarExpr:
		name, ok := namedType(typ.X)
		if !ok {
			return nil, t.errorAt(receiver.Pos(), "cannot classify %s.Accept pointer type %s", expressionString(receiver), expressionString(typeExpr))
		}
		if _, ok := t.pkg.receivers[name]; !ok {
			return nil, t.errorAt(receiver.Pos(), "unknown concrete Accept receiver %s for %s.Accept", name, expressionString(receiver))
		}
		return &ast.CallExpr{Fun: &ast.SelectorExpr{X: receiver, Sel: ast.NewIdent("AcceptInPlace")}, Args: []ast.Expr{ast.NewIdent(t.visitorVar)}}, nil
	case *ast.InterfaceType:
		if !t.isNodeInterface(typeExpr, make(map[string]bool)) {
			return nil, t.errorAt(receiver.Pos(), "cannot classify non-Node interface %s.Accept", expressionString(receiver))
		}
		return &ast.CallExpr{Fun: &ast.SelectorExpr{X: receiver, Sel: ast.NewIdent("AcceptInPlace")}, Args: []ast.Expr{ast.NewIdent(t.visitorVar)}}, nil
	case *ast.Ident:
		if _, ok := t.pkg.receivers[typ.Name]; !ok {
			return nil, t.errorAt(receiver.Pos(), "unknown concrete Accept receiver %s for %s.Accept", typ.Name, expressionString(receiver))
		}
		binding := receiver
		if ident, ok := receiver.(*ast.Ident); ok {
			local := t.locals[ident.Name]
			if local.rangeCopy && local.storage == nil {
				return nil, t.errorAt(receiver.Pos(), "concrete range value %s.Accept has no original storage", expressionString(receiver))
			}
			if local.storage != nil {
				binding = t.originalStorageExpr(local.storage)
				addressable = true
			}
		}
		if !addressable {
			return nil, t.errorAt(receiver.Pos(), "concrete value %s.Accept is not addressable", expressionString(receiver))
		}
		address := &ast.UnaryExpr{Op: token.AND, X: binding}
		return &ast.CallExpr{Fun: &ast.SelectorExpr{X: &ast.ParenExpr{X: address}, Sel: ast.NewIdent("AcceptInPlace")}, Args: []ast.Expr{ast.NewIdent(t.visitorVar)}}, nil
	default:
		return nil, t.errorAt(receiver.Pos(), "unknown type %s for %s.Accept", expressionString(typeExpr), expressionString(receiver))
	}
}

func (t *methodTransformer) isNodeInterface(expr ast.Expr, seen map[string]bool) bool {
	if ident, ok := expr.(*ast.Ident); ok {
		if ident.Name == "Node" {
			return true
		}
		if seen[ident.Name] {
			return false
		}
		seen[ident.Name] = true
		typeSpec := t.pkg.types[ident.Name]
		if typeSpec == nil {
			return false
		}
		expr = typeSpec.Type
	}
	interfaceType, ok := expr.(*ast.InterfaceType)
	if !ok {
		return false
	}
	for _, field := range interfaceType.Methods.List {
		if len(field.Names) == 0 && t.isNodeInterface(field.Type, cloneSeen(seen)) {
			return true
		}
	}
	return false
}

func (t *methodTransformer) classifyType(expr ast.Expr) ast.Expr {
	switch typ := expr.(type) {
	case *ast.StarExpr:
		return typ
	case *ast.InterfaceType:
		return typ
	case *ast.Ident:
		if _, ok := t.pkg.receivers[typ.Name]; ok {
			return typ
		}
		typeSpec := t.pkg.types[typ.Name]
		if typeSpec == nil {
			return typ
		}
		if _, ok := typeSpec.Type.(*ast.InterfaceType); ok {
			return typeSpec.Type
		}
		if alias, ok := typeSpec.Type.(*ast.Ident); ok {
			return t.classifyType(alias)
		}
		return typ
	default:
		return expr
	}
}

func (t *methodTransformer) resolveExprType(expr ast.Expr) (ast.Expr, bool, error) {
	switch value := expr.(type) {
	case *ast.Ident:
		binding, ok := t.locals[value.Name]
		if !ok {
			return nil, false, fmt.Errorf("unknown local %s", value.Name)
		}
		return binding.typ, true, nil
	case *ast.SelectorExpr:
		baseType, baseAddressable, err := t.resolveExprType(value.X)
		if err != nil {
			return nil, false, err
		}
		fieldType, err := t.fieldType(baseType, value.Sel.Name, make(map[string]bool))
		if err != nil {
			return nil, false, err
		}
		_, pointerBase := t.resolveNamed(baseType).(*ast.StarExpr)
		return fieldType, baseAddressable || pointerBase, nil
	case *ast.IndexExpr:
		container, addressable, err := t.resolveExprType(value.X)
		if err != nil {
			return nil, false, err
		}
		element, ok := t.rangeElementType(container)
		if !ok {
			return nil, false, fmt.Errorf("cannot index %s", expressionString(container))
		}
		return element, addressable, nil
	case *ast.ParenExpr:
		return t.resolveExprType(value.X)
	case *ast.StarExpr:
		base, _, err := t.resolveExprType(value.X)
		if err != nil {
			return nil, false, err
		}
		pointer, ok := t.resolveNamed(base).(*ast.StarExpr)
		if !ok {
			return nil, false, fmt.Errorf("cannot dereference %s", expressionString(base))
		}
		return pointer.X, true, nil
	case *ast.SliceExpr:
		base, addressable, err := t.resolveExprType(value.X)
		return base, addressable, err
	default:
		return nil, false, fmt.Errorf("unsupported expression %T", expr)
	}
}

func (t *methodTransformer) fieldType(base ast.Expr, fieldName string, seen map[string]bool) (ast.Expr, error) {
	base = t.resolveNamed(base)
	if pointer, ok := base.(*ast.StarExpr); ok {
		base = t.resolveNamed(pointer.X)
	}
	name, ok := namedType(base)
	if ok {
		if seen[name] {
			return nil, fmt.Errorf("embedded field cycle through %s", name)
		}
		seen[name] = true
		typeSpec := t.pkg.types[name]
		if typeSpec == nil {
			return nil, fmt.Errorf("unknown local type %s", name)
		}
		base = typeSpec.Type
	}
	structure, ok := base.(*ast.StructType)
	if !ok {
		return nil, fmt.Errorf("%s is not a struct", expressionString(base))
	}
	for _, field := range structure.Fields.List {
		for _, name := range field.Names {
			if name.Name == fieldName {
				return field.Type, nil
			}
		}
	}
	for _, field := range structure.Fields.List {
		if len(field.Names) != 0 {
			continue
		}
		if embeddedName(field.Type) == fieldName {
			return field.Type, nil
		}
		if nested, err := t.fieldType(field.Type, fieldName, cloneSeen(seen)); err == nil {
			return nested, nil
		}
	}
	return nil, fmt.Errorf("field %s not found in %s", fieldName, expressionString(base))
}

func (t *methodTransformer) resolveNamed(expr ast.Expr) ast.Expr {
	seen := make(map[string]bool)
	for {
		ident, ok := expr.(*ast.Ident)
		if !ok || seen[ident.Name] {
			return expr
		}
		seen[ident.Name] = true
		typeSpec := t.pkg.types[ident.Name]
		if typeSpec == nil {
			return expr
		}
		expr = typeSpec.Type
	}
}

func (t *methodTransformer) rangeElementType(expr ast.Expr) (ast.Expr, bool) {
	expr = t.resolveNamed(expr)
	switch typ := expr.(type) {
	case *ast.ArrayType:
		return typ.Elt, true
	case *ast.MapType:
		return typ.Value, true
	default:
		return nil, false
	}
}

func (t *methodTransformer) recordBindings(statement ast.Stmt) {
	assignment, ok := statement.(*ast.AssignStmt)
	if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) != len(assignment.Rhs) {
		return
	}
	for i, lhs := range assignment.Lhs {
		name := identName(lhs)
		if name == "" || name == "_" {
			continue
		}
		typeExpr, _, err := t.resolveExprType(assignment.Rhs[i])
		binding := localBinding{typ: typeExpr}
		if call, ok := assignment.Rhs[i].(*ast.CallExpr); ok && isIdent(call.Fun, "make") && len(call.Args) > 0 {
			typeExpr = call.Args[0]
			binding.typ = typeExpr
			if len(call.Args) > 1 {
				if length, ok := call.Args[1].(*ast.CallExpr); ok && isIdent(length.Fun, "len") && len(length.Args) == 1 {
					binding.replacementOf = length.Args[0]
				}
			}
			err = nil
		}
		if err == nil {
			binding.typ = typeExpr
			t.locals[name] = binding
		}
	}
}

func (t *methodTransformer) validateReplacementGuard(statement *ast.IfStmt) error {
	if statement.Else != nil || len(statement.Body.List) == 0 {
		return t.errorAt(statement.Pos(), "replacement guard must contain only writeback assignments")
	}
	for _, guarded := range statement.Body.List {
		if t.isReplacementTempCommit(guarded) {
			continue
		}
		assignment, ok := guarded.(*ast.AssignStmt)
		if !ok || len(assignment.Rhs) != 1 || (len(assignment.Lhs) != 1 && (len(assignment.Lhs) != 2 || !isIdent(assignment.Lhs[1], "_"))) {
			return t.errorAt(guarded.Pos(), "replacement guard contains unsupported statement %T", guarded)
		}
		target := assignment.Lhs[0]
		leftType, _, err := t.resolveExprType(target)
		if err != nil {
			return t.errorAt(assignment.Pos(), "cannot validate replacement target %s: %v", expressionString(target), err)
		}
		if _, bareResult := assignment.Rhs[0].(*ast.Ident); len(assignment.Lhs) == 1 && bareResult && isIdent(leftType, "Node") {
			continue
		}
		assertedType, dereferenced, ok := assertedResultType(assignment.Rhs[0])
		if !ok {
			return t.errorAt(assignment.Pos(), "replacement guard writeback for %s must use a direct type assertion", expressionString(target))
		}
		if dereferenced {
			pointer, pointerOK := assertedType.(*ast.StarExpr)
			if !pointerOK {
				return t.errorAt(assignment.Pos(), "replacement for %s dereferences non-pointer assertion %s", expressionString(target), expressionString(assertedType))
			}
			assertedType = pointer.X
		}
		if !sameType(leftType, assertedType) {
			return t.errorAt(assignment.Pos(), "replacement assertion %s does not match static target type %s for %s", expressionString(assertedType), expressionString(leftType), expressionString(target))
		}
	}
	return nil
}

func assertedResultType(expr ast.Expr) (ast.Expr, bool, bool) {
	dereferenced := false
	if star, ok := expr.(*ast.StarExpr); ok {
		dereferenced = true
		expr = star.X
	}
	assertion, ok := expr.(*ast.TypeAssertExpr)
	if !ok || assertion.Type == nil {
		return nil, false, false
	}
	return assertion.Type, dereferenced, true
}

func sameType(left, right ast.Expr) bool {
	return expressionString(left) == expressionString(right)
}

func sameExpression(left, right ast.Expr) bool {
	return expressionString(left) == expressionString(right)
}

func (t *methodTransformer) validateTransformedBody(body *ast.BlockStmt) error {
	var validationErr error
	ast.Inspect(body, func(node ast.Node) bool {
		if validationErr != nil {
			return false
		}
		switch current := node.(type) {
		case *ast.CallExpr:
			if isAcceptCall(current, t.visitorVar) {
				validationErr = t.errorAt(current.Pos(), "unsupported traversal grammar retains %s", expressionString(current.Fun))
				return false
			}
			if ident, ok := current.Fun.(*ast.Ident); ok && ident.Name == "shouldReplaceNode" {
				validationErr = t.errorAt(current.Pos(), "unsupported traversal grammar retains shouldReplaceNode")
				return false
			}
		case *ast.ReturnStmt:
			if len(current.Results) != 1 {
				validationErr = t.errorAt(current.Pos(), "unsupported traversal return has %d results", len(current.Results))
				return false
			}
		}
		return true
	})
	return validationErr
}

func (t *methodTransformer) withScope(fn func() (*ast.BlockStmt, error)) (*ast.BlockStmt, error) {
	oldLocals := t.locals
	t.locals = cloneLocals(oldLocals)
	defer func() { t.locals = oldLocals }()
	return fn()
}

func (t *methodTransformer) errorAt(pos token.Pos, formatString string, args ...any) error {
	return fmt.Errorf("%s: %s: %s: %s", t.filename, t.receiverName, t.pkg.fset.Position(pos), fmt.Sprintf(formatString, args...))
}

type childCall struct {
	call     *ast.CallExpr
	receiver ast.Expr
	nodeName string
	okName   string
}

func isStandardLeafAccept(method *ast.FuncDecl) bool {
	if method == nil || method.Recv == nil || len(method.Recv.List) != 1 || len(method.Recv.List[0].Names) != 1 ||
		method.Type.Params == nil || len(method.Type.Params.List) != 1 || len(method.Type.Params.List[0].Names) != 1 ||
		method.Body == nil || len(method.Body.List) != 4 {
		return false
	}
	receiverName := method.Recv.List[0].Names[0].Name
	visitorName := method.Type.Params.List[0].Names[0].Name

	enter, ok := method.Body.List[0].(*ast.AssignStmt)
	if !ok || enter.Tok != token.DEFINE || len(enter.Lhs) != 2 || len(enter.Rhs) != 1 {
		return false
	}
	enteredName := identName(enter.Lhs[0])
	skipName := identName(enter.Lhs[1])
	enterCall, ok := enter.Rhs[0].(*ast.CallExpr)
	if !ok || enteredName == "" || skipName == "" || !isMethodCall(enterCall, visitorName, "Enter") ||
		len(enterCall.Args) != 1 || !isIdent(enterCall.Args[0], receiverName) {
		return false
	}

	skip, ok := method.Body.List[1].(*ast.IfStmt)
	if !ok || skip.Init != nil || skip.Else != nil || !isIdent(skip.Cond, skipName) || len(skip.Body.List) != 1 {
		return false
	}
	skipReturn, ok := skip.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(skipReturn.Results) != 1 {
		return false
	}
	skipLeave, ok := skipReturn.Results[0].(*ast.CallExpr)
	if !ok || !isMethodCall(skipLeave, visitorName, "Leave") || len(skipLeave.Args) != 1 || !isIdent(skipLeave.Args[0], enteredName) {
		return false
	}

	rebind, ok := method.Body.List[2].(*ast.AssignStmt)
	if !ok || rebind.Tok != token.ASSIGN || len(rebind.Lhs) != 1 || len(rebind.Rhs) != 1 || !isIdent(rebind.Lhs[0], receiverName) {
		return false
	}
	assertion, ok := rebind.Rhs[0].(*ast.TypeAssertExpr)
	if !ok || !isIdent(assertion.X, enteredName) {
		return false
	}
	pointer, ok := assertion.Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	receiverType, ok := anyReceiverTypeName(method)
	if !ok || !isIdent(pointer.X, receiverType) {
		return false
	}

	leave, ok := method.Body.List[3].(*ast.ReturnStmt)
	if !ok || len(leave.Results) != 1 {
		return false
	}
	leaveCall, ok := leave.Results[0].(*ast.CallExpr)
	return ok && isMethodCall(leaveCall, visitorName, "Leave") && len(leaveCall.Args) == 1 && isIdent(leaveCall.Args[0], receiverName)
}

func childAssignment(statement ast.Stmt, visitor string) (childCall, bool) {
	assignment, ok := statement.(*ast.AssignStmt)
	if !ok || len(assignment.Lhs) != 2 || len(assignment.Rhs) != 1 {
		return childCall{}, false
	}
	call, ok := assignment.Rhs[0].(*ast.CallExpr)
	if !ok || !isAcceptCall(call, visitor) {
		return childCall{}, false
	}
	selector := call.Fun.(*ast.SelectorExpr)
	return childCall{
		call:     call,
		receiver: selector.X,
		nodeName: identName(assignment.Lhs[0]),
		okName:   identName(assignment.Lhs[1]),
	}, true
}

func childAssignmentFromIf(stmt *ast.IfStmt, visitor string) (childCall, bool) {
	if stmt.Init == nil {
		return childCall{}, false
	}
	return childAssignment(stmt.Init, visitor)
}

func isAcceptCall(call *ast.CallExpr, visitor string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "Accept" && len(call.Args) == 1 && isIdent(call.Args[0], visitor)
}

func isMethodCall(call *ast.CallExpr, receiver, method string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && isIdent(selector.X, receiver) && selector.Sel.Name == method
}

func boolCondition(expr ast.Expr, name string) (bool, bool) {
	if isIdent(expr, name) {
		return true, true
	}
	if unary, ok := expr.(*ast.UnaryExpr); ok && unary.Op == token.NOT && isIdent(unary.X, name) {
		return false, true
	}
	return false, false
}

func isReplaceModeDeclaration(statement ast.Stmt) bool {
	assignment, ok := statement.(*ast.AssignStmt)
	if !ok || len(assignment.Rhs) != 1 {
		return false
	}
	call, ok := assignment.Rhs[0].(*ast.CallExpr)
	if !ok {
		return false
	}
	ident, ok := call.Fun.(*ast.Ident)
	return ok && ident.Name == "shouldReplaceNode"
}

func isReplacementGuard(statement ast.Stmt) bool {
	ifStatement, ok := statement.(*ast.IfStmt)
	return ok && ifStatement.Init == nil && isIdent(ifStatement.Cond, "replaceNode")
}

func (t *methodTransformer) isReplacementTempCommit(statement ast.Stmt) bool {
	assignment, ok := statement.(*ast.AssignStmt)
	if !ok || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
		return false
	}
	temporary, ok := assignment.Rhs[0].(*ast.Ident)
	if !ok {
		return false
	}
	replacementOf := t.locals[temporary.Name].replacementOf
	return replacementOf != nil && sameExpression(assignment.Lhs[0], replacementOf)
}

func (t *methodTransformer) originalStorageExpr(expr ast.Expr) ast.Expr {
	switch value := expr.(type) {
	case *ast.Ident:
		if storage := t.locals[value.Name].storage; storage != nil {
			return t.originalStorageExpr(storage)
		}
		return ast.NewIdent(value.Name)
	case *ast.SelectorExpr:
		return &ast.SelectorExpr{X: t.originalStorageExpr(value.X), Sel: ast.NewIdent(value.Sel.Name)}
	case *ast.IndexExpr:
		return &ast.IndexExpr{X: t.originalStorageExpr(value.X), Index: t.originalStorageExpr(value.Index)}
	case *ast.ParenExpr:
		return &ast.ParenExpr{X: t.originalStorageExpr(value.X)}
	case *ast.StarExpr:
		return &ast.StarExpr{X: t.originalStorageExpr(value.X)}
	case *ast.SliceExpr:
		result := &ast.SliceExpr{X: t.originalStorageExpr(value.X), Slice3: value.Slice3}
		if value.Low != nil {
			result.Low = t.originalStorageExpr(value.Low)
		}
		if value.High != nil {
			result.High = t.originalStorageExpr(value.High)
		}
		if value.Max != nil {
			result.Max = t.originalStorageExpr(value.Max)
		}
		return result
	default:
		return expr
	}
}

func (t *methodTransformer) validateDirectChildWriteback(statement ast.Stmt, child childCall) (bool, error) {
	if child.nodeName == "" || child.nodeName == "_" || !identifierUsed(statement, child.nodeName) {
		return false, nil
	}
	assignment, ok := statement.(*ast.AssignStmt)
	if !ok || len(assignment.Rhs) != 1 || (len(assignment.Lhs) != 1 && (len(assignment.Lhs) != 2 || !isIdent(assignment.Lhs[1], "_"))) {
		return false, t.errorAt(statement.Pos(), "writeback for %s.Accept must be one assignment", expressionString(child.receiver))
	}
	target := assignment.Lhs[0]
	leftType, _, err := t.resolveExprType(target)
	if err != nil {
		return false, t.errorAt(statement.Pos(), "cannot resolve writeback target %s: %v", expressionString(target), err)
	}
	if isIdent(assignment.Rhs[0], child.nodeName) {
		if len(assignment.Lhs) != 1 {
			return false, t.errorAt(statement.Pos(), "bare writeback for %s.Accept cannot have a second result", expressionString(child.receiver))
		}
		if !isIdent(leftType, "Node") {
			return false, t.errorAt(statement.Pos(), "bare writeback for %s.Accept requires static Node target, got %s", expressionString(child.receiver), expressionString(leftType))
		}
	} else {
		assertedType, dereferenced, ok := assertedResultType(assignment.Rhs[0])
		if !ok {
			return false, t.errorAt(statement.Pos(), "writeback for %s.Accept must use a direct type assertion", expressionString(child.receiver))
		}
		assertionExpr := assignment.Rhs[0]
		if star, ok := assertionExpr.(*ast.StarExpr); ok {
			assertionExpr = star.X
		}
		assertion := assertionExpr.(*ast.TypeAssertExpr)
		if !isIdent(assertion.X, child.nodeName) {
			return false, t.errorAt(statement.Pos(), "writeback for %s.Accept must assert the direct child result %s", expressionString(child.receiver), child.nodeName)
		}
		if dereferenced {
			pointer, pointerOK := assertedType.(*ast.StarExpr)
			if !pointerOK {
				return false, t.errorAt(statement.Pos(), "writeback for %s dereferences non-pointer assertion %s", expressionString(target), expressionString(assertedType))
			}
			assertedType = pointer.X
		}
		if !sameType(leftType, assertedType) {
			return false, t.errorAt(statement.Pos(), "writeback assertion %s does not match static target type %s", expressionString(assertedType), expressionString(leftType))
		}
	}
	expectedStorage := t.originalStorageExpr(child.receiver)
	if !sameExpression(target, expectedStorage) && !t.isReplacementTempTarget(target, expectedStorage) {
		return false, t.errorAt(statement.Pos(), "writeback target %s does not match original child storage %s", expressionString(target), expressionString(expectedStorage))
	}
	return true, nil
}

func (t *methodTransformer) isReplacementTempTarget(target, expected ast.Expr) bool {
	targetIndex, targetOK := target.(*ast.IndexExpr)
	expectedIndex, expectedOK := expected.(*ast.IndexExpr)
	if !targetOK || !expectedOK || !sameExpression(targetIndex.Index, expectedIndex.Index) {
		return false
	}
	temporary, ok := targetIndex.X.(*ast.Ident)
	if !ok {
		return false
	}
	replacementOf := t.locals[temporary.Name].replacementOf
	return replacementOf != nil && sameExpression(replacementOf, expectedIndex.X)
}

func (t *methodTransformer) ensureNoDelayedChildResultUse(statements []ast.Stmt, child childCall) error {
	tracked := make(map[string]struct{}, 2)
	for _, name := range []string{child.nodeName, child.okName} {
		if name != "" && name != "_" {
			tracked[name] = struct{}{}
		}
	}
	for _, statement := range statements {
		if len(tracked) == 0 {
			return nil
		}
		if isReplacementGuard(statement) || isReplaceModeDeclaration(statement) {
			continue
		}
		if nextChild, ok := childAssignment(statement, t.visitorVar); ok {
			for name := range tracked {
				if identifierUsed(nextChild.receiver, name) {
					return t.errorAt(statement.Pos(), "delayed child result %s is used by a later traversal", name)
				}
			}
			delete(tracked, nextChild.nodeName)
			delete(tracked, nextChild.okName)
			continue
		}
		for name := range tracked {
			if t.identifierUsedBeforeChildOverwrite(statement, name) {
				return t.errorAt(statement.Pos(), "delayed child result %s is used outside its boolean check or writeback", name)
			}
		}
	}
	return nil
}

func (t *methodTransformer) identifierUsedBeforeChildOverwrite(statement ast.Stmt, name string) bool {
	switch current := statement.(type) {
	case *ast.RangeStmt:
		if identifierUsed(current.X, name) {
			return true
		}
		if current.Tok == token.DEFINE && (identName(current.Key) == name || identName(current.Value) == name) {
			return false
		}
		return t.identifierUsedInBlockBeforeChildOverwrite(current.Body, name)
	case *ast.IfStmt:
		if current.Init != nil {
			if child, ok := childAssignment(current.Init, t.visitorVar); ok {
				if identifierUsed(child.receiver, name) {
					return true
				}
				if child.nodeName == name || child.okName == name {
					return false
				}
			} else if identifierUsed(current.Init, name) {
				return true
			}
		}
		if identifierUsed(current.Cond, name) || t.identifierUsedInBlockBeforeChildOverwrite(current.Body, name) {
			return true
		}
		if current.Else != nil {
			return t.identifierUsedBeforeChildOverwrite(current.Else, name)
		}
		return false
	case *ast.BlockStmt:
		return t.identifierUsedInBlockBeforeChildOverwrite(current, name)
	default:
		return identifierUsed(statement, name)
	}
}

func (t *methodTransformer) identifierUsedInBlockBeforeChildOverwrite(block *ast.BlockStmt, name string) bool {
	for _, statement := range block.List {
		if isReplacementGuard(statement) {
			continue
		}
		if child, ok := childAssignment(statement, t.visitorVar); ok {
			if identifierUsed(child.receiver, name) {
				return true
			}
			if child.nodeName == name || child.okName == name {
				return false
			}
			continue
		}
		if t.identifierUsedBeforeChildOverwrite(statement, name) {
			return true
		}
	}
	return false
}

func removeUnusedMakeAssignments(statements []ast.Stmt) []ast.Stmt {
	result := statements
	for {
		changed := false
		filtered := make([]ast.Stmt, 0, len(result))
		for i, statement := range result {
			assignment, ok := statement.(*ast.AssignStmt)
			if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
				filtered = append(filtered, statement)
				continue
			}
			name := identName(assignment.Lhs[0])
			call, makeCall := assignment.Rhs[0].(*ast.CallExpr)
			if name == "" || !makeCall || !isIdent(call.Fun, "make") || identifierUsedInStatements(result[i+1:], name) {
				filtered = append(filtered, statement)
				continue
			}
			changed = true
		}
		result = filtered
		if !changed {
			return result
		}
	}
}

func (t *methodTransformer) rewriteHelperCalls(node ast.Node) error {
	var rewriteErr error
	ast.Inspect(node, func(current ast.Node) bool {
		if rewriteErr != nil {
			return false
		}
		call, ok := current.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "acceptInPlace" {
			typeExpr, _, err := t.resolveExprType(selector.X)
			if err != nil {
				rewriteErr = t.errorAt(call.Pos(), "cannot classify helper call %s: %v", expressionString(call), err)
				return false
			}
			if pointer, ok := typeExpr.(*ast.StarExpr); ok {
				typeExpr = pointer.X
			}
			receiver, ok := namedType(typeExpr)
			if !ok {
				rewriteErr = t.errorAt(call.Pos(), "cannot classify helper receiver %s", expressionString(selector.X))
				return false
			}
			if t.pkg.methods[receiver]["acceptInPlace"] == "" {
				return true
			}
			if t.pkg.methods[receiver]["acceptInPlace"] != "Visitor" || t.pkg.methods[receiver]["walkInPlace"] != "InPlaceVisitor" {
				rewriteErr = t.errorAt(call.Pos(), "helper %s must pair acceptInPlace(Visitor) with walkInPlace(InPlaceVisitor)", expressionString(selector.X))
				return false
			}
			selector.Sel = ast.NewIdent("walkInPlace")
		}
		return true
	})
	return rewriteErr
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func cloneLocals(input map[string]localBinding) map[string]localBinding {
	result := make(map[string]localBinding, len(input))
	for name, binding := range input {
		result[name] = binding
	}
	return result
}

func cloneSeen(input map[string]bool) map[string]bool {
	result := make(map[string]bool, len(input))
	for name, seen := range input {
		result[name] = seen
	}
	return result
}

func namedType(expr ast.Expr) (string, bool) {
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return "", false
	}
	return ident.Name, true
}

func embeddedName(expr ast.Expr) string {
	if pointer, ok := expr.(*ast.StarExpr); ok {
		expr = pointer.X
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}

func identName(expr ast.Expr) string {
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}

func isIdent(expr ast.Expr, name string) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == name
}

func identifierUsed(node ast.Node, name string) bool {
	used := false
	ast.Inspect(node, func(current ast.Node) bool {
		if ident, ok := current.(*ast.Ident); ok && ident.Name == name {
			used = true
		}
		return true
	})
	return used
}

func identifierUsedInStatements(statements []ast.Stmt, name string) bool {
	for _, statement := range statements {
		if identifierUsed(statement, name) {
			return true
		}
	}
	return false
}

func expressionString(node ast.Node) string {
	var buffer bytes.Buffer
	if err := format.Node(&buffer, token.NewFileSet(), node); err != nil {
		return fmt.Sprintf("%T", node)
	}
	return buffer.String()
}

// WriteFileAtomically writes generated output without exposing a partial file.
func WriteFileAtomically(path string, contents []byte) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".visitor-inplace-*.tmp")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := temporary.Write(contents); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o644); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, path)
}
