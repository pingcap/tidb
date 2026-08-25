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

package visitor_codegen

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestGenerateTraversalGrammar(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "fixture.go", traversalFixture)

	request := GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"fixture.go"},
		ExpectedReceivers: []string{"ConcreteChild", "FixtureNode", "InlineNode", "LeafNode", "NamedResultNode", "NestedLeaveNode", "ValueChild"},
	}
	result := generateFixture(t, request)

	if !reflect.DeepEqual(request.ExpectedReceivers, result.Receivers) {
		t.Fatalf("unexpected receivers: got %v, want %v", result.Receivers, request.ExpectedReceivers)
	}

	source := string(result.Source)
	if got, want := strings.Count(source, "\n\nfunc ("), len(result.Receivers); got != want {
		t.Fatalf("got %d blank-line-separated generated methods, want %d", got, want)
	}
	for _, exact := range []string{
		`func (n *LeafNode) AcceptInPlace(v InPlaceVisitor) bool {
	if skipChildren := v.Enter(n); skipChildren {
		return v.Leave(n)
	}
	return v.Leave(n)
}`,
		`func (n *InlineNode) AcceptInPlace(v InPlaceVisitor) bool {
	if skipChildren := v.Enter(n); skipChildren {
		return v.Leave(n)
	}
	for _, child := range n.Pointers {
		if !child.AcceptInPlace(v) {
			return false
		}
	}
	for _, child := range n.Interfaces {
		if !child.AcceptInPlace(v) {
			return false
		}
	}
	return v.Leave(n)
}`,
		`if !(&n.Extent.Start).AcceptInPlace(v) {
		return false
	}
	if !(&n.Extent.End).AcceptInPlace(v) {
		return false
	}`,
		`for _, table := range n.LockInfo.Tables {
			if !table.AcceptInPlace(v) {
				return false
			}
		}`,
		`if !(&n.Spec).AcceptInPlace(v) {
		return false
	}`,
		`for i := range n.Specs {
		if !(&n.Specs[i]).AcceptInPlace(v) {
			return false
		}
	}`,
		`for _, children := range n.NestedNodes {
		for _, child := range children {
			if !child.AcceptInPlace(v) {
				return false
			}
		}
	}`,
		`if !n.Helper.acceptInPlace(v) {
		return false
	}`,
		`if n.Optional != nil {
		if !n.Optional.AcceptInPlace(v) {
			return false
		}
	}`,
		`if n.LeaveEarly {
		return v.Leave(n)
	}`,
		`if n.EarlyChild != nil {
		if n.EarlyChild.AcceptInPlace(v) {
			return v.Leave(n)
		}
	}`,
	} {
		if !strings.Contains(source, exact) {
			t.Errorf("generated source does not contain exact traversal:\n%s\n\nsource:\n%s", exact, source)
		}
	}

	for _, unwanted := range []string{
		"shouldReplaceNode",
		"replaceNode",
		"newChildren",
		"legacyChildren",
		"newNode",
		"spec.AcceptInPlace",
		".Accept(v)",
		"return n, false",
	} {
		if strings.Contains(source, unwanted) {
			t.Errorf("generated source unexpectedly contains %q:\n%s", unwanted, source)
		}
	}

	second := generateFixture(t, request)
	if !reflect.DeepEqual(result, second) {
		t.Fatal("generation is not deterministic")
	}
	typeCheckFixture(t, traversalFixture, result.Source)

	t.Run("ignored_leaf_and_composite_skip", func(t *testing.T) {
		dir := t.TempDir()
		writeFixture(t, dir, "ignored_skip.go", ignoredCompositeSkipFixture)

		result := generateFixture(t, GenerateRequest{
			SourceDir:         dir,
			TraversalFiles:    []string{"ignored_skip.go"},
			ExpectedReceivers: []string{"CompositeNode", "ConcreteChild"},
		})
		for _, exact := range []string{
			`func (n *ConcreteChild) AcceptInPlace(v InPlaceVisitor) bool {
	if skipChildren := v.Enter(n); skipChildren {
		return v.Leave(n)
	}
	return v.Leave(n)
}`,
			`func (n *CompositeNode) AcceptInPlace(v InPlaceVisitor) bool {
	if skipChildren := v.Enter(n); skipChildren {
		return v.Leave(n)
	}
	if !n.Child.AcceptInPlace(v) {
		return false
	}
	return v.Leave(n)
}`,
		} {
			if !strings.Contains(string(result.Source), exact) {
				t.Errorf("generated source does not contain exact traversal:\n%s\n\nsource:\n%s", exact, result.Source)
			}
		}
	})
}

func TestGenerateChecksExpectedReceiverSetBeforeReturningSource(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "fixture.go", traversalFixture)

	result, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"fixture.go"},
		ExpectedReceivers: []string{"LeafNode"},
	})
	if err == nil {
		t.Fatal("expected receiver inventory error")
	}
	if len(result.Source) != 0 {
		t.Fatalf("returned partial source after inventory failure:\n%s", result.Source)
	}
	for _, want := range []string{"receiver inventory", "ConcreteChild", "FixtureNode"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateRejectsUnknownChildTypeWithContext(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "unknown.go", unknownChildFixture)

	result, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"unknown.go"},
		ExpectedReceivers: []string{"UnknownNode"},
	})
	if err == nil {
		t.Fatal("expected unknown child type error")
	}
	if len(result.Source) != 0 {
		t.Fatalf("returned partial source after transform failure:\n%s", result.Source)
	}
	for _, want := range []string{"unknown.go", "UnknownNode", "Child.Accept", ":"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateRejectsUnsupportedTraversalGrammar(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "unsupported.go", unsupportedGrammarFixture)

	_, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"unsupported.go"},
		ExpectedReceivers: []string{"ConcreteChild", "UnsupportedNode"},
	})
	if err == nil {
		t.Fatal("expected unsupported traversal grammar error")
	}
	for _, want := range []string{"unsupported.go", "UnsupportedNode", "Child.Accept", "unsupported traversal"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateRejectsMismatchedDirectWriteback(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "mismatch.go", mismatchedDirectWritebackFixture)

	_, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"mismatch.go"},
		ExpectedReceivers: []string{"ConcreteChild", "MismatchedNode"},
	})
	if err == nil {
		t.Fatal("expected mismatched writeback error")
	}
	for _, want := range []string{"mismatch.go", "MismatchedNode", "Right", "original child storage"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateRejectsWritebackWithAdditionalAssignment(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "multi_writeback.go", additionalDirectWritebackFixture)

	_, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"multi_writeback.go"},
		ExpectedReceivers: []string{"ConcreteChild", "MultiWritebackNode"},
	})
	if err == nil {
		t.Fatal("expected additional writeback assignment error")
	}
	for _, want := range []string{"multi_writeback.go", "MultiWritebackNode", "one assignment"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateRejectsReplacementGuardSideEffects(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "guard_side_effect.go", replacementGuardSideEffectFixture)

	_, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"guard_side_effect.go"},
		ExpectedReceivers: []string{"GuardSideEffectNode"},
	})
	if err == nil {
		t.Fatal("expected replacement guard side-effect error")
	}
	for _, want := range []string{"guard_side_effect.go", "GuardSideEffectNode", "replacement guard"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateRejectsWritebackFromTransformedChildResult(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "transformed_result.go", transformedChildResultFixture)

	_, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"transformed_result.go"},
		ExpectedReceivers: []string{"ConcreteChild", "TransformedResultNode"},
	})
	if err == nil {
		t.Fatal("expected transformed child result writeback error")
	}
	for _, want := range []string{"transformed_result.go", "TransformedResultNode", "direct child result"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateRejectsElseOnRecognizedChildCheck(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "child_else.go", childCheckElseFixture)

	_, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"child_else.go"},
		ExpectedReceivers: []string{"ChildElseNode", "ConcreteChild"},
	})
	if err == nil {
		t.Fatal("expected child-check else error")
	}
	for _, want := range []string{"child_else.go", "ChildElseNode", "else branch"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateRejectsSideEffectingReturnNode(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "return_side_effect.go", sideEffectingReturnFixture)

	_, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"return_side_effect.go"},
		ExpectedReceivers: []string{"ReturnSideEffectNode"},
	})
	if err == nil {
		t.Fatal("expected side-effecting return error")
	}
	for _, want := range []string{"return_side_effect.go", "ReturnSideEffectNode", "unsupported traversal return"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateRejectsReceiverRebindFromUnrelatedValue(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "unrelated_rebind.go", unrelatedReceiverRebindFixture)

	_, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"unrelated_rebind.go"},
		ExpectedReceivers: []string{"UnrelatedRebindNode"},
	})
	if err == nil {
		t.Fatal("expected unrelated receiver rebind error")
	}
	for _, want := range []string{"unrelated_rebind.go", "UnrelatedRebindNode", "Enter result"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateRejectsDelayedChildResultUse(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "delayed_result.go", delayedChildResultFixture)

	_, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"delayed_result.go"},
		ExpectedReceivers: []string{"ConcreteChild", "DelayedResultNode"},
	})
	if err == nil {
		t.Fatal("expected delayed child result error")
	}
	for _, want := range []string{"delayed_result.go", "DelayedResultNode", "delayed child result"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateRejectsUnindexedConcreteRangeCopy(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "range_copy.go", unindexedConcreteRangeFixture)

	_, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"range_copy.go"},
		ExpectedReceivers: []string{"ValueChild", "ValueContainer"},
	})
	if err == nil {
		t.Fatal("expected range-copy provenance error")
	}
	for _, want := range []string{"range_copy.go", "ValueContainer", "child.Accept", "original storage"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateRejectsAcceptMethodOutsideTraversalFiles(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir, "fixture.go", traversalFixture)
	writeFixture(t, dir, "extra.go", extraAcceptFixture)

	result, err := Generate(GenerateRequest{
		SourceDir:         dir,
		TraversalFiles:    []string{"fixture.go"},
		ExpectedReceivers: []string{"ConcreteChild", "FixtureNode", "InlineNode", "LeafNode", "NamedResultNode", "NestedLeaveNode", "ValueChild"},
	})
	if err == nil {
		t.Fatal("expected out-of-inventory Accept method error")
	}
	if len(result.Source) != 0 {
		t.Fatalf("returned partial source after package inventory failure:\n%s", result.Source)
	}
	for _, want := range []string{"extra.go", "ExtraNode", "outside traversal files"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestGenerateCurrentASTReceiverInventory(t *testing.T) {
	sourceDir := findASTSourceDir(t)
	result, err := Generate(GenerateRequest{SourceDir: sourceDir})
	if err != nil {
		t.Fatal(err)
	}
	const expectedReceiverCount = 213
	if len(result.Receivers) != expectedReceiverCount {
		t.Fatalf("got %d Accept receivers, want %d", len(result.Receivers), expectedReceiverCount)
	}
	for _, receiver := range result.Receivers {
		if receiver == "SetCharsetStmt" {
			t.Fatal("generated a receiver for the block-commented SetCharsetStmt")
		}
	}

	miscSource, err := os.ReadFile(filepath.Join(sourceDir, "misc.go"))
	if err != nil {
		t.Fatal(err)
	}
	commentedAccept := []byte("func (n *SetCharsetStmt) Accept(v Visitor) (Node, bool)")
	acceptOffset := strings.Index(string(miscSource), string(commentedAccept))
	if acceptOffset < 0 {
		t.Fatal("expected the lexical 214th Accept method in misc.go")
	}
	commentStart := strings.LastIndex(string(miscSource[:acceptOffset]), "/*")
	previousCommentEnd := strings.LastIndex(string(miscSource[:acceptOffset]), "*/")
	commentEnd := strings.Index(string(miscSource[acceptOffset:]), "*/")
	if commentStart < 0 || commentStart < previousCommentEnd || commentEnd < 0 {
		t.Fatal("expected the lexical 214th Accept method to remain inside a block comment")
	}
	if len(result.Source) == 0 {
		t.Fatal("generator returned empty source")
	}

	generatedFile, err := parser.ParseFile(token.NewFileSet(), "visitor_inplace_generated.go", result.Source, 0)
	if err != nil {
		t.Fatal(err)
	}
	generatedReceivers := make(map[string]int, len(result.Receivers))
	var generatedWritebacks []string
	for _, decl := range generatedFile.Decls {
		method, ok := decl.(*ast.FuncDecl)
		if !ok || method.Recv == nil || method.Name.Name != "AcceptInPlace" {
			continue
		}
		receiver, ok := receiverTypeName(method)
		if !ok {
			t.Fatalf("cannot identify generated receiver for %s", method.Name.Name)
		}
		generatedReceivers[receiver]++
		ast.Inspect(method.Body, func(node ast.Node) bool {
			assignment, ok := node.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, lhs := range assignment.Lhs {
				if _, local := lhs.(*ast.Ident); !local {
					generatedWritebacks = append(generatedWritebacks, expressionString(lhs))
				}
			}
			return true
		})
	}
	if len(generatedReceivers) != expectedReceiverCount {
		t.Fatalf("got %d generated AcceptInPlace receivers, want %d", len(generatedReceivers), expectedReceiverCount)
	}
	for _, receiver := range result.Receivers {
		if generatedReceivers[receiver] != 1 {
			t.Errorf("generated %d AcceptInPlace methods for %s, want exactly one", generatedReceivers[receiver], receiver)
		}
	}
	if len(generatedWritebacks) != 0 {
		t.Fatalf("generated in-place traversal contains child writebacks: %v", generatedWritebacks)
	}

	checkedInSource, err := os.ReadFile(filepath.Join(sourceDir, "visitor_inplace_generated.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(checkedInSource, result.Source) {
		t.Fatal("visitor_inplace_generated.go is stale; regenerate it with visitor_codegen")
	}
}

func findASTSourceDir(t *testing.T) string {
	t.Helper()
	for _, candidate := range []string{"..", "pkg/parser/ast"} {
		if _, err := os.Stat(filepath.Join(candidate, "ddl.go")); err == nil {
			return candidate
		}
	}
	t.Fatal("AST source files are not available to the test")
	return ""
}

func generateFixture(t *testing.T, request GenerateRequest) GenerateResult {
	t.Helper()
	result, err := Generate(request)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func writeFixture(t *testing.T, dir, name, source string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
}

func typeCheckFixture(t *testing.T, fixture string, generated []byte) {
	t.Helper()
	fset := token.NewFileSet()
	fixtureFile, err := parser.ParseFile(fset, "fixture.go", fixture, 0)
	if err != nil {
		t.Fatal(err)
	}
	generatedFile, err := parser.ParseFile(fset, "visitor_inplace_generated.go", generated, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (&types.Config{}).Check("fixture", fset, []*ast.File{fixtureFile, generatedFile}, nil); err != nil {
		t.Fatalf("generated fixture does not type-check: %v\n%s", err, generated)
	}
}

const traversalFixture = `package fixture

type Node interface {
	Accept(Visitor) (Node, bool)
	AcceptInPlace(InPlaceVisitor) bool
}

type ExprNode interface {
	Node
	exprNode()
}

type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}

type InPlaceVisitor interface {
	Enter(Node) bool
	Leave(Node) bool
}

func shouldReplaceNode(Visitor) bool { return true }

type LeafNode struct{}

func (n *LeafNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*LeafNode)
	return v.Leave(n)
}

type ConcreteChild struct{}

func (n *ConcreteChild) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

type ValueChild struct{}

func (n *ValueChild) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ValueChild)
	return v.Leave(n)
}

type Extent struct {
	Start ValueChild
	End   ValueChild
}

type LockInfo struct {
	Tables []*ConcreteChild
}

type replacementHelper struct{}

func (replacementHelper) accept(Visitor) bool { return true }
func (replacementHelper) acceptInPlace(InPlaceVisitor) bool { return true }

type FixtureNode struct {
	Concrete *ConcreteChild
	Optional *ConcreteChild
	Dynamic  ExprNode
	Children []*ConcreteChild
	LegacyChildren []*ConcreteChild
	Values   []ExprNode
	BareNodes []Node
	NestedNodes [][]Node
	Helper   replacementHelper
	Extent   Extent
	LockInfo *LockInfo
	Spec     ValueChild
	Specs    []ValueChild
}

func (n *FixtureNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FixtureNode)
	replaceNode := shouldReplaceNode(v)

	if n.Concrete != nil {
		node, ok := n.Concrete.Accept(v)
		if !ok {
			return n, false
		}
		if replaceNode {
			n.Concrete = node.(*ConcreteChild)
		}
	}
	if n.Dynamic != nil {
		node, ok := n.Dynamic.Accept(v)
		if !ok {
			return n, false
		}
		if replaceNode {
			n.Dynamic = node.(ExprNode)
		}
	}
	if n.Optional != nil {
		node, ok := n.Optional.Accept(v)
		if !ok {
			return n, false
		}
		n.Optional, _ = node.(*ConcreteChild)
	}
	newChildren := make([]*ConcreteChild, len(n.Children))
	for i, child := range n.Children {
		node, ok := child.Accept(v)
		if !ok {
			return n, false
		}
		if replaceNode {
			newChildren[i] = node.(*ConcreteChild)
		}
	}
	if replaceNode {
		n.Children = newChildren
	}
	legacyChildren := make([]*ConcreteChild, len(n.LegacyChildren))
	for i, child := range n.LegacyChildren {
		node, ok := child.Accept(v)
		if !ok {
			return n, false
		}
		legacyChildren[i] = node.(*ConcreteChild)
	}
	n.LegacyChildren = legacyChildren

	for i, child := range n.Values {
		node, ok := child.Accept(v)
		if !ok {
			return n, false
		}
		if replaceNode {
			n.Values[i] = node.(ExprNode)
		}
	}
	for i, child := range n.BareNodes {
		node, ok := child.Accept(v)
		if !ok {
			return n, false
		}
		n.BareNodes[i] = node
	}
	for i, children := range n.NestedNodes {
		for j, child := range children {
			node, ok := child.Accept(v)
			if !ok {
				return n, false
			}
			n.NestedNodes[i][j] = node
		}
	}

	if !n.Helper.accept(v) {
		return n, false
	}

	node, ok := n.Extent.Start.Accept(v)
	if !ok {
		return n, false
	}
	if replaceNode {
		n.Extent.Start = *node.(*ValueChild)
	}
	node, ok = n.Extent.End.Accept(v)
	if !ok {
		return n, false
	}
	if replaceNode {
		n.Extent.End = *node.(*ValueChild)
	}

	if n.LockInfo != nil {
		for i, table := range n.LockInfo.Tables {
			node, ok := table.Accept(v)
			if !ok {
				return n, false
			}
			if replaceNode {
				n.LockInfo.Tables[i] = node.(*ConcreteChild)
			}
		}
	}

	node, ok = n.Spec.Accept(v)
	if !ok {
		return n, false
	}
	if replaceNode {
		n.Spec = *node.(*ValueChild)
	}

	for i, spec := range n.Specs {
		node, ok := spec.Accept(v)
		if !ok {
			return n, false
		}
		if replaceNode {
			n.Specs[i] = *node.(*ValueChild)
		}
	}

	return v.Leave(n)
}

type InlineNode struct {
	Pointers   []*ConcreteChild
	Interfaces []ExprNode
}

func (n *InlineNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*InlineNode)
	for _, child := range n.Pointers {
		if _, ok := child.Accept(v); !ok {
			return n, false
		}
	}
	for _, child := range n.Interfaces {
		_, ok := child.Accept(v)
		if !ok {
			return n, false
		}
	}
	return v.Leave(n)
}

type NamedResultNode struct {
	Concrete *ConcreteChild
	Dynamic  ExprNode
}

func (n *NamedResultNode) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChild := v.Enter(n)
	if skipChild {
		return v.Leave(newNode)
	}
	n = newNode.(*NamedResultNode)
	if n.Concrete != nil {
		node, ok = n.Concrete.Accept(v)
		if !ok {
			return node, false
		}
		n.Concrete = node.(*ConcreteChild)
	}
	if n.Dynamic != nil {
		node, ok = n.Dynamic.Accept(v)
		if !ok {
			return n, false
		}
		n.Dynamic = node.(ExprNode)
	}
	return v.Leave(n)
}

type NestedLeaveNode struct {
	Child      *ConcreteChild
	EarlyChild *ConcreteChild
	LeaveEarly bool
}

func (n *NestedLeaveNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*NestedLeaveNode)
	if n.Child != nil {
		node, ok := n.Child.Accept(v)
		if !ok {
			return n, false
		}
		n.Child = node.(*ConcreteChild)
	}
	if n.LeaveEarly {
		return v.Leave(n)
	}
	if n.EarlyChild != nil {
		newNode, childOK := n.EarlyChild.Accept(v)
		if childOK {
			return v.Leave(n)
		}
		n.EarlyChild = newNode.(*ConcreteChild)
	}
	return v.Leave(n)
}
`

const unknownChildFixture = `package fixture

type Node interface {
	Accept(Visitor) (Node, bool)
}

type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}

type UnknownChild interface {
	Accept(Visitor) (Node, bool)
}

type UnknownNode struct {
	Child UnknownChild
}

func (n *UnknownNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*UnknownNode)
	child, ok := n.Child.Accept(v)
	if !ok {
		return n, false
	}
	n.Child = child.(UnknownChild)
	return v.Leave(n)
}
`

const unsupportedGrammarFixture = `package fixture

type Node interface { Accept(Visitor) (Node, bool) }
type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}
type ConcreteChild struct{}
func (n *ConcreteChild) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	return v.Leave(newNode)
}
type UnsupportedNode struct { Child *ConcreteChild }
func (n *UnsupportedNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	for i := 0; i < 1; i++ {
		child, ok := n.Child.Accept(v)
		if !ok { return n, false }
		n.Child = child.(*ConcreteChild)
	}
	return v.Leave(n)
}
`

const ignoredCompositeSkipFixture = `package fixture

type Node interface { Accept(Visitor) (Node, bool) }
type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}
type ConcreteChild struct{}
func (n *ConcreteChild) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}
type CompositeNode struct { Child *ConcreteChild }
func (n *CompositeNode) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	n = newNode.(*CompositeNode)
	child, ok := n.Child.Accept(v)
	if !ok { return n, false }
	n.Child = child.(*ConcreteChild)
	return v.Leave(n)
}
`

const mismatchedDirectWritebackFixture = `package fixture

type Node interface { Accept(Visitor) (Node, bool) }
type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}
type ConcreteChild struct{}
func (n *ConcreteChild) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	return v.Leave(newNode)
}
type MismatchedNode struct {
	Left  *ConcreteChild
	Right *ConcreteChild
}
func (n *MismatchedNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	n = newNode.(*MismatchedNode)
	child, ok := n.Left.Accept(v)
	if !ok { return n, false }
	n.Right = child.(*ConcreteChild)
	return v.Leave(n)
}
`

const additionalDirectWritebackFixture = `package fixture

type Node interface { Accept(Visitor) (Node, bool) }
type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}
type ConcreteChild struct{}
func (n *ConcreteChild) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	return v.Leave(newNode)
}
type MultiWritebackNode struct {
	Child *ConcreteChild
	Other *ConcreteChild
}
func (n *MultiWritebackNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	n = newNode.(*MultiWritebackNode)
	child, ok := n.Child.Accept(v)
	if !ok { return n, false }
	n.Child, n.Other = child.(*ConcreteChild), n.Other
	return v.Leave(n)
}
`

const replacementGuardSideEffectFixture = `package fixture

type Node interface { Accept(Visitor) (Node, bool) }
type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}
func shouldReplaceNode(Visitor) bool { return true }
func recordReplacement() {}
type GuardSideEffectNode struct{}
func (n *GuardSideEffectNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	n = newNode.(*GuardSideEffectNode)
	replaceNode := shouldReplaceNode(v)
	if replaceNode {
		recordReplacement()
	}
	return v.Leave(n)
}
`

const transformedChildResultFixture = `package fixture

type Node interface { Accept(Visitor) (Node, bool) }
type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}
func identity(n Node) Node { return n }
type ConcreteChild struct{}
func (n *ConcreteChild) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	return v.Leave(newNode)
}
type TransformedResultNode struct { Child *ConcreteChild }
func (n *TransformedResultNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	n = newNode.(*TransformedResultNode)
	child, ok := n.Child.Accept(v)
	if !ok { return n, false }
	n.Child = identity(child).(*ConcreteChild)
	return v.Leave(n)
}
`

const childCheckElseFixture = `package fixture

type Node interface { Accept(Visitor) (Node, bool) }
type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}
func recordSuccess() {}
type ConcreteChild struct{}
func (n *ConcreteChild) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	return v.Leave(newNode)
}
type ChildElseNode struct { Child *ConcreteChild }
func (n *ChildElseNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	n = newNode.(*ChildElseNode)
	child, ok := n.Child.Accept(v)
	if !ok {
		return n, false
	} else {
		recordSuccess()
	}
	n.Child = child.(*ConcreteChild)
	return v.Leave(n)
}
`

const sideEffectingReturnFixture = `package fixture

type Node interface { Accept(Visitor) (Node, bool) }
type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}
func replacement(n Node) Node { return n }
type ReturnSideEffectNode struct{}
func (n *ReturnSideEffectNode) Accept(v Visitor) (Node, bool) {
	_, _ = v.Enter(n)
	return replacement(n), false
}
`

const unrelatedReceiverRebindFixture = `package fixture

type Node interface { Accept(Visitor) (Node, bool) }
type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}
func alternate(n Node) Node { return n }
type UnrelatedRebindNode struct{}
func (n *UnrelatedRebindNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	n = alternate(n).(*UnrelatedRebindNode)
	return v.Leave(n)
}
`

const delayedChildResultFixture = `package fixture

type Node interface { Accept(Visitor) (Node, bool) }
type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}
func trace() {}
type ConcreteChild struct{}
func (n *ConcreteChild) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	return v.Leave(newNode)
}
type DelayedResultNode struct { Child *ConcreteChild }
func (n *DelayedResultNode) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	n = newNode.(*DelayedResultNode)
	child, ok := n.Child.Accept(v)
	if !ok { return n, false }
	trace()
	n.Child = child.(*ConcreteChild)
	return v.Leave(n)
}
`

const unindexedConcreteRangeFixture = `package fixture

type Node interface { Accept(Visitor) (Node, bool) }
type Visitor interface {
	Enter(Node) (Node, bool)
	Leave(Node) (Node, bool)
}
type ValueChild struct{}
func (n *ValueChild) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	return v.Leave(newNode)
}
type ValueContainer struct { Values []ValueChild }
func (n *ValueContainer) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren { return v.Leave(newNode) }
	n = newNode.(*ValueContainer)
	for _, child := range n.Values {
		_, ok := child.Accept(v)
		if !ok { return n, false }
	}
	return v.Leave(n)
}
`

const extraAcceptFixture = `package fixture

type ExtraNode struct{}
func (n *ExtraNode) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}
`
