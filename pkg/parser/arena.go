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
	"github.com/pingcap/tidb/pkg/parser/auth"
)

const (
	// defaultBlockSize is the default arena block size (8 KB).
	// Grows by doubling for larger queries.
	defaultBlockSize = 8 * 1024

	// slabSize is the number of elements pre-allocated per typed slab batch.
	// 64 gives a good balance: large enough to amortize allocation overhead,
	// small enough to avoid excessive waste for small queries.
	slabSize = 64
)

// ---------------------------------------------------------------------------
// Typed slab allocator — GC-safe batch allocation for hot AST node types
// ---------------------------------------------------------------------------
//
// Go's GC needs type information to trace pointers within objects. A generic
// []byte arena allocator is invisible to the GC: pointer fields (strings,
// interfaces, slices) inside arena-allocated AST nodes won't be traced,
// causing dangling pointers and SIGBUS.
//
// Typed slabs solve this: []ast.ColumnName is a typed slice — the GC knows
// every field's layout and traces all interior pointers. Instead of N
// individual new(T) calls (each creating a small heap object), we make
// N/slabSize calls to make([]T, slabSize). This reduces heap object count,
// GC marking work, and allocation overhead.

// slab is a pre-allocated batch of typed objects.
type slab[T interface{}] struct {
	items []T
	idx   int
}

// alloc returns a pointer to a zero-initialized element from the slab.
// Allocates a new batch when the current one is exhausted.
func (s *slab[T]) alloc() *T {
	if s.idx >= len(s.items) {
		s.items = make([]T, slabSize)
		s.idx = 0
	}
	p := &s.items[s.idx]
	s.idx++
	return p
}

// reset resets the slab index for reuse. The backing slice is retained by the
// GC for as long as any element pointer is alive — no dangling references.
func (s *slab[T]) reset() {
	s.items = nil
	s.idx = 0
}

// ---------------------------------------------------------------------------
// Arena
// ---------------------------------------------------------------------------

// Arena provides memory allocation for AST nodes during parsing.
//
// It contains typed slabs for the most frequently allocated AST node types.
// These slabs are GC-safe: each uses a typed Go slice, so the garbage collector
// can properly trace all pointer fields within allocated nodes.
//
// For less common types, the generic Alloc[T] function falls back to new(T).
type Arena struct {
	// Typed slabs for hot AST node types (GC-safe batch allocation).
	columnNames  slab[ast.ColumnName]
	joins        slab[ast.Join]
	subqueryExpr slab[ast.SubqueryExpr]
	funcCallExpr slab[ast.FuncCallExpr]
	varAssign    slab[ast.VariableAssignment]
	defaultExpr  slab[ast.DefaultExpr]
	tableOption  slab[ast.TableOption]
	showStmt     slab[ast.ShowStmt]
	constraint   slab[ast.Constraint]
	selectStmt   slab[ast.SelectStmt]
	userIdentity slab[auth.UserIdentity]
}

// NewArena creates a new Arena.
func NewArena() *Arena {
	return &Arena{}
}

// Reset resets the arena for reuse. All slab indices are reset. The typed slab
// backing slices are released (but stay alive in the GC as long as any element
// pointer is reachable).
func (a *Arena) Reset() {
	a.columnNames.reset()
	a.joins.reset()
	a.subqueryExpr.reset()
	a.funcCallExpr.reset()
	a.varAssign.reset()
	a.defaultExpr.reset()
	a.tableOption.reset()
	a.showStmt.reset()
	a.constraint.reset()
	a.selectStmt.reset()
	a.userIdentity.reset()
}

// ---------------------------------------------------------------------------
// Hot-type slab allocation methods
// ---------------------------------------------------------------------------

// AllocColumnName allocates an ast.ColumnName from the typed slab.
func (a *Arena) AllocColumnName() *ast.ColumnName { return a.columnNames.alloc() }

// AllocJoin allocates an ast.Join from the typed slab.
func (a *Arena) AllocJoin() *ast.Join { return a.joins.alloc() }

// AllocSubqueryExpr allocates an ast.SubqueryExpr from the typed slab.
func (a *Arena) AllocSubqueryExpr() *ast.SubqueryExpr { return a.subqueryExpr.alloc() }

// AllocFuncCallExpr allocates an ast.FuncCallExpr from the typed slab.
func (a *Arena) AllocFuncCallExpr() *ast.FuncCallExpr { return a.funcCallExpr.alloc() }

// AllocVariableAssignment allocates an ast.VariableAssignment from the typed slab.
func (a *Arena) AllocVariableAssignment() *ast.VariableAssignment { return a.varAssign.alloc() }

// AllocDefaultExpr allocates an ast.DefaultExpr from the typed slab.
func (a *Arena) AllocDefaultExpr() *ast.DefaultExpr { return a.defaultExpr.alloc() }

// AllocTableOption allocates an ast.TableOption from the typed slab.
func (a *Arena) AllocTableOption() *ast.TableOption { return a.tableOption.alloc() }

// AllocShowStmt allocates an ast.ShowStmt from the typed slab.
func (a *Arena) AllocShowStmt() *ast.ShowStmt { return a.showStmt.alloc() }

// AllocConstraint allocates an ast.Constraint from the typed slab.
func (a *Arena) AllocConstraint() *ast.Constraint { return a.constraint.alloc() }

// AllocSelectStmt allocates an ast.SelectStmt from the typed slab.
func (a *Arena) AllocSelectStmt() *ast.SelectStmt { return a.selectStmt.alloc() }

// AllocUserIdentity allocates an auth.UserIdentity from the typed slab.
func (a *Arena) AllocUserIdentity() *auth.UserIdentity { return a.userIdentity.alloc() }

// ---------------------------------------------------------------------------
// Generic allocator (fallback for non-hot types)
// ---------------------------------------------------------------------------

// Alloc allocates a zero-initialized value of type T.
//
// For hot AST node types, use the dedicated Arena methods (e.g.
// AllocColumnName, AllocSelectStmt) which use GC-safe typed slab allocation.
// This generic function falls back to heap allocation via new(T).
func Alloc[T interface{}](_ *Arena) *T {
	return new(T)
}

// AllocSlice allocates a slice of n elements of type T.
func AllocSlice[T interface{}](_ *Arena, n int) []T {
	if n == 0 {
		return nil
	}
	return make([]T, n)
}
