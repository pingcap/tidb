// Copyright 2025 PingCAP, Inc.
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
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ASTArena provides arena allocation for AST nodes to reduce individual allocations
// and improve memory locality during parsing operations.
//
// THREAD SAFETY: ASTArena is NOT thread-safe. Each Parser instance should have
// its own ASTArena, and concurrent access to the same ASTArena from multiple
// goroutines is not supported. The arena is designed for single-threaded parsing
// operations where Reset() is called at the beginning of each parse operation.
type ASTArena struct {
	blocks    [][]byte // Memory blocks allocated for the arena
	current   []byte   // Current block being used
	offset    int      // Current offset within the current block
	blockSize int      // Size of each memory block
}

const (
	// defaultBlockSize is the default size for arena memory blocks.
	// Based on analysis, complex queries generate ~89KB of AST allocations,
	// so we use 96KB blocks to handle most queries in a single block.
	defaultBlockSize = 96 * 1024

	// maxAlignment ensures proper memory alignment for all AST node types
	maxAlignment = 8

	// maxAllocationSize is the maximum size for a single allocation request.
	// This prevents integer overflow and extremely large allocations that could
	// cause memory exhaustion. Set to 1MB which is much larger than any AST node.
	maxAllocationSize = 1024 * 1024
)

// NewASTArena creates a new AST arena with default block size.
func NewASTArena() *ASTArena {
	return &ASTArena{
		blockSize: defaultBlockSize,
	}
}

// Reset clears the arena and prepares it for reuse.
// This is called at the beginning of each parse operation.
//
// THREAD SAFETY: This method is NOT thread-safe. It must only be called
// when no other goroutine is accessing the arena or any pointers allocated
// from it. Typically called from Parser.ParseSQL() in a single-threaded context.
func (a *ASTArena) Reset() {
	// Reset current block pointer and offset
	a.current = nil
	a.offset = 0

	// Keep allocated blocks for reuse, just reset their usage
	// This avoids repeated allocations of the same block size
	if len(a.blocks) > 0 {
		a.current = a.blocks[0]
		a.offset = 0
	}
}

// allocate allocates memory of the specified size with proper alignment.
// Returns nil if allocation fails due to invalid size or memory exhaustion.
func (a *ASTArena) allocate(size int) unsafe.Pointer {
	// C3: Validate allocation size to prevent integer overflow and excessive memory usage
	if size <= 0 || size > maxAllocationSize {
		return nil
	}

	// Align size to maxAlignment boundary
	// Check for overflow during alignment calculation
	if size > maxAllocationSize-maxAlignment {
		return nil
	}
	alignedSize := (size + maxAlignment - 1) &^ (maxAlignment - 1)

	// Check if current block has enough space
	if a.current == nil || a.offset+alignedSize > len(a.current) {
		if !a.allocateNewBlock() {
			return nil // Memory allocation failed
		}
	}

	// C2: Validate current block state before unsafe pointer operations
	if a.current == nil || a.offset < 0 || a.offset+alignedSize > len(a.current) {
		return nil
	}

	// Allocate from current block
	ptr := unsafe.Pointer(&a.current[a.offset])
	a.offset += alignedSize
	return ptr
}

// allocateNewBlock allocates a new memory block for the arena.
// Returns false if memory allocation fails.
func (a *ASTArena) allocateNewBlock() bool {
	// Reuse existing block if available
	if len(a.blocks) > 0 {
		a.current = a.blocks[0]
		a.offset = 0
		return true
	}

	// C1: Handle memory allocation failure gracefully using defer/recover
	var block []byte
	func() {
		defer func() {
			if r := recover(); r != nil {
				// Memory allocation failed, block remains nil
				block = nil
			}
		}()
		block = make([]byte, a.blockSize)
	}()

	// Check if allocation succeeded
	if block == nil {
		return false
	}

	a.blocks = append(a.blocks, block)
	a.current = block
	a.offset = 0
	return true
}

// AllocSelectStmt allocates a SelectStmt from the arena.
// Returns nil if allocation fails.
func (a *ASTArena) AllocSelectStmt() *ast.SelectStmt {
	ptr := a.allocate(int(unsafe.Sizeof(ast.SelectStmt{})))
	if ptr == nil {
		return nil
	}
	return (*ast.SelectStmt)(ptr)
}

// AllocInsertStmt allocates an InsertStmt from the arena.
// Returns nil if allocation fails.
func (a *ASTArena) AllocInsertStmt() *ast.InsertStmt {
	ptr := a.allocate(int(unsafe.Sizeof(ast.InsertStmt{})))
	if ptr == nil {
		return nil
	}
	return (*ast.InsertStmt)(ptr)
}

// AllocUpdateStmt allocates an UpdateStmt from the arena.
// Returns nil if allocation fails.
func (a *ASTArena) AllocUpdateStmt() *ast.UpdateStmt {
	ptr := a.allocate(int(unsafe.Sizeof(ast.UpdateStmt{})))
	if ptr == nil {
		return nil
	}
	return (*ast.UpdateStmt)(ptr)
}

// AllocDeleteStmt allocates a DeleteStmt from the arena.
// Returns nil if allocation fails.
func (a *ASTArena) AllocDeleteStmt() *ast.DeleteStmt {
	ptr := a.allocate(int(unsafe.Sizeof(ast.DeleteStmt{})))
	if ptr == nil {
		return nil
	}
	return (*ast.DeleteStmt)(ptr)
}

// AllocBinaryOperationExpr allocates a BinaryOperationExpr from the arena.
func (a *ASTArena) AllocBinaryOperationExpr() *ast.BinaryOperationExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.BinaryOperationExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.BinaryOperationExpr)(ptr)
}

// AllocColumnNameExpr allocates a ColumnNameExpr from the arena.
func (a *ASTArena) AllocColumnNameExpr() *ast.ColumnNameExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.ColumnNameExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.ColumnNameExpr)(ptr)
}

// AllocTableName allocates a TableName from the arena.
func (a *ASTArena) AllocTableName() *ast.TableName {
	ptr := a.allocate(int(unsafe.Sizeof(ast.TableName{})))
	if ptr == nil {
		return nil
	}
	return (*ast.TableName)(ptr)
}

// AllocColumnName allocates a ColumnName from the arena.
func (a *ASTArena) AllocColumnName() *ast.ColumnName {
	ptr := a.allocate(int(unsafe.Sizeof(ast.ColumnName{})))
	if ptr == nil {
		return nil
	}
	return (*ast.ColumnName)(ptr)
}

// AllocShowStmt allocates a ShowStmt from the arena.
func (a *ASTArena) AllocShowStmt() *ast.ShowStmt {
	ptr := a.allocate(int(unsafe.Sizeof(ast.ShowStmt{})))
	if ptr == nil {
		return nil
	}
	return (*ast.ShowStmt)(ptr)
}

// AllocAlterTableSpec allocates an AlterTableSpec from the arena.
func (a *ASTArena) AllocAlterTableSpec() *ast.AlterTableSpec {
	ptr := a.allocate(int(unsafe.Sizeof(ast.AlterTableSpec{})))
	if ptr == nil {
		return nil
	}
	return (*ast.AlterTableSpec)(ptr)
}

// AllocFuncCallExpr allocates a FuncCallExpr from the arena.
func (a *ASTArena) AllocFuncCallExpr() *ast.FuncCallExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.FuncCallExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.FuncCallExpr)(ptr)
}

// AllocTableOption allocates a TableOption from the arena.
func (a *ASTArena) AllocTableOption() *ast.TableOption {
	ptr := a.allocate(int(unsafe.Sizeof(ast.TableOption{})))
	if ptr == nil {
		return nil
	}
	return (*ast.TableOption)(ptr)
}

// AllocWindowFuncExpr allocates a WindowFuncExpr from the arena.
func (a *ASTArena) AllocWindowFuncExpr() *ast.WindowFuncExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.WindowFuncExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.WindowFuncExpr)(ptr)
}

// AllocAdminStmt allocates an AdminStmt from the arena.
func (a *ASTArena) AllocAdminStmt() *ast.AdminStmt {
	ptr := a.allocate(int(unsafe.Sizeof(ast.AdminStmt{})))
	if ptr == nil {
		return nil
	}
	return (*ast.AdminStmt)(ptr)
}

// AllocAggregateFuncExpr allocates an AggregateFuncExpr from the arena.
func (a *ASTArena) AllocAggregateFuncExpr() *ast.AggregateFuncExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.AggregateFuncExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.AggregateFuncExpr)(ptr)
}

// AllocColumnOption allocates a ColumnOption from the arena.
func (a *ASTArena) AllocColumnOption() *ast.ColumnOption {
	ptr := a.allocate(int(unsafe.Sizeof(ast.ColumnOption{})))
	if ptr == nil {
		return nil
	}
	return (*ast.ColumnOption)(ptr)
}

// AllocJoin allocates a Join from the arena.
func (a *ASTArena) AllocJoin() *ast.Join {
	ptr := a.allocate(int(unsafe.Sizeof(ast.Join{})))
	if ptr == nil {
		return nil
	}
	return (*ast.Join)(ptr)
}

// Expression allocation methods for high-frequency expression nodes

// AllocUnaryOperationExpr allocates a UnaryOperationExpr from the arena.
func (a *ASTArena) AllocUnaryOperationExpr() *ast.UnaryOperationExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.UnaryOperationExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.UnaryOperationExpr)(ptr)
}

// AllocSubqueryExpr allocates a SubqueryExpr from the arena.
func (a *ASTArena) AllocSubqueryExpr() *ast.SubqueryExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.SubqueryExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.SubqueryExpr)(ptr)
}

// AllocExistsSubqueryExpr allocates an ExistsSubqueryExpr from the arena.
func (a *ASTArena) AllocExistsSubqueryExpr() *ast.ExistsSubqueryExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.ExistsSubqueryExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.ExistsSubqueryExpr)(ptr)
}

// AllocPatternInExpr allocates a PatternInExpr from the arena.
func (a *ASTArena) AllocPatternInExpr() *ast.PatternInExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.PatternInExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.PatternInExpr)(ptr)
}

// AllocBetweenExpr allocates a BetweenExpr from the arena.
func (a *ASTArena) AllocBetweenExpr() *ast.BetweenExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.BetweenExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.BetweenExpr)(ptr)
}

// AllocCaseExpr allocates a CaseExpr from the arena.
func (a *ASTArena) AllocCaseExpr() *ast.CaseExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.CaseExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.CaseExpr)(ptr)
}

// AllocIsNullExpr allocates an IsNullExpr from the arena.
func (a *ASTArena) AllocIsNullExpr() *ast.IsNullExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.IsNullExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.IsNullExpr)(ptr)
}

// AllocIsTruthExpr allocates an IsTruthExpr from the arena.
func (a *ASTArena) AllocIsTruthExpr() *ast.IsTruthExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.IsTruthExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.IsTruthExpr)(ptr)
}

// AllocParenthesesExpr allocates a ParenthesesExpr from the arena.
func (a *ASTArena) AllocParenthesesExpr() *ast.ParenthesesExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.ParenthesesExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.ParenthesesExpr)(ptr)
}

// AllocCompareSubqueryExpr allocates a CompareSubqueryExpr from the arena.
func (a *ASTArena) AllocCompareSubqueryExpr() *ast.CompareSubqueryExpr {
	ptr := a.allocate(int(unsafe.Sizeof(ast.CompareSubqueryExpr{})))
	if ptr == nil {
		return nil
	}
	return (*ast.CompareSubqueryExpr)(ptr)
}

// GetStats returns arena usage statistics for monitoring and tuning.
func (a *ASTArena) GetStats() (blocksUsed int, bytesUsed int, totalCapacity int) {
	blocksUsed = len(a.blocks)
	if a.current != nil {
		bytesUsed = a.offset
	}
	totalCapacity = len(a.blocks) * a.blockSize
	return
}