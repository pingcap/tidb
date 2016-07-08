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
// See the License for the specific language governing permissions and
// limitations under the License.
package parser

import (
	"fmt"
	"unsafe"

	"github.com/pingcap/tidb/ast"
)

var (
	sizeDeleteStmt      = int(unsafe.Sizeof(ast.DeleteStmt{}))
	sizeInsertStmt      = int(unsafe.Sizeof(ast.InsertStmt{}))
	sizeUnionStmt       = int(unsafe.Sizeof(ast.UnionStmt{}))
	sizeUpdateStmt      = int(unsafe.Sizeof(ast.UpdateStmt{}))
	sizeSelectStmt      = int(unsafe.Sizeof(ast.SelectStmt{}))
	sizeShowStmt        = int(unsafe.Sizeof(ast.ShowStmt{}))
	sizeAssignment      = int(unsafe.Sizeof(ast.Assignment{}))
	sizeByItem          = int(unsafe.Sizeof(ast.ByItem{}))
	sizeFieldList       = int(unsafe.Sizeof(ast.FieldList{}))
	sizeGroupByClause   = int(unsafe.Sizeof(ast.GroupByClause{}))
	sizeHavingClause    = int(unsafe.Sizeof(ast.HavingClause{}))
	sizeJoin            = int(unsafe.Sizeof(ast.Join{}))
	sizeLimit           = int(unsafe.Sizeof(ast.Limit{}))
	sizeOnCondition     = int(unsafe.Sizeof(ast.OnCondition{}))
	sizeOrderByClause   = int(unsafe.Sizeof(ast.OrderByClause{}))
	sizeSelectField     = int(unsafe.Sizeof(ast.SelectField{}))
	sizeTableName       = int(unsafe.Sizeof(ast.TableName{}))
	sizeTableRefsClause = int(unsafe.Sizeof(ast.TableRefsClause{}))
	sizeTableSource     = int(unsafe.Sizeof(ast.TableSource{}))
	sizeUnionSelectList = int(unsafe.Sizeof(ast.UnionSelectList{}))
	sizeWildCardField   = int(unsafe.Sizeof(ast.WildCardField{}))
)

const blockSize = 168 // 30*1024 - 16

type node struct {
	block  [blockSize]byte
	offset int
	next   *node
}

type allocator struct {
	cache []yySymType
	head  *node
	tail  *node
}

func newAllocator() *allocator {
	n := &node{}
	fmt.Println("*******!!!!!!\n")
	fmt.Printf("%p %p\n", &n.block[0], &n.block[blockSize-1])

	return &allocator{
		cache: make([]yySymType, 140),
		head:  n,
		tail:  n,
	}
}

// alloc allocates an object with size, return its address.
// notice: cannot allocate object bigger than blockSize.
// warn: the returned object is not initialized, i.e, we adopt C malloc idiom instead of Go.
func (ac *allocator) alloc(size int) unsafe.Pointer {
	n := ac.tail
	if n.offset+size > blockSize {
		if n.next != nil {
			panic("don't reuse, so should not run here!")
			n = n.next
		} else {
			n = &node{}
			fmt.Println("*******!!!!!!\n")
			fmt.Printf("%p %p\n", &n.block[0], &n.block[blockSize-1])
			ac.tail.next = n
		}
		ac.tail = n
	}

	ret := unsafe.Pointer(&n.block[n.offset])
	n.offset += size
	return ret
}

func (ac *allocator) reset() {
	panic("fuck")
	for p := ac.head; p != nil; p = p.next {
		p.offset = 0
	}
	ac.tail = ac.head
}

func (ac *allocator) allocDeleteStmt() *ast.DeleteStmt {
	return (*ast.DeleteStmt)(ac.alloc(sizeDeleteStmt))
}
func (ac *allocator) allocInsertStmt() *ast.InsertStmt {
	ret := (*ast.InsertStmt)(ac.alloc(sizeInsertStmt))
	*ret = ast.InsertStmt{}
	if uintptr(unsafe.Pointer(ret)) == uintptr(unsafe.Pointer(ac.tail)) {
		fmt.Println("allocate from a new node")
		for p := ac.head; p != nil; p = p.next {
			fmt.Printf("offset=%d %p\n", p.offset, p.next)
		}
	}
	return ret
}
func (ac *allocator) allocUnionStmt() *ast.UnionStmt {
	return (*ast.UnionStmt)(ac.alloc(sizeUnionStmt))
}
func (ac *allocator) allocUpdateStmt() *ast.UpdateStmt {
	return (*ast.UpdateStmt)(ac.alloc(sizeUpdateStmt))
}
func (ac *allocator) allocSelectStmt() *ast.SelectStmt {
	return (*ast.SelectStmt)(ac.alloc(sizeSelectStmt))
}
func (ac *allocator) allocShowStmt() *ast.ShowStmt {
	return (*ast.ShowStmt)(ac.alloc(sizeShowStmt))
}
func (ac *allocator) allocAssignment() *ast.Assignment {
	return (*ast.Assignment)(ac.alloc(sizeAssignment))
}
func (ac *allocator) allocByItem() *ast.ByItem {
	return (*ast.ByItem)(ac.alloc(sizeByItem))
}
func (ac *allocator) allocFieldList() *ast.FieldList {
	return (*ast.FieldList)(ac.alloc(sizeFieldList))
}
func (ac *allocator) allocGroupByClause() *ast.GroupByClause {
	return (*ast.GroupByClause)(ac.alloc(sizeGroupByClause))
}
func (ac *allocator) allocHavingClause() *ast.HavingClause {
	return (*ast.HavingClause)(ac.alloc(sizeHavingClause))
}
func (ac *allocator) allocJoin() *ast.Join {
	return (*ast.Join)(ac.alloc(sizeJoin))
}
func (ac *allocator) allocLimit() *ast.Limit {
	return (*ast.Limit)(ac.alloc(sizeLimit))
}
func (ac *allocator) allocOnCondition() *ast.OnCondition {
	return (*ast.OnCondition)(ac.alloc(sizeOnCondition))
}
func (ac *allocator) allocOrderByClause() *ast.OrderByClause {
	return (*ast.OrderByClause)(ac.alloc(sizeOrderByClause))
}
func (ac *allocator) allocSelectField() *ast.SelectField {
	return (*ast.SelectField)(ac.alloc(sizeSelectField))
}
func (ac *allocator) allocTableName() *ast.TableName {
	return (*ast.TableName)(ac.alloc(sizeTableName))
}
func (ac *allocator) allocTableRefsClause() *ast.TableRefsClause {
	return (*ast.TableRefsClause)(ac.alloc(sizeTableRefsClause))
}
func (ac *allocator) allocTableSource() *ast.TableSource {
	return (*ast.TableSource)(ac.alloc(sizeTableSource))
}
func (ac *allocator) allocUnionSelectList() *ast.UnionSelectList {
	return (*ast.UnionSelectList)(ac.alloc(sizeUnionSelectList))
}
func (ac *allocator) allocWildCardField() *ast.WildCardField {
	return (*ast.WildCardField)(ac.alloc(sizeWildCardField))
}
