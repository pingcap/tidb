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
	"unsafe"

	"github.com/pingcap/tidb/ast"
)

var (
	sizeInsertStmt = int(unsafe.Sizeof(ast.InsertStmt{}))
)

const blockSize = 32*1024*1024 - 16

type node struct {
	block [blockSize]byte
	off   int
	next  *node
}

type allocator struct {
	cache []yySymType
	head  *node
	tail  *node
	ref   []interface{}
}

func newAllocator() *allocator {
	n := &node{}
	return &allocator{
		cache: make([]yySymType, 140),
		head:  n,
		tail:  n,
		ref:   make([]interface{}, 0, 256),
	}
}

func (ac *allocator) reset() {
	for p := ac.head; p != nil; p = p.next {
		p.block = [blockSize]byte{}
		p.off = 0
	}
	ac.tail = ac.head
}

func (ac *allocator) protect(v interface{}) {
	ac.ref = append(ac.ref, v)
}

// alloc allocates an object and return its address.
// use it at your own risk:
// 1. the returned object is already zero-initialized.
// 2. can't alloc object larger than blockSize.
// 3. if the allocated object has references to other object, GC may recycle them.
func (ac *allocator) alloc(size int) unsafe.Pointer {
	n := ac.tail
	if n.off+size > blockSize {
		if n.next != nil {
			n = n.next
		} else {
			n := &node{}
			ac.tail.next = n
		}
		ac.tail = n
	}

	ret := unsafe.Pointer(&n.block[n.off])
	n.off += size
	return ret
}
