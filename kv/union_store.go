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

package kv

import (
	"github.com/pingcap/parser/model"
)

// FlagsOp TODO:duplicated for BR describes KeyFlags modify operation.
type FlagsOp uint16

// Option is used for customizing kv store's behaviors during a transaction.
type Option int

// Options is an interface of a set of options. Each option is associated with a value.
type Options interface {
	// Get gets an option value.
	Get(opt Option) (v interface{}, ok bool)
}

// UnionStore is a store that wraps a snapshot for read and a MemBuffer for buffered write.
// Also, it provides some transaction related utilities.
type UnionStore interface {
	Retriever

	// HasPresumeKeyNotExists returns whether the key presumed key not exists error for the lazy check.
	HasPresumeKeyNotExists(k Key) bool
	// UnmarkPresumeKeyNotExists deletes the key presume key not exists error flag for the lazy check.
	UnmarkPresumeKeyNotExists(k Key)
	// CacheIndexName caches the index name.
	// PresumeKeyNotExists will use this to help decode error message.
	CacheTableInfo(id int64, info *model.TableInfo)
	// GetIndexName returns the cached index name.
	// If there is no such index already inserted through CacheIndexName, it will return UNKNOWN.
	GetTableInfo(id int64) *model.TableInfo

	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option.
	SetOption(opt Option, val interface{})
	// DelOption deletes an option.
	DelOption(opt Option)
	// GetOption gets an option.
	GetOption(opt Option) interface{}
	// GetMemBuffer return the MemBuffer binding to this unionStore.
	GetMemBuffer() MemBuffer
}

// AssertionType is the type of a assertion.
type AssertionType int

// The AssertionType constants.
const (
	None AssertionType = iota
	Exist
	NotExist
)
