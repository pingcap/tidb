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

// UnionStore is a store that wraps a snapshot for read and a MemBuffer for buffered write.
// Also, it provides some transaction related utilities.
type UnionStore interface {
	Retriever

	// HasPresumeKeyNotExists returns whether the key presumed key not exists error for the lazy check.
	HasPresumeKeyNotExists(k Key) bool
	// UnmarkPresumeKeyNotExists deletes the key presume key not exists error flag for the lazy check.
	UnmarkPresumeKeyNotExists(k Key)

	// SetOption sets an option with a value, when val is nil, uses the default
	// value of this option.
	SetOption(opt int, val interface{})
	// DelOption deletes an option.
	DelOption(opt int)
	// GetOption gets an option.
	GetOption(opt int) interface{}
	// GetMemBuffer return the MemBuffer binding to this unionStore.
	GetMemBuffer() MemBuffer
}
