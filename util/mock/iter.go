// Copyright 2021 PingCAP, Inc.
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

package mock

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/assert"
)

// SliceIter is used to iterate slice
type SliceIter struct {
	data []*kv.Entry
	cur  int
}

// NewSliceIter creates a new SliceIter
func NewSliceIter(data []*kv.Entry) *SliceIter {
	return &SliceIter{
		data,
		0,
	}
}

// GetSlice returns the inner slice
func (i *SliceIter) GetSlice() []*kv.Entry {
	return i.data
}

// Valid returns true if the current iterator is valid.
func (i *SliceIter) Valid() bool {
	return i.cur >= 0 && i.cur < len(i.data)
}

// Key returns the current key.
func (i *SliceIter) Key() kv.Key {
	if !i.Valid() {
		return nil
	}
	return i.data[i.cur].Key
}

// Value returns the current value.
func (i *SliceIter) Value() []byte {
	if !i.Valid() {
		return nil
	}
	return i.data[i.cur].Value
}

// Next goes the next position. Always return error for this iterator
func (i *SliceIter) Next() error {
	if !i.Valid() {
		return errors.New("iterator is invalid")
	}

	i.cur++
	return nil
}

// Close closes the iterator.
func (i *SliceIter) Close() {
	i.cur = -1
}

// MockedIter is a mocked iter for test
type MockedIter struct {
	kv.Iterator
	t                *testing.T
	nextErr          error
	closed           bool
	failOnMultiClose bool
}

// NewMockIterFromRecords creates a new MockedIter
func NewMockIterFromRecords(t *testing.T, records []*kv.Entry, failOnMultiClose bool) *MockedIter {
	return &MockedIter{
		t:                t,
		Iterator:         NewSliceIter(records),
		failOnMultiClose: failOnMultiClose,
	}
}

// InjectNextError injects error to its Next
func (i *MockedIter) InjectNextError(err error) {
	i.nextErr = err
}

// GetInjectedNextError get the injected error
func (i *MockedIter) GetInjectedNextError() error {
	return i.nextErr
}

// FailOnMultiClose set if should fail when Close is invoked more than once
func (i *MockedIter) FailOnMultiClose(fail bool) {
	i.failOnMultiClose = fail
}

// Next implements kv.Iterator.Next
func (i *MockedIter) Next() error {
	if i.nextErr != nil {
		return i.nextErr
	}
	return i.Iterator.Next()
}

// Close implements kv.Iterator.Close
func (i *MockedIter) Close() {
	if i.closed && i.failOnMultiClose {
		assert.FailNow(i.t, "Multi close iter")
	}
	i.closed = true
	i.Iterator.Close()
}

// Closed returns if the iter is closed
func (i *MockedIter) Closed() bool {
	return i.closed
}
