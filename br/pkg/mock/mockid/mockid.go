// Copyright 2019 TiKV Project Authors.
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

package mockid

import "sync/atomic"

// IDAllocator mocks IDAllocator and it is only used for test.
type IDAllocator struct {
	base uint64
}

// NewIDAllocator creates a new IDAllocator.
func NewIDAllocator() *IDAllocator {
	return &IDAllocator{base: 0}
}

// Alloc returns a new id.
func (alloc *IDAllocator) Alloc() (uint64, error) {
	return atomic.AddUint64(&alloc.base, 1), nil
}

// Rebase implements the IDAllocator interface.
func (alloc *IDAllocator) Rebase() error {
	return nil
}
