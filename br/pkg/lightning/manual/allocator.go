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

package manual

import (
	"fmt"

	"go.uber.org/atomic"
)

type Allocator struct {
	RefCnt *atomic.Int64
}

func (a Allocator) Alloc(n int) []byte {
	if a.RefCnt != nil {
		a.RefCnt.Add(1)
	}
	return New(n)
}

func (a Allocator) Free(b []byte) {
	if a.RefCnt != nil {
		a.RefCnt.Add(-1)
	}
	Free(b)
}

func (a Allocator) CheckRefCnt() error {
	if a.RefCnt != nil && a.RefCnt.Load() != 0 {
		return fmt.Errorf("memory leak detected, refCnt: %d", a.RefCnt.Load())
	}
	return nil
}
