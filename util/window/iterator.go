// Copyright 2022 PingCAP, Inc.
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

package window

import (
	"fmt"

	"golang.org/x/exp/constraints"
)

// Iterator iterates the buckets within the window.
type Iterator[T constraints.Integer | constraints.Float] struct {
	count         int
	iteratedCount int
	cur           *Bucket[T]
}

// Next returns true util all of the buckets has been iterated.
func (i *Iterator[T]) Next() bool {
	return i.count != i.iteratedCount
}

// Bucket gets current bucket.
func (i *Iterator[T]) Bucket() Bucket[T] {
	if !(i.Next()) {
		panic(fmt.Errorf("stat/metric: iteration out of range iteratedCount: %d count: %d", i.iteratedCount, i.count))
	}
	bucket := *i.cur
	i.iteratedCount++
	i.cur = i.cur.Next()
	return bucket
}
