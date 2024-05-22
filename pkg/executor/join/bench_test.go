// Copyright 2024 PingCAP, Inc.
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

package join

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util"
)

func BenchmarkHashTableBuild(b *testing.B) {
	b.StopTimer()
	rowTable, err := createRowTable(3000000)
	if err != nil {
		b.Fatal(err)
	}
	subTable := newSubTable(rowTable)
	segmentCount := len(rowTable.segments)
	b.StartTimer()
	subTable.build(0, segmentCount)
}

func BenchmarkHashTableConcurrentBuild(b *testing.B) {
	b.StopTimer()
	rowTable, err := createRowTable(3000000)
	if err != nil {
		b.Fatal(err)
	}
	subTable := newSubTable(rowTable)
	segmentCount := len(rowTable.segments)
	buildThreads := 3
	wg := util.WaitGroupWrapper{}
	b.StartTimer()
	for i := 0; i < buildThreads; i++ {
		segmentStart := segmentCount / buildThreads * i
		segmentEnd := segmentCount / buildThreads * (i + 1)
		if i == buildThreads-1 {
			segmentEnd = segmentCount
		}
		wg.Run(func() {
			subTable.build(segmentStart, segmentEnd)
		})
	}
	wg.Wait()
}
