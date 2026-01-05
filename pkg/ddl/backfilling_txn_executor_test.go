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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestExpectedIngestWorkerCnt(t *testing.T) {
	tests := []struct {
		concurrency int
		avgRowSize  int
		isNextgen   bool
		expReader   int
		expWriter   int
	}{
		// Nextgen path
		{10, 100, true, 10, 10},
		{20, 500, true, 20, 20},

		// Non-nextgen path, avgRowSize = 0
		{10, 0, false, 5, 7},
		{40, 0, false, 16, 16},
		{1, 0, false, 1, 2},

		// Non-nextgen path, various avgRowSize
		{10, 100, false, 5, 10},
		{10, 300, false, 10, 10},
		{10, 600, false, 20, 10},
		{10, 2000, false, 40, 10},
		{10, 5000, false, 80, 10},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("concurrency%d_avgRowSize%d_isNextgen%v", tt.concurrency, tt.avgRowSize, tt.isNextgen), func(t *testing.T) {
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/expectedIngestWorkerCnt", func(isNextgen *bool) {
				*isNextgen = tt.isNextgen
			})
			reader, writer := expectedIngestWorkerCnt(tt.concurrency, tt.avgRowSize)
			require.Equal(t, tt.expReader, reader, "concurrency: %d, avgRowSize: %d, isNextgen: %v", tt.concurrency, tt.avgRowSize, tt.isNextgen)
			require.Equal(t, tt.expWriter, writer, "concurrency: %d, avgRowSize: %d, isNextgen: %v", tt.concurrency, tt.avgRowSize, tt.isNextgen)
		})
	}
}
