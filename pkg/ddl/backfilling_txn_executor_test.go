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

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
)

func TestExpectedIngestWorkerCntNextgen(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("only for nextgen")
	}
	tests := []struct {
		concurrency int
		avgRowSize  int
		expReader   int
		expWriter   int
	}{
		{10, 100, 10, 10},
		{20, 500, 20, 20},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("concurrency%d_avgRowSize%d", tt.concurrency, tt.avgRowSize), func(t *testing.T) {
			reader, writer := expectedIngestWorkerCnt(tt.concurrency, tt.avgRowSize)
			require.Equal(t, tt.expReader, reader, "concurrency: %d, avgRowSize: %d", tt.concurrency, tt.avgRowSize)
			require.Equal(t, tt.expWriter, writer, "concurrency: %d, avgRowSize: %d", tt.concurrency, tt.avgRowSize)
		})
	}
}

func TestExpectedIngestWorkerCntClassic(t *testing.T) {
	if kerneltype.IsNextGen() {
		t.Skip("only for classic")
	}
	tests := []struct {
		concurrency int
		avgRowSize  int
		expReader   int
		expWriter   int
	}{
		// Non-nextgen path, avgRowSize = 0
		{10, 0, 5, 7},
		{40, 0, 16, 16},
		{1, 0, 1, 2},

		// Non-nextgen path, various avgRowSize
		{10, 100, 5, 10},
		{10, 300, 10, 10},
		{10, 600, 20, 10},
		{10, 2000, 40, 10},
		{10, 5000, 80, 10},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("concurrency%d_avgRowSize%d", tt.concurrency, tt.avgRowSize), func(t *testing.T) {
			reader, writer := expectedIngestWorkerCnt(tt.concurrency, tt.avgRowSize)
			require.Equal(t, tt.expReader, reader, "concurrency: %d, avgRowSize: %d", tt.concurrency, tt.avgRowSize)
			require.Equal(t, tt.expWriter, writer, "concurrency: %d, avgRowSize: %d", tt.concurrency, tt.avgRowSize)
		})
	}
}
