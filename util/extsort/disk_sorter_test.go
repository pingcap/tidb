// Copyright 2023 PingCAP, Inc.
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

package extsort_test

import (
	"testing"

	"github.com/pingcap/tidb/util/extsort"
	"github.com/stretchr/testify/require"
)

func TestDiskSorter(t *testing.T) {
	sorter, err := extsort.OpenDiskSorter(t.TempDir(), &extsort.DiskSorterOptions{
		WriterBufferSize: 32 * 1024,
	})
	require.NoError(t, err)
	runCommonTest(t, sorter)
	require.NoError(t, sorter.Close())
}

func TestDiskSorterParallel(t *testing.T) {
	sorter, err := extsort.OpenDiskSorter(t.TempDir(), &extsort.DiskSorterOptions{
		WriterBufferSize: 32 * 1024,
	})
	require.NoError(t, err)
	runParallelTest(t, sorter)
	require.NoError(t, sorter.Close())
}
