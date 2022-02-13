// Copyright 2020 PingCAP, Inc.
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

package common_test

import (
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/stretchr/testify/require"
)

func TestGetStorageSize(t *testing.T) {
	// only ensure we can get storage size.
	d := t.TempDir()
	size, err := common.GetStorageSize(d)
	require.NoError(t, err)
	require.Greater(t, size.Capacity, uint64(0))
	require.Greater(t, size.Available, uint64(0))
}
