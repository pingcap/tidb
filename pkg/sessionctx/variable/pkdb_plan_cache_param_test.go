// Copyright 2026 PingCAP, Inc.
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

package variable

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestPlanCacheParamListInvalidZeroTemporal(t *testing.T) {
	params := NewPlanCacheParamList()
	params.Append(types.NewTimeDatum(types.NewTime(types.ZeroCoreTime, mysql.TypeDatetime, types.DefaultFsp)))
	require.True(t, params.HasInvalidZeroTemporal())

	params.Reset()
	require.False(t, params.HasInvalidZeroTemporal())

	params.Append(types.NewTimeDatum(types.NewTime(types.FromDate(2024, 1, 2, 0, 0, 0, 0), mysql.TypeDatetime, types.DefaultFsp)))
	require.False(t, params.HasInvalidZeroTemporal())
}
