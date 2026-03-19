// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestShouldCheckAssumedServer(t *testing.T) {
	if kerneltype.IsClassic() {
		require.False(t, shouldCheckAssumedServer(&model.Job{TableID: 100}))
		require.False(t, shouldCheckAssumedServer(&model.Job{TableID: metadef.ReservedGlobalIDUpperBound}))
	} else {
		require.False(t, shouldCheckAssumedServer(&model.Job{TableID: 100}))
		require.True(t, shouldCheckAssumedServer(&model.Job{TableID: metadef.ReservedGlobalIDUpperBound}))
	}
}
