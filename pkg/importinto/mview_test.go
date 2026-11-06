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

package importinto_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/importinto"
	"github.com/stretchr/testify/require"
)

func TestMViewBuildReadTS(t *testing.T) {
	readTS, err := importinto.MViewBuildReadTS([]byte(`{"DDLJobID":42,"Plan":{"QuerySource":{"ReadTS":1234}}}`))
	require.NoError(t, err)
	require.Equal(t, uint64(1234), readTS)
	for _, data := range []string{
		`invalid`,
		`null`,
		`{}`,
		`{"Plan":{"QuerySource":{"ReadTS":1234}}}`,
		`{"DDLJobID":42}`,
		`{"DDLJobID":42,"Plan":{"QuerySource":null}}`,
		`{"DDLJobID":42,"Plan":{"QuerySource":{"ReadTS":0}}}`,
		`{"DDLJobID":42,"Plan":{"QuerySource":{"ReadTS":"invalid"}}}`,
	} {
		t.Run(data, func(t *testing.T) {
			_, err := importinto.MViewBuildReadTS([]byte(data))
			require.Error(t, err)
		})
	}
}
