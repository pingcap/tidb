// Copyright 2019 PingCAP, Inc.
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
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/stretchr/testify/require"
)

func TestMViewBuildReadTS(t *testing.T) {
	data, err := json.Marshal(&importer.TaskMeta{
		JobID: 42,
		Plan:  importer.Plan{QuerySource: &importer.QuerySource{ReadTS: 1234}},
	})
	require.NoError(t, err)
	readTS, err := mviewBuildReadTS(data)
	require.NoError(t, err)
	require.EqualValues(t, 1234, readTS)

	for _, meta := range []*importer.TaskMeta{
		{Plan: importer.Plan{QuerySource: &importer.QuerySource{ReadTS: 1234}}},
		{JobID: 42},
		{JobID: 42, Plan: importer.Plan{QuerySource: &importer.QuerySource{}}},
	} {
		data, err := json.Marshal(meta)
		require.NoError(t, err)
		_, err = mviewBuildReadTS(data)
		require.ErrorContains(t, err, "no read timestamp")
	}
	_, err = mviewBuildReadTS([]byte("{"))
	require.Error(t, err)
}
