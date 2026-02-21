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

package model

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMLflowModelSignature(t *testing.T) {
	path := filepath.Join("testdata", "mlflow", "minimal", "MLmodel")
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	info, err := ParseMLflowModel(data)
	require.NoError(t, err)
	require.True(t, info.HasPyFunc)
	require.Equal(t, []string{"x"}, info.InputNames)
	require.Equal(t, []string{"y"}, info.OutputNames)
}
