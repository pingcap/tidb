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

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetOrDecodeArgsV2(t *testing.T) {
	j := &Job{
		Version: JobVersion2,
		Type:    ActionTruncateTable,
		ArgsV2: &TruncateTableArgs{
			FKCheck: true,
		},
	}
	_, err := j.Encode(true)
	require.NoError(t, err)
	require.NotNil(t, j.RawArgsV2)
	// return existing argsV2
	argsV2, err := GetOrDecodeArgsV2[TruncateTableArgs](j)
	require.NoError(t, err)
	require.Same(t, j.ArgsV2, argsV2)
	// unmarshal from json
	var argsBak *TruncateTableArgs
	argsBak, j.ArgsV2 = j.ArgsV2.(*TruncateTableArgs), nil
	argsV2, err = GetOrDecodeArgsV2[TruncateTableArgs](j)
	require.NoError(t, err)
	require.NotNil(t, argsV2)
	require.NotSame(t, argsBak, argsV2)
}
