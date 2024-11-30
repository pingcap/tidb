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

func TestActionBDRMap(t *testing.T) {
	require.Equal(t, len(ActionMap), len(ActionBDRMap))

	totalActions := 0
	for bdrType, actions := range BDRActionMap {
		for _, action := range actions {
			require.Equal(t, bdrType, ActionBDRMap[action], "action %s", action)
		}
		totalActions += len(actions)
	}

	require.Equal(t, totalActions, len(ActionBDRMap))
}
