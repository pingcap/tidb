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

package modelruntime

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsCustomOpError(t *testing.T) {
	cases := []string{
		"no op registered for CustomFoo",
		"failed to find custom op",
		"Custom op domain is not registered",
		"no schema registered for op: CustomBar",
	}
	for _, msg := range cases {
		require.True(t, IsCustomOpError(errors.New(msg)))
	}
	require.False(t, IsCustomOpError(errors.New("random failure")))
}
