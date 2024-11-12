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

package cmp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompare(t *testing.T) {
	require.Equal(t, -1, Compare(1, 2))
	require.Equal(t, 1, Compare(2, 1))
	require.Zero(t, Compare(1, 1))

	require.Equal(t, -1, Compare("a", "b"))
	require.Equal(t, 1, Compare("b", "a"))
	require.Zero(t, Compare("a", "a"))
}
