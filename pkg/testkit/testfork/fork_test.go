// Copyright 2022 PingCAP, Inc.
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

package testfork

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForkSubTest(t *testing.T) {
	var values [][]any
	RunTest(t, func(t *T) {
		x := Pick(t, []int{1, 2, 3})
		y := PickEnum(t, "a", "b")
		var z any
		if x == 2 {
			z = PickEnum(t, 10, 11)
		} else {
			z = Pick(t, []string{"g", "h"})
		}
		values = append(values, []any{x, y, z})
	})
	require.Equal(t, [][]any{
		{1, "a", "g"},
		{1, "a", "h"},
		{1, "b", "g"},
		{1, "b", "h"},
		{2, "a", 10},
		{2, "a", 11},
		{2, "b", 10},
		{2, "b", 11},
		{3, "a", "g"},
		{3, "a", "h"},
		{3, "b", "g"},
		{3, "b", "h"},
	}, values)
}
