// Copyright 2021 PingCAP, Inc.
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

package slice

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlice(t *testing.T) {
	tests := []struct {
		a     []int
		allOf bool
	}{
		{[]int{}, true},
		{[]int{1, 2, 3}, false},
		{[]int{1, 3}, false},
		{[]int{2, 2, 4}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.a), func(t *testing.T) {
			even := func(val int) bool { return val%2 == 0 }
			require.Equal(t, test.allOf, AllOf(test.a, even))
		})
	}
}
