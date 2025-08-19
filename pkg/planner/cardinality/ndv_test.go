// Copyright 2025 PingCAP, Inc.
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

package cardinality

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScaleNDV(t *testing.T) {
	type TestCase struct {
		OriginalNDV  float64
		OriginalRows float64
		SelectedRows float64
		NewNDV       float64
	}
	cases := []TestCase{
		{0, 0, 0, 0},
		{10, 0, 100, 0},
		{10, 100, 100, 10},
		{10, 100, 1, 1},
		{10, 100, 2, 1.83},
		{10, 100, 10, 6.51},
		{10, 100, 50, 9.99},
		{10, 100, 80, 10.00},
		{10, 100, 90, 10.00},
	}
	for _, tc := range cases {
		newNDV := ScaleNDV(tc.OriginalNDV, tc.OriginalRows, tc.SelectedRows)
		require.Equal(t, fmt.Sprintf("%.2f", tc.NewNDV), fmt.Sprintf("%.2f", newNDV), tc)
	}
}
