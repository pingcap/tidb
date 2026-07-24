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

package aggfuncs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCalculateMergeKeepsIdenticalMeanVarianceZero(t *testing.T) {
	const input = 110236466.0

	variance := calculateMerge(1, 49, input, input*49, 0, 0)

	require.Zero(t, variance)
}
