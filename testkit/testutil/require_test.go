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

package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestCompareUnorderedString(t *testing.T) {
	defer goleak.VerifyNone(t)
	require.True(t, CompareUnorderedStringSlice([]string{"1", "1", "2"}, []string{"1", "1", "2"}))
	require.True(t, CompareUnorderedStringSlice([]string{"1", "1", "2"}, []string{"1", "2", "1"}))
	require.False(t, CompareUnorderedStringSlice([]string{"1", "1"}, []string{"1", "2", "1"}))
	require.False(t, CompareUnorderedStringSlice([]string{"1", "1", "2"}, []string{"1", "2", "2"}))
	require.True(t, CompareUnorderedStringSlice(nil, nil))
	require.False(t, CompareUnorderedStringSlice([]string{}, nil))
	require.False(t, CompareUnorderedStringSlice(nil, []string{}))
}
