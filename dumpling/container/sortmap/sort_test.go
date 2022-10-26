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
// See the License for the specific language governing permissions and
// limitations under the License.

package sortmap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	m := map[string]string{
		"1":  "1",
		"2":  "2",
		"3":  "3",
		"10": "10",
	}

	expected := []Pair[string, string]{
		{"1", "1"},
		{"10", "10"},
		{"2", "2"},
		{"3", "3"},
	}

	require.Equal(t, expected, Sort(m))
}
