// Copyright 2015 PingCAP, Inc.
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

package ast

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestT(t *testing.T) {
	abc := NewCIStr("aBC")
	require.Equal(t, "aBC", abc.O)
	require.Equal(t, "abc", abc.L)
	require.Equal(t, "aBC", abc.String())
}

func TestUnmarshalCIStr(t *testing.T) {
	var ci CIStr

	// Test unmarshal CIStr from a single string.
	str := "aaBB"
	buf, err := json.Marshal(str)
	require.NoError(t, err)
	require.NoError(t, ci.UnmarshalJSON(buf))
	require.Equal(t, str, ci.O)
	require.Equal(t, "aabb", ci.L)

	buf, err = json.Marshal(ci)
	require.NoError(t, err)
	require.Equal(t, `{"O":"aaBB","L":"aabb"}`, string(buf))
	require.NoError(t, ci.UnmarshalJSON(buf))
	require.Equal(t, str, ci.O)
	require.Equal(t, "aabb", ci.L)
}
