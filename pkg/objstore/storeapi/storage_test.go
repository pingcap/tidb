// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storeapi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrefix(t *testing.T) {
	require.EqualValues(t, "", NewPrefix(""))
	for _, c := range []string{"dir", "/dir", "dir/", "/dir/"} {
		require.EqualValues(t, "dir/", NewPrefix(c))
	}
	require.EqualValues(t, "dir/sub/sub2/sub3/", NewPrefix("/dir/sub/sub2/sub3/"))
	prefix := NewPrefix("")
	require.EqualValues(t, "", prefix.JoinStr(""))
	for _, c := range []string{"dir", "/dir", "dir/", "/dir/"} {
		require.EqualValues(t, "dir/", prefix.JoinStr(c))
	}
	prefix = NewPrefix("/parent")
	require.EqualValues(t, "parent/", prefix.JoinStr(""))
	for _, c := range []string{"dir", "/dir", "dir/", "/dir/"} {
		require.EqualValues(t, "parent/dir/", prefix.JoinStr(c))
	}
	require.EqualValues(t, "parent/file.txt", prefix.ObjectKey("file.txt"))
	require.EqualValues(t, "parent//file.txt", prefix.ObjectKey("/file.txt"))
}

func TestGetHTTPRange(t *testing.T) {
	full, val := GetHTTPRange(0, 0)
	require.True(t, full)
	require.Empty(t, val)

	full, val = GetHTTPRange(0, 100)
	require.False(t, full)
	require.EqualValues(t, "bytes=0-99", val)

	full, val = GetHTTPRange(50, 100)
	require.False(t, full)
	require.EqualValues(t, "bytes=50-99", val)

	full, val = GetHTTPRange(50, 0)
	require.False(t, full)
	require.EqualValues(t, "bytes=50-", val)
}
