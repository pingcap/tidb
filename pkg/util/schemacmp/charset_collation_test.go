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

package schemacmp_test

import (
	"testing"

	. "github.com/pingcap/tidb/pkg/util/schemacmp"
	"github.com/stretchr/testify/require"
)

func TestCharsetCompare(t *testing.T) {
	// Ensure normalization makes comparisons case-insensitive.
	cmp, err := Charset("UTF8").Compare(Charset("utf8"))
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	cmp, err = Charset("UTF8MB3").Compare(Charset("utf8"))
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	_, err = Charset("uTF8").Compare(Charset("GBK"))
	require.ErrorContains(t, err, "incompatible charset (utf8 vs gbk)")

	cmp, err = Charset("latin1").Compare(Charset("utf8mb4"))
	require.NoError(t, err)
	require.Equal(t, -1, cmp)

	cmp, err = Charset("utf8mb4").Compare(Charset("utf8mb3"))
	require.NoError(t, err)
	require.Equal(t, 1, cmp)

	_, err = Charset("other1").Compare(Charset("other2"))
	require.ErrorContains(t, err, "incompatible charset (other1 vs other2)")

	_, err = Charset("other1").Compare(Charset("utf8"))
	require.ErrorContains(t, err, "incompatible charset (other1 vs utf8)")
}

func TestCollationCompare(t *testing.T) {
	// Ensure Compare only depends on the normalized kind, not the original input string.
	cmp, err := Collation("UTF8_BIN").Compare(Collation("utf8_bin"))
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	// Collations without a suffix (no underscore) should also compare correctly.
	cmp, err = Collation("binary").Compare(Collation("BINARY"))
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	// binary collation is different with other charset's _bin collation
	_, err = Collation("binary").Compare(Collation("utf8mb4_bin"))
	require.ErrorContains(t, err, "incompatible collation (binary vs utf8mb4_bin)")

	cmp, err = Collation("UTF8MB3_BIN").Compare(Collation("utf8_bin"))
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	cmp, err = Collation("LATIN1_BIN").Compare(Collation("utf8mb4_bin"))
	require.NoError(t, err)
	require.Equal(t, -1, cmp)

	_, err = Collation("UTF8_BIN").Compare(Collation("GBK_BIN"))
	// note the error message is charset because collation suffix is the same
	require.ErrorContains(t, err, "incompatible charset (utf8 vs gbk)")

	cmp, err = Collation("utf8mb4_general_ci").Compare(Collation("utf8_general_ci"))
	require.NoError(t, err)
	require.Equal(t, 1, cmp)

	_, err = Collation("utf8mb4_general_ci").Compare(Collation("utf8mb4_0900_ai_ci"))
	require.ErrorContains(t, err, "incompatible collation (utf8mb4_general_ci vs utf8mb4_0900_ai_ci)")

	_, err = Collation("other_cs_bin").Compare(Collation("other_cs_ci"))
	require.ErrorContains(t, err, "incompatible collation (other_cs_bin vs other_cs_ci)")

	// special fallback cases, where collation is not set and charset is known to TiDB
	_, err = Collation("unknowCS").Compare(Collation("unknowCS2"))
	require.ErrorContains(t, err, "incompatible charset (unknowcs vs unknowcs2)")

	cmp, err = Collation("unknowCS").Compare(Collation("unknowCS"))
	require.NoError(t, err)
	require.Equal(t, 0, cmp)
}

func TestCharsetJoin(t *testing.T) {
	join, err := Charset("utf8").Join(Charset("latin1"))
	require.NoError(t, err)
	require.Equal(t, "utf8mb4", join.Unwrap())

	join, err = Charset("latin1").Join(Charset("utf8mb3"))
	require.NoError(t, err)
	require.Equal(t, "utf8mb4", join.Unwrap())
}

func TestCollationJoin(t *testing.T) {
	join, err := Collation("utf8_bin").Join(Collation("latin1_bin"))
	require.NoError(t, err)
	require.Equal(t, "utf8mb4_bin", join.Unwrap())

	join, err = Collation("latin1_general_cs").Join(Collation("utf8_general_cs"))
	require.NoError(t, err)
	require.Equal(t, "utf8mb4_general_cs", join.Unwrap())
}
