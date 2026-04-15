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

func TestCharsetCompareUsesFamily(t *testing.T) {
	// Ensure Compare only depends on the normalized kind, not the original input string.
	cmp, err := Charset("UTF8").Compare(Charset("utf8"))
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	cmp, err = Charset("UTF8MB3").Compare(Charset("utf8"))
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	// Ensure error messages keep the original values.
	_, err = Charset("uTF8").Compare(Charset("GBK"))
	require.ErrorContains(t, err, "incompatible mysql charset (uTF8 vs GBK)")

	cmp, err = Charset("latin1").Compare(Charset("utf8mb4"))
	require.NoError(t, err)
	require.Equal(t, -1, cmp)

	cmp, err = Charset("utf8mb4").Compare(Charset("utf8mb3"))
	require.NoError(t, err)
	require.Equal(t, 1, cmp)

	_, err = Charset("other1").Compare(Charset("other2"))
	require.ErrorContains(t, err, "incompatible mysql charset (other1 vs other2)")
}

func TestCollationCompareUsesFamily(t *testing.T) {
	// Ensure Compare only depends on the normalized kind, not the original input string.
	cmp, err := Collation("UTF8_BIN").Compare(Collation("utf8_bin"))
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	cmp, err = Collation("UTF8MB3_BIN").Compare(Collation("utf8_bin"))
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	cmp, err = Collation("LATIN1_BIN").Compare(Collation("utf8mb4_bin"))
	require.NoError(t, err)
	require.Equal(t, -1, cmp)

	// Ensure error messages keep the original values.
	_, err = Collation("UTF8_BIN").Compare(Collation("GBK_BIN"))
	require.ErrorContains(t, err, "incompatible mysql collation (UTF8_BIN vs GBK_BIN)")

	cmp, err = Collation("utf8mb4_general_ci").Compare(Collation("utf8_general_ci"))
	require.NoError(t, err)
	require.Equal(t, 1, cmp)

	_, err = Collation("utf8mb4_general_ci").Compare(Collation("utf8mb4_0900_ai_ci"))
	require.ErrorContains(t, err, "incompatible mysql collation (utf8mb4_general_ci vs utf8mb4_0900_ai_ci)")

	_, err = Collation("other_cs_bin").Compare(Collation("other_cs_ci"))
	require.ErrorContains(t, err, "incompatible mysql collation (other_cs_bin vs other_cs_ci)")
}
