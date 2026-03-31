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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package printer

import (
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestPrintResult(t *testing.T) {
	cols := []string{"col1", "col2", "col3"}
	datas := [][]string{{"11"}, {"21", "22", "23"}}
	result, ok := GetPrintResult(cols, datas)
	require.False(t, ok)
	require.Equal(t, "", result)

	datas = [][]string{{"11", "12", "13"}, {"21", "22", "23"}}
	expect := `
+------+------+------+
| col1 | col2 | col3 |
+------+------+------+
| 11   | 12   | 13   |
| 21   | 22   | 23   |
+------+------+------+
`
	result, ok = GetPrintResult(cols, datas)
	require.True(t, ok)
	require.Equal(t, expect[1:], result)

	datas = nil
	result, ok = GetPrintResult(cols, datas)
	require.False(t, ok)
	require.Equal(t, "", result)

	cols = nil
	result, ok = GetPrintResult(cols, datas)
	require.False(t, ok)
	require.Equal(t, "", result)
}

func TestGetTiDBInfo(t *testing.T) {
	info := GetTiDBInfo()
	if kerneltype.IsNextGen() {
		require.Contains(t, info, "\nKernel Type: Next Generation")
		expectedReleaseVersion, err := mysql.BuildTiDBXReleaseVersion(mysql.NormalizeTiDBReleaseVersionForNextGen(mysql.TiDBReleaseVersion))
		require.NoError(t, err)
		require.Contains(t, info, "Release Version: "+expectedReleaseVersion)
	} else {
		require.Contains(t, info, "\nKernel Type: Classic")
		require.Contains(t, info, "Release Version: "+mysql.TiDBReleaseVersion)
	}
	require.NotContains(t, info, "TiDB Component Version:")
}

func TestPrintTiDBInfo(t *testing.T) {
	core, recorded := observer.New(zap.InfoLevel)
	restore := log.ReplaceGlobals(
		zap.New(core),
		&log.ZapProperties{
			Core:  core,
			Level: zap.NewAtomicLevelAt(zap.InfoLevel),
		},
	)
	defer restore()

	PrintTiDBInfo()
	entries := recorded.FilterMessage("Welcome to TiDB.").All()
	require.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	if kerneltype.IsNextGen() {
		require.Equal(t, mysql.NormalizeTiDBReleaseVersionForNextGen(mysql.TiDBReleaseVersion), fields["TiDB Component Version"])
	} else {
		_, ok := fields["TiDB Component Version"]
		require.False(t, ok)
	}
}
