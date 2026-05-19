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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreparePluginModFileWithTiDBReplaces(t *testing.T) {
	tempDir := t.TempDir()
	tidbDir := filepath.Join(tempDir, "tidb")
	pkgDir := filepath.Join(tempDir, "enterprise-plugin", "whitelist")
	require.NoError(t, os.MkdirAll(filepath.Join(tidbDir, "pkg", "parser"), 0o755))
	require.NoError(t, os.MkdirAll(pkgDir, 0o755))

	require.NoError(t, os.WriteFile(filepath.Join(tidbDir, "go.mod"), []byte(`
module github.com/pingcap/tidb

go 1.25.9

replace (
	github.com/pingcap/tidb => ../tidb
	github.com/pingcap/tidb/pkg/parser => ./pkg/parser
	github.com/pingcap/kvproto => github.com/YuhaoZhang00/kvproto v0.0.0-20260514083546-6392d3ef7006
	github.com/tikv/client-go/v2 => github.com/YuhaoZhang00/client-go/v2 v2.0.8-0.20260514081829-33af58c5fb00
	github.com/tikv/pd/client => github.com/YuhaoZhang00/pd/client v0.0.0-20260514081800-e393a2130e70
)
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(pkgDir, "go.mod"), []byte(`
module github.com/pingcap/enterprise-plugin/whitelist

go 1.25.8

require github.com/pingcap/tidb v0.0.0

replace github.com/pingcap/tidb => ../../tidb
`), 0o644))

	modFile, cleanup, err := preparePluginModFile(pkgDir, tidbDir)
	require.NoError(t, err)
	defer cleanup()
	require.NotEmpty(t, modFile)

	contentBytes, err := os.ReadFile(modFile)
	require.NoError(t, err)
	content := string(contentBytes)
	require.Contains(t, content, "github.com/pingcap/tidb => ../../tidb")
	require.Contains(t, content, "github.com/pingcap/kvproto => github.com/YuhaoZhang00/kvproto v0.0.0-20260514083546-6392d3ef7006")
	require.Contains(t, content, "github.com/tikv/client-go/v2 => github.com/YuhaoZhang00/client-go/v2 v2.0.8-0.20260514081829-33af58c5fb00")
	require.Contains(t, content, "github.com/tikv/pd/client => github.com/YuhaoZhang00/pd/client v0.0.0-20260514081800-e393a2130e70")
	require.Contains(t, content, "github.com/pingcap/tidb/pkg/parser => "+filepath.Join(tidbDir, "pkg", "parser"))
	require.False(t, strings.Contains(content, "github.com/pingcap/tidb => ../tidb"))
}
