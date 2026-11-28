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

package ddl_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func setupStarterFTSDDLTest(t *testing.T) {
	t.Helper()
	if !kerneltype.IsNextGen() {
		t.Skip("starter deploy mode is nextgen-only")
	}
	previous := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() { require.NoError(t, deploymode.Set(previous)) })
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`)
}

func TestFullTextIndexCreateTableEndToEnd(t *testing.T) {
	setupStarterFTSDDLTest(t)
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithMockTiFlash(2))
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	t.Cleanup(func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	})
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table docs (" +
		"id int primary key, " +
		"title varchar(200), " +
		"body text, " +
		"fulltext key ft_title (title), " +
		"fulltext index ft_body (body with (NGRAM(MIN_GRAM = 2, MAX_GRAM = 4), PATH_HIERARCHY(DELIMITER = '.'))), " +
		"fulltext (body))")

	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("docs"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), tbl.Meta().TiFlashReplica.Count)
	// Look indexes up by name so the assertions do not depend on declaration order
	// or on whether the int primary key is materialized as a PRIMARY index.
	ftTitle := tbl.Meta().FindIndexByName("ft_title")
	require.NotNil(t, ftTitle)
	require.Equal(t, ast.IndexTypeFulltext, ftTitle.Tp)
	require.Equal(t, ast.FullTextParserTypeStandardV1, ftTitle.FullTextInfo.ParserType)
	require.Len(t, ftTitle.FullTextInfo.IndexFields, 1)
	require.Equal(t, ast.FullTextParserTypeStandardV1, ftTitle.FullTextInfo.IndexFields[0].IndexFieldOption.AnalyzerType)

	ftBody := tbl.Meta().FindIndexByName("ft_body")
	require.NotNil(t, ftBody)
	require.Empty(t, ftBody.FullTextInfo.ParserType)
	require.Len(t, ftBody.FullTextInfo.IndexFields, 2)
	ngramField := ftBody.FullTextInfo.IndexFields[0].IndexFieldOption
	require.Equal(t, ast.FullTextParserTypeNgramV1, ngramField.AnalyzerType)
	require.Equal(t, uint64(2), ngramField.Ngram.MinGram)
	require.Equal(t, uint64(4), ngramField.Ngram.MaxGram)
	pathField := ftBody.FullTextInfo.IndexFields[1].IndexFieldOption
	require.Equal(t, ast.FullTextParserTypePathHierarchyV1, pathField.AnalyzerType)
	require.Equal(t, ".", pathField.PathHierarchy.Delimiter)

	// The anonymous constraint must be named after its first column.
	ftAnon := tbl.Meta().FindIndexByName("body")
	require.NotNil(t, ftAnon)
	require.Equal(t, ast.FullTextParserTypeStandardV1, ftAnon.FullTextInfo.ParserType)

	// SHOW CREATE TABLE must round-trip through the parser and recreate the same metadata.
	showCreate := tk.MustQuery("show create table docs").Rows()[0][1].(string)
	require.Contains(t, showCreate, "FULLTEXT INDEX `ft_title`(`title`) WITH PARSER STANDARD")
	require.Contains(t, showCreate, "WITH (NGRAM(MIN_GRAM = 2, MAX_GRAM = 4, GRANULARITY = 'word'), PATH_HIERARCHY(DELIMITER = '.'))")
	tk.MustExec("drop table docs")
	tk.MustExec(showCreate)
	tbl, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("docs"))
	require.NoError(t, err)
	rtBody := tbl.Meta().FindIndexByName("ft_body")
	require.NotNil(t, rtBody)
	require.Equal(t, ast.FullTextParserTypeNgramV1, rtBody.FullTextInfo.IndexFields[0].IndexFieldOption.AnalyzerType)
	rtNgram := rtBody.FullTextInfo.IndexFields[0].IndexFieldOption
	require.Equal(t, uint64(2), rtNgram.Ngram.MinGram)
	require.Equal(t, uint64(4), rtNgram.Ngram.MaxGram)
	rtPath := rtBody.FullTextInfo.IndexFields[1].IndexFieldOption
	require.Equal(t, ".", rtPath.PathHierarchy.Delimiter)
}

func TestFullTextIndexAddAndCreateEndToEnd(t *testing.T) {
	setupStarterFTSDDLTest(t)
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithMockTiFlash(2))
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	t.Cleanup(func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	})
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (id int primary key, title varchar(200), body text)")
	tk.MustContainErrMsg("alter table t add fulltext index ft_body (body)", "columnar replica must exist")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("alter table t add fulltext index ft_body (body)")
	tk.MustExec("create fulltext index ft_title on t (title with (MULTILINGUAL))")
	tk.MustExec("create fulltext index ft_multi on t (title, body)")

	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	ftBody := tbl.Meta().FindIndexByName("ft_body")
	require.NotNil(t, ftBody)
	require.Equal(t, ast.IndexTypeFulltext, ftBody.Tp)
	require.Equal(t, ast.FullTextParserTypeStandardV1, ftBody.FullTextInfo.ParserType)
	require.Len(t, ftBody.FullTextInfo.IndexFields, 1)

	ftTitle := tbl.Meta().FindIndexByName("ft_title")
	require.NotNil(t, ftTitle)
	require.Empty(t, ftTitle.FullTextInfo.ParserType)
	require.Equal(t, ast.FullTextParserTypeMultilingualV1, ftTitle.FullTextInfo.IndexFields[0].IndexFieldOption.AnalyzerType)

	ftMulti := tbl.Meta().FindIndexByName("ft_multi")
	require.NotNil(t, ftMulti)
	require.Len(t, ftMulti.Columns, 2)
	require.Equal(t, ast.FullTextParserTypeStandardV1, ftMulti.FullTextInfo.ParserType)
	require.Len(t, ftMulti.FullTextInfo.IndexFields, 2)

	// DDL-layer parser option validation must surface a typed ddl error code.
	tk.MustGetErrCode("create fulltext index ft_bad on t (body) with parser NGRAM(MAX_GRAM = 9)", errno.ErrUnsupportedDDLOperation)
	tk.MustContainErrMsg("create fulltext index ft_bad on t (body) with parser NGRAM(MAX_GRAM = 9)",
		"MAX_GRAM must be less than or equal to 5")
	// Unknown parser names are rejected by the preprocessor.
	tk.MustContainErrMsg("create fulltext index ft_bad on t (body) with parser foo", "Unsupported parser 'foo'")
	// Column-level unknown parser names are also rejected by the preprocessor.
	tk.MustContainErrMsg("create fulltext index ft_bad on t (body with (foo))", "Unsupported parser 'foo'")
}
