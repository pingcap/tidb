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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/stretchr/testify/require"
)

func TestPublicFTSIndexOnColumns(t *testing.T) {
	title := pmodel.NewCIStr("title")
	body := pmodel.NewCIStr("body")
	titleIndex := &model.IndexInfo{
		State:        model.StatePublic,
		Columns:      []*model.IndexColumn{{Name: title}},
		FullTextInfo: &model.FullTextIndexInfo{ParserType: model.FullTextParserTypeStandardV1},
	}
	bodyIndex := &model.IndexInfo{
		State:        model.StatePublic,
		Columns:      []*model.IndexColumn{{Name: body}},
		FullTextInfo: &model.FullTextIndexInfo{ParserType: model.FullTextParserTypeStandardV1},
	}
	unsupportedIndex := &model.IndexInfo{
		State: model.StatePublic,
		Columns: []*model.IndexColumn{
			{Name: title},
			{Name: body},
		},
		FullTextInfo: &model.FullTextIndexInfo{ParserType: model.FullTextParserTypeMultilingualV1},
	}
	compositeIndex := &model.IndexInfo{
		State: model.StatePublic,
		Columns: []*model.IndexColumn{
			{Name: title},
			{Name: body},
		},
		FullTextInfo: &model.FullTextIndexInfo{ParserType: model.FullTextParserTypeNgramV1},
	}
	tblInfo := &model.TableInfo{Indices: []*model.IndexInfo{titleIndex, bodyIndex, unsupportedIndex, compositeIndex}}

	require.Same(t, compositeIndex, publicFTSIndexOnColumns(tblInfo, []pmodel.CIStr{title, body}, true))
	require.Nil(t, publicFTSIndexOnColumns(tblInfo, []pmodel.CIStr{body, title}, true))
	require.Same(t, unsupportedIndex, publicFTSIndexOnColumns(tblInfo, []pmodel.CIStr{title, body}, false))
	require.Same(t, titleIndex, publicFTSIndexOnColumns(tblInfo, []pmodel.CIStr{title}, true))
	require.Nil(t, publicFTSIndexOnColumns(tblInfo, []pmodel.CIStr{title, pmodel.NewCIStr("missing")}, true))
	require.Nil(t, publicFTSIndexOnColumns(nil, []pmodel.CIStr{title, body}, true))
}

func TestFindMatchingFullTextIndexForMultiColumnMatch(t *testing.T) {
	title := pmodel.NewCIStr("title")
	body := pmodel.NewCIStr("body")
	compositeIndex := &model.IndexInfo{
		State: model.StatePublic,
		Columns: []*model.IndexColumn{
			{Name: title, Offset: 0},
			{Name: body, Offset: 1},
		},
		FullTextInfo: &model.FullTextIndexInfo{ParserType: model.FullTextParserTypeNgramV1},
	}
	tblInfo := &model.TableInfo{
		Columns: []*model.ColumnInfo{{ID: 1, Name: title}, {ID: 2, Name: body}},
		Indices: []*model.IndexInfo{compositeIndex},
	}
	ds := &logicalop.DataSource{TableInfo: tblInfo}
	ftsInfo := &expression.FTSInfo{
		IsMatchAgainst: true,
		Columns: []*expression.Column{
			{ID: 1, OrigName: title.O},
			{ID: 2, OrigName: body.O},
		},
	}

	require.Same(t, compositeIndex, findMatchingFullTextIndex(ds, ftsInfo))
	ftsInfo.Columns[0], ftsInfo.Columns[1] = ftsInfo.Columns[1], ftsInfo.Columns[0]
	require.Nil(t, findMatchingFullTextIndex(ds, ftsInfo))
}
