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

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
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
	nonPublicIndex := &model.IndexInfo{
		State: model.StateWriteOnly,
		Columns: []*model.IndexColumn{
			{Name: title},
			{Name: body},
		},
		FullTextInfo: &model.FullTextIndexInfo{ParserType: model.FullTextParserTypeNgramV1},
	}
	compositeIndex := &model.IndexInfo{
		State: model.StatePublic,
		Columns: []*model.IndexColumn{
			{Name: title},
			{Name: body},
		},
		FullTextInfo: &model.FullTextIndexInfo{ParserType: model.FullTextParserTypeMultilingualV1},
	}
	tblInfo := &model.TableInfo{Indices: []*model.IndexInfo{titleIndex, bodyIndex, nonPublicIndex, compositeIndex}}

	// Index lookup matches the FULLTEXT definition; parser support is checked
	// separately when deciding whether TiFlash can evaluate the same analyzer.
	require.Same(t, compositeIndex, publicFTSIndexOnColumns(tblInfo, []pmodel.CIStr{title, body}))
	require.Same(t, compositeIndex, publicFTSIndexOnColumns(tblInfo, []pmodel.CIStr{body, title}))
	require.Nil(t, publicFTSIndexOnColumns(tblInfo, []pmodel.CIStr{title, title}))
	require.Same(t, titleIndex, publicFTSIndexOnColumns(tblInfo, []pmodel.CIStr{title}))
	require.Nil(t, publicFTSIndexOnColumns(tblInfo, []pmodel.CIStr{title, pmodel.NewCIStr("missing")}))
	require.Nil(t, publicFTSIndexOnColumns(nil, []pmodel.CIStr{title, body}))
	require.Nil(t, publicFTSIndexOnColumns(tblInfo, nil))
	require.Nil(t, publicFTSIndexOnColumns(&model.TableInfo{Indices: []*model.IndexInfo{titleIndex, bodyIndex, nonPublicIndex}}, []pmodel.CIStr{body, title}))
}
