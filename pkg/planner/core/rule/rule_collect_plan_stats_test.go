// Copyright 2025 PingCAP, Inc.
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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestCollectHistNeededItemsReturnsStableOrder(t *testing.T) {
	histNeededColumns := []model.StatsLoadItem{
		{TableItemID: model.TableItemID{TableID: 3, ID: 2, IsIndex: false}, FullLoad: true},
		{TableItemID: model.TableItemID{TableID: 1, ID: 5, IsIndex: false}, FullLoad: false},
	}
	histNeededIndices := map[model.TableItemID]struct{}{
		{TableID: 2, ID: 1, IsIndex: true}: {},
		{TableID: 1, ID: 2, IsIndex: true}: {},
	}

	expected := []model.StatsLoadItem{
		{TableItemID: model.TableItemID{TableID: 1, ID: 2, IsIndex: true}, FullLoad: true},
		{TableItemID: model.TableItemID{TableID: 1, ID: 5, IsIndex: false}, FullLoad: false},
		{TableItemID: model.TableItemID{TableID: 2, ID: 1, IsIndex: true}, FullLoad: true},
		{TableItemID: model.TableItemID{TableID: 3, ID: 2, IsIndex: false}, FullLoad: true},
	}
	require.Equal(t, expected, collectHistNeededItems(histNeededColumns, histNeededIndices))
	for range 500 {
		require.Equal(t, expected, collectHistNeededItems(histNeededColumns, histNeededIndices))
	}
}
