// Copyright 2024 PingCAP, Inc.
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

package domainmisc

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/table/temptable"
)

// GetLatestIndexInfo gets the index info of latest schema version from given table id,
// it returns nil if the schema version is not changed
func GetLatestIndexInfo(ctx base.PlanContext, id int64, startVer int64) (map[int64]*model.IndexInfo, bool, error) {
	dom := domain.GetDomain(ctx)
	if dom == nil {
		return nil, false, errors.New("domain not found for ctx")
	}
	is := temptable.AttachLocalTemporaryTableInfoSchema(ctx, dom.InfoSchema())
	if is.SchemaMetaVersion() == startVer {
		return nil, false, nil
	}
	latestIndexes := make(map[int64]*model.IndexInfo)

	latestTbl, latestTblExist := is.TableByID(context.Background(), id)
	if latestTblExist {
		latestTblInfo := latestTbl.Meta()
		for _, index := range latestTblInfo.Indices {
			latestIndexes[index.ID] = index
		}
	}
	return latestIndexes, true, nil
}
