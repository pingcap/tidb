package domainmisc

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/model"
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
