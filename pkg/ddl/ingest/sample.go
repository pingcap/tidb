package ingest

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// EstimateTableRowSize estimates the row size in bytes of a table.
func EstimateTableRowSize(
	ctx context.Context,
	exec sqlexec.RestrictedSQLExecutor,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	idxInfos []*model.IndexInfo,
) (rowAvgSize, idxAvgSize int) {
	defer util.Recover(metrics.LabelDDL, "estimateTableRowSize", nil, false)
	start := time.Now()
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "select * from %n.%n tablesample regions() limit 10;", dbInfo.Name.L, tblInfo.Name.L)
	if err != nil {
		logutil.Logger(ctx).Warn("fail to estimate row size",
			zap.String("db", dbInfo.Name.L), zap.String("tbl", tblInfo.Name.L), zap.Error(err))
		return 0, 0
	}

	rowMin, rowMax, rowAvg := collectSamples(rows, tblInfo.Columns)

	allIdxAvg := 0
	for _, idxInfo := range idxInfos {
		idxCols := make([]*model.ColumnInfo, 0, len(idxInfo.Columns))
		for _, idxCol := range idxInfo.Columns {
			idxCols = append(idxCols, tblInfo.Columns[idxCol.Offset])
		}
		idxMin, idxMax, idxAvg := collectSamples(rows, idxCols)
		logutil.Logger(ctx).Info("estimate index size per row",
			zap.Int("avgSize", idxAvg),
			zap.Int("minSize", idxMin),
			zap.Int("maxSize", idxMax),
		)
		allIdxAvg += idxAvg
	}

	logutil.Logger(ctx).Info("estimate row size",
		zap.Int64("tableID", tblInfo.ID),
		zap.Int("avgSize", rowAvg),
		zap.Int("minSize", rowMin),
		zap.Int("maxSize", rowMax),
		zap.Int("count", len(rows)),
		zap.Duration("duration", time.Since(start)),
	)

	return rowAvg, allIdxAvg
}

func collectSamples(samples []chunk.Row, cols []*model.ColumnInfo) (minSize, maxSize, avgSize int) {
	minSize = math.MaxInt
	total := 0
	for i := range len(samples) {
		row := 0
		for _, col := range cols {
			datum := samples[i].GetDatum(col.Offset, &col.FieldType)
			evs, err := codec.EstimateValueSize(datum)
			if err == nil {
				row += evs
			}
		}
		maxSize = max(maxSize, row)
		minSize = min(minSize, row)
		total += row
	}
	return minSize, maxSize, total / len(samples)
}
