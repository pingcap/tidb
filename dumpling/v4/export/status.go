// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql"
	"fmt"
	"time"

	tcontext "github.com/pingcap/dumpling/v4/context"

	"github.com/docker/go-units"
	"go.uber.org/zap"
)

const logProgressTick = 2 * time.Minute

func (d *Dumper) runLogProgress(tctx *tcontext.Context) {
	conf := d.conf
	totalTables := float64(calculateTableCount(conf.Tables))
	logProgressTicker := time.NewTicker(logProgressTick)
	lastCheckpoint := time.Now()
	lastBytes := float64(0)
	defer logProgressTicker.Stop()
	for {
		select {
		case <-tctx.Done():
			tctx.L().Debug("stopping log progress")
			return
		case <-logProgressTicker.C:
			nanoseconds := float64(time.Since(lastCheckpoint).Nanoseconds())

			completedTables := ReadCounter(finishedTablesCounter, conf.Labels)
			finishedBytes := ReadCounter(finishedSizeCounter, conf.Labels)
			finishedRows := ReadCounter(finishedRowsCounter, conf.Labels)
			estimateTotalRows := ReadCounter(estimateTotalRowsCounter, conf.Labels)

			tctx.L().Info("progress",
				zap.String("tables", fmt.Sprintf("%.0f/%.0f (%.1f%%)", completedTables, totalTables, completedTables/totalTables*100)),
				zap.String("finished rows", fmt.Sprintf("%.0f", finishedRows)),
				zap.String("estimate total rows", fmt.Sprintf("%.0f", estimateTotalRows)),
				zap.String("finished size", units.HumanSize(finishedBytes)),
				zap.Float64("average speed(MiB/s)", (finishedBytes-lastBytes)/(1048576e-9*nanoseconds)),
			)

			lastCheckpoint = time.Now()
			lastBytes = finishedBytes
		}
	}
}

func calculateTableCount(m DatabaseTables) int {
	cnt := 0
	for _, tables := range m {
		for _, table := range tables {
			if table.Type == TableTypeBase {
				cnt++
			}
		}
	}
	return cnt
}

func (d *Dumper) getEstimateTotalRowsCount(tctx *tcontext.Context, conn *sql.Conn) error {
	conf := d.conf
	var totalCount uint64
	for db, tables := range conf.Tables {
		for _, m := range tables {
			if m.Type == TableTypeBase {
				// get pk or uk for explain
				field, err := pickupPossibleField(db, m.Name, conn, conf)
				if err != nil {
					return err
				}
				c := estimateCount(tctx, db, m.Name, conn, field, conf)
				totalCount += c
			}
		}
	}
	AddCounter(estimateTotalRowsCounter, conf.Labels, float64(totalCount))
	return nil
}
