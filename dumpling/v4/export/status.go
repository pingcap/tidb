// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"fmt"
	"time"

	"github.com/docker/go-units"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

const logProgressTick = 2 * time.Minute

func (d *Dumper) runLogProgress(ctx context.Context) {
	conf := d.conf
	totalTables := float64(calculateTableCount(conf.Tables))
	logProgressTicker := time.NewTicker(logProgressTick)
	lastCheckpoint := time.Now()
	lastBytes := float64(0)
	defer logProgressTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Debug("stopping log progress")
			return
		case <-logProgressTicker.C:
			nanoseconds := float64(time.Since(lastCheckpoint).Nanoseconds())

			completedTables := ReadCounter(finishedTablesCounter.With(conf.Labels))
			finishedBytes := ReadCounter(finishedSizeCounter.With(conf.Labels))
			finishedRows := ReadCounter(finishedRowsCounter.With(conf.Labels))

			log.Info("progress",
				zap.String("tables", fmt.Sprintf("%.0f/%.0f (%.1f%%)", completedTables, totalTables, completedTables/totalTables*100)),
				zap.String("finished rows", fmt.Sprintf("%.0f", finishedRows)),
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
