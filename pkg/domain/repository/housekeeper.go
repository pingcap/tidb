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

package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

func calcNextTick(now time.Time) time.Duration {
	// only activated at 2am
	next := time.Date(now.Year(), now.Month(), now.Day(), 2, 0, 0, 0, time.Local)
	if !next.After(now) {
		next = time.Date(now.Year(), now.Month(), now.Day()+1, 2, 0, 0, 0, time.Local)
	}
	return next.Sub(now)
}

func (w *Worker) createAllPartitions(ctx context.Context, exec sqlexec.SQLExecutor, is infoschema.InfoSchema) error {
	sb := &strings.Builder{}
	for _, tbl := range workloadTables {
		tbSchema, err := is.TableByName(ctx, workloadSchemaCIStr, model.NewCIStr(tbl.destTable))
		if err != nil {
			logutil.BgLogger().Info("housekeeper can't get table", zap.String("tb", tbl.destTable), zap.Error(err))
			return err
		}
		tbInfo := tbSchema.Meta()

		sb.Reset()
		sqlescape.MustFormatSQL(sb, "ALTER TABLE %n.%n ADD PARTITION (", WorkloadSchema, tbl.destTable)
		if !generatePartitionRanges(sb, tbInfo) {
			fmt.Fprintf(sb, ")")
			err = execRetry(ctx, exec, sb.String())
			if err != nil {
				logutil.BgLogger().Info("housekeeper can't add partitions", zap.String("parts", sb.String()), zap.Error(err))
				return err
			}
		}
	}
	return nil
}

func (w *Worker) dropOldPartitions(ctx context.Context, exec sqlexec.SQLExecutor, is infoschema.InfoSchema, now time.Time) error {
	retention := int(retentionDays.Load())
	if retention == 0 {
		// disabled housekeeping
		return nil
	}

	sb := &strings.Builder{}
	for _, tbl := range workloadTables {
		tbSchema, err := is.TableByName(ctx, workloadSchemaCIStr, model.NewCIStr(tbl.destTable))
		if err != nil {
			logutil.BgLogger().Info("housekeeper can't get table", zap.String("tb", tbl.destTable), zap.Error(err))
			continue
		}
		tbInfo := tbSchema.Meta()
		for _, pt := range tbInfo.GetPartitionInfo().Definitions {
			ot, err := time.Parse("p20060102", pt.Name.L)
			if err != nil {
				logutil.BgLogger().Info("housekeeper can't parse partition name", zap.String("part", pt.Name.L), zap.Error(err))
				break
			}
			if int(now.Sub(ot).Hours()/24) < retention {
				continue
			}
			sb.Reset()
			sqlescape.MustFormatSQL(sb, "ALTER TABLE %s.%s DROP PARTITION %s",
				WorkloadSchema, tbl.destTable, pt.Name.L)
			err = execRetry(ctx, exec, sb.String())
			if err != nil {
				logutil.BgLogger().Info("housekeeper can't drop partition", zap.String("part", pt.Name.L), zap.Error(err))
				break
			}
		}
	}
	return nil
}

func (w *Worker) startHouseKeeper(ctx context.Context) func() {
	return func() {
		now := time.Now()
		timer := time.NewTimer(calcNextTick(now))
		defer timer.Stop()

		_sessctx := w.getSessionWithRetry()
		defer w.sesspool.Put(_sessctx)
		exec := _sessctx.(sqlexec.SQLExecutor)
		sess := _sessctx.(sessionctx.Context)
		for {
			select {
			case <-w.exit:
				return
			case <-ctx.Done():
				return
			case now := <-timer.C:
				// Owner only
				if !w.owner.IsOwner() {
					continue
				}

				// get the latest infoschema
				is := sess.GetDomainInfoSchema().(infoschema.InfoSchema)
				err := execRetry(ctx, exec, "use "+workloadSchemaCIStr.L)
				if err != nil {
					logutil.BgLogger().Info("can't switch db", zap.Error(err))
					continue
				}

				// create new partitions
				if err := w.createAllPartitions(ctx, exec, is); err != nil {
					continue
				}

				// drop old partitions
				if err := w.dropOldPartitions(ctx, exec, is, now); err != nil {
					continue
				}

				// reschedule, drain channel first
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(calcNextTick(now))
			}
		}
	}
}
