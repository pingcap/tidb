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

package workloadrepo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
)

func calcNextTick(now time.Time) time.Duration {
	// only activated at 2am
	next := time.Date(now.Year(), now.Month(), now.Day(), 2, 0, 0, 0, time.Local)
	if !next.After(now) {
		next = next.AddDate(0, 0, 1)
	}
	return next.Sub(now)
}

func createAllPartitions(ctx context.Context, sess sessionctx.Context, is infoschema.InfoSchema) error {
	sb := &strings.Builder{}
	for _, tbl := range workloadTables {
		tbSchema, err := is.TableByName(ctx, workloadSchemaCIStr, model.NewCIStr(tbl.destTable))
		if err != nil {
			logutil.BgLogger().Info("workload repository cannot get table", zap.String("tbl", tbl.destTable), zap.NamedError("err", err))
			return err
		}
		tbInfo := tbSchema.Meta()

		sb.Reset()
		sqlescape.MustFormatSQL(sb, "ALTER TABLE %n.%n ADD PARTITION (", WorkloadSchema, tbl.destTable)
		if !generatePartitionRanges(sb, tbInfo) {
			fmt.Fprintf(sb, ")")
			_, err = execRetry(ctx, sess, sb.String())
			if err != nil {
				logutil.BgLogger().Info("workload repository cannot add partitions", zap.String("parts", sb.String()), zap.NamedError("err", err))
				return err
			}
		}
	}
	return nil
}

func (w *worker) dropOldPartitions(ctx context.Context, sess sessionctx.Context, is infoschema.InfoSchema, now time.Time) error {
	w.Lock()
	retention := int(w.retentionDays)
	w.Unlock()

	if retention == 0 {
		// disabled housekeeping
		return nil
	}

	sb := &strings.Builder{}
	for _, tbl := range workloadTables {
		tbSchema, err := is.TableByName(ctx, workloadSchemaCIStr, model.NewCIStr(tbl.destTable))
		if err != nil {
			logutil.BgLogger().Info("workload repository cannot get table", zap.String("tbl", tbl.destTable), zap.NamedError("err", err))
			continue
		}
		tbInfo := tbSchema.Meta()
		for _, pt := range tbInfo.GetPartitionInfo().Definitions {
			ot, err := time.Parse("p20060102", pt.Name.L)
			if err != nil {
				logutil.BgLogger().Info("workload repository cannot parse partition name", zap.String("part", pt.Name.L), zap.NamedError("err", err))
				break
			}
			if int(now.Sub(ot).Hours()/24) < retention {
				continue
			}
			sb.Reset()
			sqlescape.MustFormatSQL(sb, "ALTER TABLE %s.%s DROP PARTITION %s",
				WorkloadSchema, tbl.destTable, pt.Name.L)
			_, err = execRetry(ctx, sess, sb.String())
			if err != nil {
				logutil.BgLogger().Info("workload repository cannot drop partition", zap.String("part", pt.Name.L), zap.NamedError("err", err))
				break
			}
		}
	}
	return nil
}

func (w *worker) startHouseKeeper(ctx context.Context) func() {
	return func() {
		now := time.Now()
		timer := time.NewTimer(calcNextTick(now))
		defer timer.Stop()

		_sessctx := w.getSessionWithRetry()
		defer w.sesspool.Put(_sessctx)
		sess := _sessctx.(sessionctx.Context)
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-timer.C:
				// Owner only
				if !w.owner.IsOwner() {
					continue
				}

				// get the latest infoschema
				is := sessiontxn.GetTxnManager(sess).GetTxnInfoSchema()

				// create new partitions
				if err := createAllPartitions(ctx, sess, is); err != nil {
					continue
				}

				// drop old partitions
				if err := w.dropOldPartitions(ctx, sess, is, now); err != nil {
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
