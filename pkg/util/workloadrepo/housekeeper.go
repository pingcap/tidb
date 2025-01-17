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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
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

func createPartition(ctx context.Context, is infoschema.InfoSchema, tbl *repositoryTable, sess sessionctx.Context, now time.Time) error {
	tbSchema, err := is.TableByName(ctx, workloadSchemaCIStr, ast.NewCIStr(tbl.destTable))
	if err != nil {
		logutil.BgLogger().Info("workload repository cannot get table", zap.String("tbl", tbl.destTable), zap.NamedError("err", err))
		return err
	}
	tbInfo := tbSchema.Meta()

	sb := &strings.Builder{}
	sqlescape.MustFormatSQL(sb, "ALTER TABLE %n.%n ADD PARTITION (", WorkloadSchema, tbl.destTable)
	skip, err := generatePartitionRanges(sb, tbInfo, now)
	if err != nil {
		return err
	}
	if !skip {
		fmt.Fprintf(sb, ")")
		_, err = execRetry(ctx, sess, sb.String())
		if err != nil {
			logutil.BgLogger().Info("workload repository cannot add partitions", zap.String("parts", sb.String()), zap.NamedError("err", err))
			return err
		}
	}
	return nil
}

func (w *worker) createAllPartitions(ctx context.Context, sess sessionctx.Context, is infoschema.InfoSchema, now time.Time) error {
	for _, tbl := range w.workloadTables {
		if err := createPartition(ctx, is, &tbl, sess, now); err != nil {
			return err
		}
	}
	return nil
}

func dropOldPartition(ctx context.Context, is infoschema.InfoSchema,
	tbl *repositoryTable, now time.Time, retention int, sess sessionctx.Context) error {
	tbSchema, err := is.TableByName(ctx, workloadSchemaCIStr, ast.NewCIStr(tbl.destTable))
	if err != nil {
		return fmt.Errorf("workload repository could not find table `%s`: %v", tbl.destTable, err)
	}
	tbInfo := tbSchema.Meta()
	if tbInfo == nil {
		return fmt.Errorf("workload repository could not load information for '%s'", tbl.destTable)
	}
	pi := tbInfo.GetPartitionInfo()
	if pi == nil || pi.Definitions == nil {
		return fmt.Errorf("workload repository could not load partition information for '%s'", tbl.destTable)
	}
	for _, pt := range pi.Definitions {
		ot, err := parsePartitionName(pt.Name.L)
		if err != nil {
			return fmt.Errorf("workload repository could not cannot parse partition name (%s) for '%s': %v", pt.Name.L, tbl.destTable, err)
		}
		if int(now.Sub(ot).Hours()/24) < retention {
			continue
		}
		sb := &strings.Builder{}
		sqlescape.MustFormatSQL(sb, "ALTER TABLE %n.%n DROP PARTITION %n",
			WorkloadSchema, tbl.destTable, pt.Name.L)
		_, err = execRetry(ctx, sess, sb.String())
		if err != nil {
			return fmt.Errorf("workload repository cannot drop partition (%s) on '%s': %v", pt.Name.L, tbl.destTable, err)
		}
	}

	return nil
}

func (w *worker) dropOldPartitions(ctx context.Context, sess sessionctx.Context, is infoschema.InfoSchema, now time.Time, retention int) error {
	if retention == 0 {
		// disabled housekeeping
		return nil
	}

	var err error
	for _, tbl := range w.workloadTables {
		err2 := dropOldPartition(ctx, is, &tbl, now, retention, sess)
		if err2 != nil {
			logutil.BgLogger().Warn("workload repository could not drop partitions", zap.NamedError("err", err2))
			err = errors.Join(err, err2)
		}
	}

	return err
}

func (w *worker) getHouseKeeper(ctx context.Context, fn func(time.Time) time.Duration) func() {
	return func() {
		now := time.Now()
		timer := time.NewTimer(fn(now))
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
				if err := w.createAllPartitions(ctx, sess, is, now); err != nil {
					continue
				}

				w.Lock()
				retention := int(w.retentionDays)
				w.Unlock()

				// drop old partitions
				if err := w.dropOldPartitions(ctx, sess, is, now, retention); err != nil {
					continue
				}

				timer.Reset(fn(now))
			}
		}
	}
}

func (w *worker) startHouseKeeper(ctx context.Context) func() {
	return w.getHouseKeeper(ctx, calcNextTick)
}
