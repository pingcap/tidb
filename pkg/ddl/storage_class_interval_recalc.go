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

package ddl

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

const (
	// storageClassIntervalRecalcCheckInterval is how often the owner re-reads
	// the recalculation period, so changing it takes effect without waiting
	// for the previous period to end.
	storageClassIntervalRecalcCheckInterval = time.Minute

	storageClassIntervalRecalcFailedMetric = "storage_class_interval_recalc_failed"
)

// storageClassIntervalChanges are the partition storage class changes that
// re-evaluating the interval selectors of a table would make.
type storageClassIntervalChanges struct {
	hasInterval bool
	// toIA are partitions converted from STANDARD to IA.
	toIA []string
	// others are partitions converted in any other way. Time only moves
	// forward, so these indicate clock skew and are never applied by
	// recalculation.
	others []string
}

func evalStorageClassIntervalChanges(tblInfo *model.TableInfo, now time.Time) (storageClassIntervalChanges, error) {
	var changes storageClassIntervalChanges
	if tblInfo.Partition == nil || tblInfo.EngineAttribute == "" {
		return changes, nil
	}
	attr, err := model.ParseEngineAttributeFromString(tblInfo.EngineAttribute)
	if err != nil {
		return changes, errors.Trace(err)
	}
	if attr.StorageClass == nil {
		return changes, nil
	}
	settings, err := BuildStorageClassSettingsFromJSON(attr.StorageClass)
	if err != nil {
		return changes, errors.Trace(err)
	}
	partitions := tblInfo.Partition.Definitions
	evaluator, err := newStorageClassPartitionEvaluator(tblInfo, partitions, settings, now)
	if err != nil {
		return changes, errors.Trace(err)
	}
	changes.hasInterval = evaluator.hasInterval
	if !evaluator.hasInterval {
		return changes, nil
	}
	for i := range partitions {
		part := &partitions[i]
		def, err := evaluator.matchingDef(part)
		if err != nil {
			return changes, errors.Trace(err)
		}
		newTier := model.StorageClassTierDefault
		if def != nil {
			newTier = def.Tier
		}
		oldTier := normalizedStorageClassTransitionTarget(part.StorageClassTier)
		newTier = normalizedStorageClassTransitionTarget(newTier)
		switch {
		case oldTier == newTier:
		case oldTier == model.StorageClassTierStandard && newTier == model.StorageClassTierIA:
			changes.toIA = append(changes.toIA, part.Name.O)
		default:
			changes.others = append(changes.others, part.Name.O)
		}
	}
	return changes, nil
}

// storageClassChangesOnlyToIA reports whether an interval recalculation job
// should apply: at least one partition changes, and every change is from
// STANDARD to IA. Anything else means the rule changed or the clock moved
// backwards, and the job does nothing.
func storageClassChangesOnlyToIA(tblInfo *model.TableInfo, old, current map[int64]physicalStorageClass) bool {
	logger := logutil.DDLLogger().With(zap.Int64("tableID", tblInfo.ID), zap.String("table", tblInfo.Name.O))
	changed := changedStorageClassPhysicalIDs(old, current)
	if len(changed) == 0 {
		logger.Info("storage class interval recalculation skipped, no partition changes")
		return false
	}
	for physicalID := range changed {
		if normalizedStorageClassTransitionTarget(old[physicalID].tier) != model.StorageClassTierStandard ||
			normalizedStorageClassTransitionTarget(current[physicalID].tier) != model.StorageClassTierIA {
			logger.Warn("storage class interval recalculation skipped, partitions would leave IA",
				zap.Int64("physicalID", physicalID))
			return false
		}
	}
	return true
}

// storageClassIntervalRecalculator periodically re-evaluates interval
// selectors on the DDL owner and submits ActionModifyEngineAttribute jobs for
// tables with partitions newly crossing the cutoff.
type storageClassIntervalRecalculator struct {
	infoCache *infoschema.InfoCache
	sessPool  *sess.Pool
	executor  *executor
}

func (r *storageClassIntervalRecalculator) run(ctx context.Context) {
	var lastRound time.Time
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		period, err := r.loadPeriod(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logutil.DDLLogger().Warn("load storage class interval recalculation period failed", zap.Error(err))
			metrics.DDLCounter.WithLabelValues(storageClassIntervalRecalcFailedMetric).Inc()
		} else if period > 0 && (lastRound.IsZero() || time.Since(lastRound) >= period) &&
			config.GetGlobalConfig().EnableStorageClass {
			lastRound = time.Now()
			r.runRound(ctx)
			failpoint.InjectCall("afterStorageClassIntervalRecalcRound")
		}
		checkInterval := storageClassIntervalRecalcCheckInterval
		failpoint.InjectCall("mockStorageClassIntervalRecalcCheckInterval", &checkInterval)
		timer.Reset(checkInterval)
	}
}

func (r *storageClassIntervalRecalculator) loadPeriod(ctx context.Context) (time.Duration, error) {
	if r.sessPool == nil {
		return 0, errors.New("session pool is unavailable")
	}
	sctx, err := r.sessPool.Get()
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer r.sessPool.Put(sctx)
	//nolint:forbidigo
	value, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBStorageClassIntervalRecalcPeriod)
	if err != nil {
		return 0, errors.Trace(err)
	}
	period, err := time.ParseDuration(value)
	if err != nil {
		return 0, errors.Trace(err)
	}
	failpoint.InjectCall("mockStorageClassIntervalRecalcPeriod", &period)
	return period, ctx.Err()
}

func (r *storageClassIntervalRecalculator) runRound(ctx context.Context) {
	defer func() {
		if rec := recover(); rec != nil {
			logutil.DDLLogger().Warn("storage class interval recalculation panicked",
				zap.Any("recover", rec), zap.Stack("stack"))
			metrics.DDLCounter.WithLabelValues(storageClassIntervalRecalcFailedMetric).Inc()
		}
	}()
	is := r.infoCache.GetLatest()
	if is == nil {
		logutil.DDLLogger().Warn("storage class interval recalculation skipped, information schema is unavailable")
		metrics.DDLCounter.WithLabelValues(storageClassIntervalRecalcFailedMetric).Inc()
		return
	}
	now := storageClassIntervalNow()
	var tables, submitted, failed int
	defer func() {
		logutil.DDLLogger().Info("storage class interval recalculation round finished",
			zap.Time("now", now), zap.Int("tables", tables), zap.Int("submitted", submitted), zap.Int("failed", failed))
	}()
	for _, schema := range is.ListTablesWithSpecialAttribute(infoschemacontext.PartitionAttribute) {
		dbInfo, ok := is.SchemaByName(schema.DBName)
		if !ok {
			continue
		}
		for _, tblInfo := range schema.TableInfos {
			if ctx.Err() != nil {
				return
			}
			tables++
			isSubmitted, err := r.recalcTable(ctx, dbInfo, tblInfo, now)
			if isSubmitted {
				submitted++
			}
			if err != nil && ctx.Err() == nil {
				failed++
				logutil.DDLLogger().Warn("storage class interval recalculation failed",
					zap.Int64("tableID", tblInfo.ID), zap.String("table", tblInfo.Name.O), zap.Error(err))
				metrics.DDLCounter.WithLabelValues(storageClassIntervalRecalcFailedMetric).Inc()
			}
		}
	}
}

// recalcTable submits a job if partitions of the table newly cross the cutoff,
// and reports whether it did.
func (r *storageClassIntervalRecalculator) recalcTable(
	ctx context.Context,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	now time.Time,
) (submitted bool, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = errors.Errorf("storage class interval recalculation panicked: %v", rec)
			logutil.DDLLogger().Warn("storage class interval recalculation panicked", zap.Stack("stack"))
		}
	}()
	changes, err := evalStorageClassIntervalChanges(tblInfo, now)
	if err != nil || !changes.hasInterval {
		return false, err
	}
	if len(changes.others) > 0 {
		return false, errors.Errorf("partitions %v would leave IA, the clock may have moved backwards", changes.others)
	}
	if len(changes.toIA) == 0 {
		return false, nil
	}
	failpoint.InjectCall("beforeStorageClassIntervalRecalcSubmit", tblInfo.ID)
	if err := r.submit(ctx, dbInfo, tblInfo, changes.toIA); err != nil {
		return false, err
	}
	return true, nil
}

// submit enqueues the job without waiting for it to finish. A failed or
// skipped job is retried by the next round, which re-evaluates the table.
func (r *storageClassIntervalRecalculator) submit(
	ctx context.Context,
	dbInfo *model.DBInfo,
	tblInfo *model.TableInfo,
	toIA []string,
) error {
	query, err := sqlescape.EscapeSQL("ALTER TABLE %n.%n ENGINE_ATTRIBUTE = %?", dbInfo.Name.O, tblInfo.Name.O, tblInfo.EngineAttribute)
	if err != nil {
		return errors.Trace(err)
	}
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		SchemaName: dbInfo.Name.L,
		TableName:  tblInfo.Name.L,
		Type:       model.ActionModifyEngineAttribute,
		BinlogInfo: &model.HistoryInfo{},
		TraceInfo:  &tracing.TraceInfo{},
		Query:      query,
	}
	args := &model.ModifyTableEngineAttributeArgs{
		EngineAttribute:       tblInfo.EngineAttribute,
		IntervalRecalculation: true,
	}
	jobW := NewJobWrapperWithArgs(job, args, false)
	// Buffered, so the submit loop never blocks if this owner retires before
	// reading the result.
	jobW.ResultCh[0] = make(chan jobSubmitResult, 1)
	select {
	case r.executor.limitJobCh <- jobW:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case result := <-jobW.ResultCh[0]:
		// Nobody waits for this job to finish.
		r.executor.delJobDoneCh(result.jobID)
		if result.err != nil {
			return errors.Trace(result.err)
		}
		logutil.DDLLogger().Info("storage class interval recalculation submitted",
			zap.Int64("jobID", result.jobID), zap.Int64("tableID", tblInfo.ID),
			zap.String("table", fmt.Sprintf("%s.%s", dbInfo.Name.O, tblInfo.Name.O)), zap.Strings("toIA", toIA))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
