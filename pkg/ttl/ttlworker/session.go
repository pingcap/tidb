// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttlworker

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	statshandle "github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/metrics"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// The following two functions are using `sqlexec.SQLExecutor` to represent session
// which is actually not correct. It's a work around for the cyclic dependency problem.
// It actually doesn't accept arbitrary SQLExecutor, but just `*session.session`, which means
// you cannot pass the `(ttl/session).Session` into it.
// Use `sqlexec.SQLExecutor` and `sessionctx.Session` or another other interface (including
// `interface{}`) here is the same, I just pick one small enough interface.
// Also, we cannot use the functions in `session/session.go` (to avoid cyclic dependency), so
// registering function here is really needed.

func withSession(pool syssession.Pool, fn func(session.Session) error) error {
	return pool.WithSession(func(s *syssession.Session) error {
		return s.WithSessionContext(func(sctx sessionctx.Context) error {
			if intest.InTest {
				// Only for test, in this case, the return session is mockSession
				if se, ok := sctx.(session.Session); ok {
					return fn(se)
				}
			}

			exec := statshandle.AttachStatsCollector(sctx.GetSQLExecutor())
			defer statshandle.DetachStatsCollector(exec)

			se := session.NewSession(sctx, s.AvoidReuse)
			restore, err := prepareSession(se)
			if err != nil {
				return err
			}
			defer restore()
			return fn(se)
		})
	})
}

func prepareSession(se session.Session) (func(), error) {
	originalRetryLimit := se.GetSessionVars().RetryLimit
	originalEnable1PC := se.GetSessionVars().Enable1PC
	originalEnableAsyncCommit := se.GetSessionVars().EnableAsyncCommit
	originalTimeZone, restoreTimeZone := "", false
	originalIsolationReadEngines, restoreIsolationReadEngines := "", false

	restore := func() {
		_, err := se.ExecuteSQL(context.Background(), fmt.Sprintf("set tidb_retry_limit=%d", originalRetryLimit))
		if err != nil {
			logutil.BgLogger().Warn("fail to reset tidb_retry_limit", zap.Int64("originalRetryLimit", originalRetryLimit), zap.Error(err))
			se.AvoidReuse()
			return
		}

		if !originalEnable1PC {
			_, err = se.ExecuteSQL(context.Background(), "set tidb_enable_1pc=OFF")
			terror.Log(err)
			if err != nil {
				se.AvoidReuse()
				return
			}
		}

		if !originalEnableAsyncCommit {
			_, err = se.ExecuteSQL(context.Background(), "set tidb_enable_async_commit=OFF")
			terror.Log(err)
			if err != nil {
				se.AvoidReuse()
				return
			}
		}

		if restoreTimeZone {
			_, err = se.ExecuteSQL(context.Background(), "set @@time_zone=%?", originalTimeZone)
			terror.Log(err)
			if err != nil {
				se.AvoidReuse()
				return
			}
		}

		if restoreIsolationReadEngines {
			_, err = se.ExecuteSQL(context.Background(), "set tidb_isolation_read_engines=%?", originalIsolationReadEngines)
			terror.Log(err)
			if err != nil {
				se.AvoidReuse()
				return
			}
		}
	}

	// store and set the retry limit to 0
	_, err := se.ExecuteSQL(context.Background(), "set tidb_retry_limit=0")
	if err != nil {
		return nil, err
	}

	// set enable 1pc to ON
	_, err = se.ExecuteSQL(context.Background(), "set tidb_enable_1pc=ON")
	if err != nil {
		return nil, err
	}

	// set enable async commit to ON
	_, err = se.ExecuteSQL(context.Background(), "set tidb_enable_async_commit=ON")
	if err != nil {
		return nil, err
	}

	// Force rollback the session to guarantee the session is not in any explicit transaction
	if _, err = se.ExecuteSQL(context.Background(), "ROLLBACK"); err != nil {
		return nil, err
	}

	// set the time zone to UTC
	rows, err := se.ExecuteSQL(context.Background(), "select @@time_zone")
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 || rows[0].Len() == 0 {
		return nil, errors.New("failed to get time_zone variable")
	}
	originalTimeZone = rows[0].GetString(0)

	_, err = se.ExecuteSQL(context.Background(), "set @@time_zone='UTC'")
	if err != nil {
		return nil, err
	}
	restoreTimeZone = true

	// allow the session in TTL to use all read engines.
	_, hasTiDBEngine := se.GetSessionVars().IsolationReadEngines[kv.TiDB]
	_, hasTiKVEngine := se.GetSessionVars().IsolationReadEngines[kv.TiKV]
	_, hasTiFlashEngine := se.GetSessionVars().IsolationReadEngines[kv.TiFlash]
	if !hasTiDBEngine || !hasTiKVEngine || !hasTiFlashEngine {
		rows, err := se.ExecuteSQL(context.Background(), "select @@tidb_isolation_read_engines")
		if err != nil {
			return nil, err
		}

		if len(rows) == 0 || rows[0].Len() == 0 {
			return nil, errors.New("failed to get tidb_isolation_read_engines variable")
		}
		originalIsolationReadEngines = rows[0].GetString(0)

		_, err = se.ExecuteSQL(context.Background(), "set tidb_isolation_read_engines='tikv,tiflash,tidb'")
		if err != nil {
			return nil, err
		}

		restoreIsolationReadEngines = true
	}

	return restore, nil
}

func newTableSession(se session.Session, tbl *cache.PhysicalTable, expire time.Time, jobType cache.TTLJobType) (*ttlTableSession, error) {
	var cfg ttlWorkConfig
	switch jobType {
	case cache.TTLJobTypeSoftDelete:
		cfg = ttlWorkConfig{
			jobType:   jobType,
			jobEnable: vardef.SoftDeleteJobEnable.Load,
			validateNew: func(_ context.Context, _ session.Session, _ *cache.PhysicalTable, _ *cache.PhysicalTable, newTblInfo *model.TableInfo, _ time.Time) error {
				if newTblInfo.SoftdeleteInfo == nil {
					return errors.New("table softdelete disabled")
				}
				return nil
			},
		}
	case cache.TTLJobTypeTTL:
		cfg = ttlWorkConfig{
			jobType:   jobType,
			jobEnable: vardef.EnableTTLJob.Load,
			validateNew: func(ctx context.Context, s session.Session, oldTbl, newTbl *cache.PhysicalTable, newTblInfo *model.TableInfo, expire time.Time) error {
				if !newTbl.TTLInfo.Enable {
					return errors.New("table TTL disabled")
				}
				newTimeCol, err := newTbl.TimeColumnForJob(cache.TTLJobTypeTTL)
				if err != nil {
					return err
				}
				oldTimeCol, err := oldTbl.TimeColumnForJob(cache.TTLJobTypeTTL)
				if err != nil {
					return err
				}
				if newTimeCol.Name.L != oldTimeCol.Name.L {
					return errors.New("time column name changed")
				}
				if newTblInfo.TTLInfo.IntervalExprStr != oldTbl.TTLInfo.IntervalExprStr ||
					newTblInfo.TTLInfo.IntervalTimeUnit != oldTbl.TTLInfo.IntervalTimeUnit {
					newExpireTime, err := newTbl.EvalExpireTimeForJob(ctx, s, s.Now(), cache.TTLJobTypeTTL)
					if err != nil {
						return err
					}
					if newExpireTime.Before(expire) {
						return errors.New("expire interval changed")
					}
				}
				return nil
			},
		}
		se.GetSessionVars().SoftDeleteRewrite = false
	default:
		return nil, errors.Errorf("unknown job type: %s", jobType)
	}

	return &ttlTableSession{
		Session: se,
		tbl:     tbl,
		expire:  expire,
		cfg:     cfg,
	}, nil
}

// NewScanSession creates a session for scan
func NewScanSession(se session.Session, tbl *cache.PhysicalTable, expire time.Time, jobType cache.TTLJobType) (*ttlTableSession, func() error, error) {
	origConcurrency := se.GetSessionVars().DistSQLScanConcurrency()
	origPaging := se.GetSessionVars().EnablePaging
	se.GetSessionVars().InternalSQLScanUserTable = true
	restore := func() error {
		se.GetSessionVars().InternalSQLScanUserTable = false
		_, err := se.ExecuteSQL(context.Background(), "set @@tidb_distsql_scan_concurrency=%?", origConcurrency)
		terror.Log(err)
		if err != nil {
			se.AvoidReuse()
		}

		_, tmpErr := se.ExecuteSQL(context.Background(), "set @@tidb_enable_paging=%?", origPaging)
		if tmpErr != nil {
			err = multierr.Append(err, tmpErr)
			se.AvoidReuse()
		}

		return err
	}

	// Set the distsql scan concurrency to 1 to reduce the number of cop tasks in TTL scan.
	if _, err := se.ExecuteSQL(context.Background(), "set @@tidb_distsql_scan_concurrency=1"); err != nil {
		terror.Log(restore())
		return nil, nil, err
	}

	// Disable tidb_enable_paging because we have already had a `LIMIT` in the SQL to limit the result set.
	// If `tidb_enable_paging` is enabled, it may have multiple cop tasks even in one region that makes some extra
	// processed keys in TiKV side, see issue: https://github.com/pingcap/tidb/issues/58342.
	// Disable it to make the scan more efficient.
	if _, err := se.ExecuteSQL(context.Background(), "set @@tidb_enable_paging=OFF"); err != nil {
		terror.Log(restore())
		return nil, nil, err
	}

	tblSe, err := newTableSession(se, tbl, expire, jobType)
	if err != nil {
		terror.Log(restore())
		return nil, nil, err
	}

	return tblSe, restore, nil
}

type ttlTableSession struct {
	session.Session
	tbl    *cache.PhysicalTable
	expire time.Time
	cfg    ttlWorkConfig
}

type ttlWorkConfig struct {
	jobType     cache.TTLJobType
	jobEnable   func() bool
	validateNew func(ctx context.Context, s session.Session, oldTbl, newTbl *cache.PhysicalTable, newTblInfo *model.TableInfo, expire time.Time) error
}

func (s *ttlTableSession) ExecuteSQLWithCheck(ctx context.Context, sql string) ([]chunk.Row, bool, error) {
	tracer := metrics.PhaseTracerFromCtx(ctx)
	defer tracer.EnterPhase(tracer.Phase())

	tracer.EnterPhase(metrics.PhaseOther)
	if !s.cfg.jobEnable() {
		return nil, false, errors.Errorf("global %s job is disabled", s.cfg.jobType)
	}

	if err := s.ResetWithGlobalTimeZone(ctx); err != nil {
		return nil, false, err
	}

	var result []chunk.Row
	shouldRetry := true
	err := s.RunInTxn(ctx, func() error {
		tracer.EnterPhase(metrics.PhaseQuery)
		defer tracer.EnterPhase(tracer.Phase())
		rows, err := s.ExecuteSQL(ctx, sql)
		tracer.EnterPhase(metrics.PhaseCheckTTL)
		// We must check the configuration after ExecuteSQL because of MDL and the meta the current transaction used
		// can only be determined after executed one query.
		if validateErr := s.validateTTLWork(ctx); validateErr != nil {
			shouldRetry = false
			return errors.Annotatef(validateErr, "table '%s.%s' meta changed, should abort current job", s.tbl.Schema, s.tbl.Name)
		}

		if err != nil {
			return err
		}

		result = rows
		return nil
	}, session.TxnModeOptimistic)

	if err != nil {
		return nil, shouldRetry, err
	}

	return result, false, nil
}

func (s *ttlTableSession) validateTTLWork(ctx context.Context) error {
	newTblInfo, err := s.SessionInfoSchema().TableInfoByName(s.tbl.Schema, s.tbl.Name)
	if err != nil {
		return err
	}

	if s.tbl.TableInfo == newTblInfo {
		return nil
	}

	if s.tbl.TableInfo.ID != newTblInfo.ID {
		return errors.New("table id changed")
	}

	var newPhysical *cache.PhysicalTable
	newPhysical, err = cache.NewPhysicalTable(
		s.tbl.Schema,
		newTblInfo,
		s.tbl.Partition,
		s.cfg.jobType == cache.TTLJobTypeTTL,
		s.cfg.jobType == cache.TTLJobTypeSoftDelete,
	)
	if err != nil {
		return err
	}

	if newPhysical.ID != s.tbl.ID {
		return errors.New("physical id changed")
	}

	if s.tbl.Partition.L != "" {
		if newPhysical.PartitionDef.Name.L != s.tbl.PartitionDef.Name.L {
			return errors.New("partition name changed")
		}
	}

	return s.cfg.validateNew(ctx, s.Session, s.tbl, newPhysical, newTblInfo, s.expire)
}
