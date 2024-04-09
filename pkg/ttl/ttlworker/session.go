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

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/metrics"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
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

// AttachStatsCollector attaches the stats collector for the session.
// this function is registered in BootstrapSession in /session/session.go
var AttachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
	return s
}

// DetachStatsCollector removes the stats collector for the session
// this function is registered in BootstrapSession in /session/session.go
var DetachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
	return s
}

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

func getSession(pool sessionPool) (session.Session, error) {
	resource, err := pool.Get()
	if err != nil {
		return nil, err
	}

	if se, ok := resource.(session.Session); ok {
		// Only for test, in this case, the return session is mockSession
		return se, nil
	}

	sctx, ok := resource.(sessionctx.Context)
	if !ok {
		pool.Put(resource)
		return nil, errors.Errorf("%T cannot be casted to sessionctx.Context", sctx)
	}

	exec := sctx.GetSQLExecutor()
	originalRetryLimit := sctx.GetSessionVars().RetryLimit
	originalEnable1PC := sctx.GetSessionVars().Enable1PC
	originalEnableAsyncCommit := sctx.GetSessionVars().EnableAsyncCommit
	originalTimeZone, restoreTimeZone := "", false

	se := session.NewSession(sctx, exec, func(se session.Session) {
		_, err = se.ExecuteSQL(context.Background(), fmt.Sprintf("set tidb_retry_limit=%d", originalRetryLimit))
		if err != nil {
			intest.AssertNoError(err)
			logutil.BgLogger().Error("fail to reset tidb_retry_limit", zap.Int64("originalRetryLimit", originalRetryLimit), zap.Error(err))
		}

		if !originalEnable1PC {
			_, err = se.ExecuteSQL(context.Background(), "set tidb_enable_1pc=OFF")
			intest.AssertNoError(err)
			terror.Log(err)
		}

		if !originalEnableAsyncCommit {
			_, err = se.ExecuteSQL(context.Background(), "set tidb_enable_async_commit=OFF")
			intest.AssertNoError(err)
			terror.Log(err)
		}

		if restoreTimeZone {
			_, err = se.ExecuteSQL(context.Background(), "set @@time_zone=%?", originalTimeZone)
			intest.AssertNoError(err)
			terror.Log(err)
		}

		DetachStatsCollector(exec)

		pool.Put(resource)
	})

	exec = AttachStatsCollector(exec)

	// store and set the retry limit to 0
	_, err = se.ExecuteSQL(context.Background(), "set tidb_retry_limit=0")
	if err != nil {
		se.Close()
		return nil, err
	}

	// set enable 1pc to ON
	_, err = se.ExecuteSQL(context.Background(), "set tidb_enable_1pc=ON")
	if err != nil {
		se.Close()
		return nil, err
	}

	// set enable async commit to ON
	_, err = se.ExecuteSQL(context.Background(), "set tidb_enable_async_commit=ON")
	if err != nil {
		se.Close()
		return nil, err
	}

	// Force rollback the session to guarantee the session is not in any explicit transaction
	if _, err = se.ExecuteSQL(context.Background(), "ROLLBACK"); err != nil {
		se.Close()
		return nil, err
	}

	// set the time zone to UTC
	rows, err := se.ExecuteSQL(context.Background(), "select @@time_zone")
	if err != nil {
		se.Close()
		return nil, err
	}

	if len(rows) == 0 || rows[0].Len() == 0 {
		se.Close()
		return nil, errors.New("failed to get time_zone variable")
	}
	originalTimeZone = rows[0].GetString(0)

	_, err = se.ExecuteSQL(context.Background(), "set @@time_zone='UTC'")
	if err != nil {
		se.Close()
		return nil, err
	}
	restoreTimeZone = true

	return se, nil
}

func newTableSession(se session.Session, tbl *cache.PhysicalTable, expire time.Time) *ttlTableSession {
	return &ttlTableSession{
		Session: se,
		tbl:     tbl,
		expire:  expire,
	}
}

type ttlTableSession struct {
	session.Session
	tbl    *cache.PhysicalTable
	expire time.Time
}

func (s *ttlTableSession) ExecuteSQLWithCheck(ctx context.Context, sql string) ([]chunk.Row, bool, error) {
	tracer := metrics.PhaseTracerFromCtx(ctx)
	defer tracer.EnterPhase(tracer.Phase())

	tracer.EnterPhase(metrics.PhaseOther)
	if !variable.EnableTTLJob.Load() {
		return nil, false, errors.New("global TTL job is disabled")
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
		if validateErr := validateTTLWork(ctx, s.Session, s.tbl, s.expire); validateErr != nil {
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

func validateTTLWork(ctx context.Context, s session.Session, tbl *cache.PhysicalTable, expire time.Time) error {
	curTbl, err := s.SessionInfoSchema().TableByName(tbl.Schema, tbl.Name)
	if err != nil {
		return err
	}

	newTblInfo := curTbl.Meta()
	if tbl.TableInfo == newTblInfo {
		return nil
	}

	if tbl.TableInfo.ID != newTblInfo.ID {
		return errors.New("table id changed")
	}

	newTTLTbl, err := cache.NewPhysicalTable(tbl.Schema, newTblInfo, tbl.Partition)
	if err != nil {
		return err
	}

	if newTTLTbl.ID != tbl.ID {
		return errors.New("physical id changed")
	}

	if tbl.Partition.L != "" {
		if newTTLTbl.PartitionDef.Name.L != tbl.PartitionDef.Name.L {
			return errors.New("partition name changed")
		}
	}

	if !newTTLTbl.TTLInfo.Enable {
		return errors.New("table TTL disabled")
	}

	if newTTLTbl.TimeColumn.Name.L != tbl.TimeColumn.Name.L {
		return errors.New("time column name changed")
	}

	if newTblInfo.TTLInfo.IntervalExprStr != tbl.TTLInfo.IntervalExprStr ||
		newTblInfo.TTLInfo.IntervalTimeUnit != tbl.TTLInfo.IntervalTimeUnit {
		newExpireTime, err := newTTLTbl.EvalExpireTime(ctx, s, s.Now())
		if err != nil {
			return err
		}

		if newExpireTime.Before(expire) {
			return errors.New("expire interval changed")
		}
	}

	return nil
}
