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
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

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

	exec, ok := resource.(sqlexec.SQLExecutor)
	if !ok {
		pool.Put(resource)
		return nil, errors.Errorf("%T cannot be casted to sqlexec.SQLExecutor", sctx)
	}

	se := session.NewSession(sctx, exec, func() {
		pool.Put(resource)
	})

	if _, err = se.ExecuteSQL(context.Background(), "commit"); err != nil {
		se.Close()
		return nil, err
	}

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

func (s *ttlTableSession) ExecuteSQLWithCheck(ctx context.Context, sql string) (rows []chunk.Row, shouldRetry bool, err error) {
	if !variable.EnableTTLJob.Load() {
		return nil, false, errors.New("global TTL job is disabled")
	}

	if err = s.ResetWithGlobalTimeZone(ctx); err != nil {
		return nil, false, err
	}

	err = s.RunInTxn(ctx, func() error {
		rows, err = s.ExecuteSQL(ctx, sql)
		// We must check the configuration after ExecuteSQL because of MDL and the meta the current transaction used
		// can only be determined after executed one query.
		if validateErr := validateTTLWork(ctx, s.Session, s.tbl, s.expire); validateErr != nil {
			shouldRetry = false
			return errors.Annotatef(validateErr, "table '%s.%s' meta changed, should abort current job", s.tbl.Schema, s.tbl.Name)
		}

		if err != nil {
			shouldRetry = true
			return err
		}
		return nil
	})

	if err != nil {
		return nil, shouldRetry, err
	}

	return rows, false, nil
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
