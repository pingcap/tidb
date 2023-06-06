// Copyright 2023 PingCAP, Inc.
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

package tablestore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/timer/api"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

type tableTimerStoreCore struct {
	pool     sessionPool
	dbName   string
	tblName  string
	etcd     *clientv3.Client
	notifier api.TimerWatchEventNotifier
}

// NewTableTimerStore create a new timer store based on table
func NewTableTimerStore(clusterID uint64, pool sessionPool, dbName, tblName string, etcd *clientv3.Client) *api.TimerStore {
	var notifier api.TimerWatchEventNotifier
	if etcd != nil {
		notifier = NewEtcdNotifier(clusterID, etcd)
	} else {
		notifier = api.NewMemTimerWatchEventNotifier()
	}

	return &api.TimerStore{
		TimerStoreCore: &tableTimerStoreCore{
			pool:     pool,
			dbName:   dbName,
			tblName:  tblName,
			notifier: notifier,
		},
	}
}

func (s *tableTimerStoreCore) Create(ctx context.Context, record *api.TimerRecord) (string, error) {
	if record == nil {
		return "", errors.New("timer should not be nil")
	}

	if record.ID != "" {
		return "", errors.New("ID should not be specified when create record")
	}

	if record.Version != 0 {
		return "", errors.New("Version should not be specified when create record")
	}

	if !record.CreateTime.IsZero() {
		return "", errors.New("CreateTime should not be specified when create record")
	}

	if err := record.Validate(); err != nil {
		return "", err
	}

	sctx, back, err := s.takeSession()
	if err != nil {
		return "", err
	}
	defer back()

	sql, args := buildInsertTimerSQL(s.dbName, s.tblName, record)
	_, err = executeSQL(ctx, sctx, sql, args...)
	if err != nil {
		return "", err
	}

	rows, err := executeSQL(ctx, sctx, "select @@last_insert_id")
	if err != nil {
		return "", err
	}

	timerID := rows[0].GetString(0)
	s.notifier.Notify(api.WatchTimerEventCreate, timerID)
	return timerID, nil
}

func (s *tableTimerStoreCore) List(ctx context.Context, cond api.Cond) ([]*api.TimerRecord, error) {
	sctx, back, err := s.takeSession()
	if err != nil {
		return nil, err
	}
	defer back()

	seTZ := sctx.GetSessionVars().Location()
	sql, args, err := buildSelectTimerSQL(s.dbName, s.tblName, cond)
	if err != nil {
		return nil, err
	}

	rows, err := executeSQL(ctx, sctx, sql, args...)
	if err != nil {
		return nil, err
	}

	timers := make([]*api.TimerRecord, 0, len(rows))
	for _, row := range rows {
		var timerData []byte
		if !row.IsNull(3) {
			timerData = row.GetBytes(3)
		}

		var watermark time.Time
		if !row.IsNull(8) {
			watermark, err = row.GetTime(8).GoTime(seTZ)
			if err != nil {
				return nil, err
			}
		}

		var eventData []byte
		if !row.IsNull(12) {
			eventData = row.GetBytes(12)
		}

		var eventStart time.Time
		if !row.IsNull(13) {
			eventStart, err = row.GetTime(13).GoTime(seTZ)
			if err != nil {
				return nil, err
			}
		}

		var summaryData []byte
		if !row.IsNull(14) {
			summaryData = row.GetBytes(14)
		}

		var createTime time.Time
		if !row.IsNull(15) {
			createTime, err = row.GetTime(15).GoTime(seTZ)
			if err != nil {
				return nil, err
			}
		}

		timer := &api.TimerRecord{
			ID: strconv.FormatUint(row.GetUint64(0), 10),
			TimerSpec: api.TimerSpec{
				Namespace:       row.GetString(1),
				Key:             row.GetString(2),
				Data:            timerData,
				SchedPolicyType: api.SchedPolicyType(row.GetString(5)),
				SchedPolicyExpr: row.GetString(6),
				HookClass:       row.GetString(7),
				Watermark:       watermark,
				Enable:          row.GetInt64(9) != 0,
			},
			EventStatus: api.SchedEventStatus(row.GetString(10)),
			EventID:     row.GetString(11),
			EventData:   eventData,
			EventStart:  eventStart,
			SummaryData: summaryData,
			CreateTime:  createTime,
			Version:     row.GetUint64(17),
		}
		timers = append(timers, timer)
	}
	return timers, nil
}

func (s *tableTimerStoreCore) Update(ctx context.Context, timerID string, update *api.TimerUpdate) error {
	sctx, back, err := s.takeSession()
	if err != nil {
		return err
	}
	defer back()

	err = runInTxn(ctx, sctx, func() error {
		/* #nosec G202: SQL string concatenation */
		getCheckColsSQL := fmt.Sprintf(
			"SELECT EVENT_ID, VERSION, SCHED_POLICY_TYPE, SCHED_POLICY_EXPR FROM %s WHERE ID=%%?",
			indentString(s.dbName, s.tblName),
		)

		rows, err := executeSQL(ctx, sctx, getCheckColsSQL, timerID)
		if err != nil {
			return err
		}

		if len(rows) == 0 {
			return api.ErrTimerNotExist
		}

		err = checkUpdateConstraints(
			update,
			rows[0].GetString(0),
			rows[0].GetUint64(1),
			api.SchedPolicyType(rows[0].GetString(2)),
			rows[0].GetString(3),
		)

		if err != nil {
			return err
		}

		updateSQL, args := buildUpdateTimerSQL(s.dbName, s.tblName, timerID, update)
		if _, err = executeSQL(ctx, sctx, updateSQL, args...); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	s.notifier.Notify(api.WatchTimerEventUpdate, timerID)
	return nil
}

func (s *tableTimerStoreCore) Delete(ctx context.Context, timerID string) (bool, error) {
	sctx, back, err := s.takeSession()
	if err != nil {
		return false, err
	}
	defer back()

	deleteSQL, args := buildDeleteTimerSQL(s.dbName, s.tblName, timerID)
	_, err = executeSQL(ctx, sctx, deleteSQL, args...)
	if err != nil {
		return false, err
	}

	rows, err := executeSQL(ctx, sctx, "SELECT ROW_COUNT()")
	if err != nil {
		return false, err
	}

	exist := rows[0].GetInt64(0) > 0
	if exist {
		s.notifier.Notify(api.WatchTimerEventDelete, timerID)
	}
	return exist, nil
}

func (*tableTimerStoreCore) WatchSupported() bool {
	return true
}

func (s *tableTimerStoreCore) Watch(ctx context.Context) api.WatchTimerChan {
	return s.notifier.Watch(ctx)
}

func (s *tableTimerStoreCore) Close() {
	s.notifier.Close()
}

func (s *tableTimerStoreCore) takeSession() (sessionctx.Context, func(), error) {
	r, err := s.pool.Get()
	if err != nil {
		return nil, nil, err
	}

	sctx, ok := r.(sessionctx.Context)
	if !ok {
		s.pool.Put(r)
		return nil, nil, errors.New("session is not the type sessionctx.Context")
	}

	back := func() {
		if _, err = executeSQL(context.Background(), sctx, "ROLLBACK"); err != nil {
			terror.Log(err)
			return
		}
		s.pool.Put(r)
	}

	return sctx, back, nil
}

func checkUpdateConstraints(update *api.TimerUpdate, eventID string, version uint64, policy api.SchedPolicyType, expr string) error {
	if val, ok := update.CheckEventID.Get(); ok && eventID != val {
		return api.ErrEventIDNotMatch
	}

	if val, ok := update.CheckVersion.Get(); ok && version != val {
		return api.ErrVersionNotMatch
	}

	checkPolicy := false
	if val, ok := update.SchedPolicyType.Get(); ok {
		checkPolicy = true
		policy = val
	}

	if val, ok := update.SchedPolicyExpr.Get(); ok {
		checkPolicy = true
		expr = val
	}

	if checkPolicy {
		if _, err := api.CreateSchedEventPolicy(policy, expr); err != nil {
			return errors.Wrap(err, "schedule event configuration is not valid")
		}
	}

	return nil
}

func executeSQL(ctx context.Context, sctx sessionctx.Context, sql string, args ...any) ([]chunk.Row, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalTimer)
	sqlExec, ok := sctx.(sqlexec.SQLExecutor)
	if !ok {
		return nil, errors.New("session is not the type of SQLExecutor")
	}

	rs, err := sqlExec.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	if rs == nil {
		return nil, nil
	}

	defer terror.Call(rs.Close)
	return sqlexec.DrainRecordSet(ctx, rs, 1)
}

func runInTxn(ctx context.Context, sctx sessionctx.Context, fn func() error) error {
	if _, err := executeSQL(ctx, sctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}

	success := false
	defer func() {
		if !success {
			_, err := executeSQL(ctx, sctx, "ROLLBACK")
			terror.Log(err)
		}
	}()

	if err := fn(); err != nil {
		return err
	}

	if _, err := executeSQL(ctx, sctx, "COMMIT"); err != nil {
		return err
	}

	success = true
	return nil
}
