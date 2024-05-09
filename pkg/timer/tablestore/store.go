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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
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

	sql, args, err := buildInsertTimerSQL(s.dbName, s.tblName, record)
	if err != nil {
		return "", err
	}

	exec := sctx.GetSQLExecutor()
	_, err = executeSQL(ctx, exec, sql, args...)
	if err != nil {
		return "", err
	}

	rows, err := executeSQL(ctx, exec, "select @@last_insert_id")
	if err != nil {
		return "", err
	}

	timerID := strconv.FormatUint(rows[0].GetUint64(0), 10)
	s.notifier.Notify(api.WatchTimerEventCreate, timerID)
	return timerID, nil
}

func (s *tableTimerStoreCore) List(ctx context.Context, cond api.Cond) ([]*api.TimerRecord, error) {
	sctx, back, err := s.takeSession()
	if err != nil {
		return nil, err
	}
	defer back()

	if sessVars := sctx.GetSessionVars(); sessVars.GetEnableIndexMerge() {
		// Enable index merge is used to make sure filtering timers with tags quickly.
		// Currently, we are using multi-value index to index tags for timers which requires index merge enabled.
		// see: https://docs.pingcap.com/tidb/dev/choose-index#use-a-multi-valued-index
		sessVars.SetEnableIndexMerge(true)
		defer sessVars.SetEnableIndexMerge(false)
	}

	seTZ := sctx.GetSessionVars().Location()
	sql, args, err := buildSelectTimerSQL(s.dbName, s.tblName, cond)
	if err != nil {
		return nil, err
	}

	exec := sctx.GetSQLExecutor()
	rows, err := executeSQL(ctx, exec, sql, args...)
	if err != nil {
		return nil, err
	}

	tidbTimeZone, err := sctx.GetSessionVars().GetGlobalSystemVar(ctx, variable.TimeZone)
	if err != nil {
		return nil, err
	}

	timers := make([]*api.TimerRecord, 0, len(rows))
	for _, row := range rows {
		var timerData []byte
		if !row.IsNull(3) {
			timerData = row.GetBytes(3)
		}

		tz := row.GetString(4)
		tzParse := tz
		// handling value "TIDB" is for compatibility of version 7.3.0
		if tz == "" || strings.EqualFold(tz, "TIDB") {
			tzParse = tidbTimeZone
		}

		loc, err := timeutil.ParseTimeZone(tzParse)
		if err != nil {
			loc = timeutil.SystemLocation()
		}

		var watermark time.Time
		if !row.IsNull(8) {
			watermark, err = row.GetTime(8).GoTime(seTZ)
			if err != nil {
				return nil, err
			}
			watermark = watermark.In(loc)
		}

		var ext timerExt
		if !row.IsNull(10) {
			extJSON := row.GetJSON(10).String()
			if err = json.Unmarshal([]byte(extJSON), &ext); err != nil {
				return nil, err
			}
		}

		var eventData []byte
		if !row.IsNull(13) {
			eventData = row.GetBytes(13)
		}

		var eventStart time.Time
		if !row.IsNull(14) {
			eventStart, err = row.GetTime(14).GoTime(seTZ)
			if err != nil {
				return nil, err
			}
			eventStart = eventStart.In(loc)
		}

		var summaryData []byte
		if !row.IsNull(15) {
			summaryData = row.GetBytes(15)
		}

		var createTime time.Time
		if !row.IsNull(16) {
			createTime, err = row.GetTime(16).GoTime(seTZ)
			if err != nil {
				return nil, err
			}
			createTime = createTime.In(loc)
		}

		timer := &api.TimerRecord{
			ID: strconv.FormatUint(row.GetUint64(0), 10),
			TimerSpec: api.TimerSpec{
				Namespace:       row.GetString(1),
				Key:             row.GetString(2),
				Tags:            ext.Tags,
				Data:            timerData,
				TimeZone:        tz,
				SchedPolicyType: api.SchedPolicyType(row.GetString(5)),
				SchedPolicyExpr: row.GetString(6),
				HookClass:       row.GetString(7),
				Watermark:       watermark,
				Enable:          row.GetInt64(9) != 0,
			},
			ManualRequest: ext.Manual.ToManualRequest(),
			EventStatus:   api.SchedEventStatus(row.GetString(11)),
			EventID:       row.GetString(12),
			EventData:     eventData,
			EventStart:    eventStart,
			EventExtra:    ext.Event.ToEventExtra(),
			SummaryData:   summaryData,
			Location:      loc,
			CreateTime:    createTime,
			Version:       row.GetUint64(18),
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

	exec := sctx.GetSQLExecutor()
	err = runInTxn(ctx, exec, func() error {
		/* #nosec G202: SQL string concatenation */
		getCheckColsSQL := fmt.Sprintf(
			"SELECT EVENT_ID, VERSION, SCHED_POLICY_TYPE, SCHED_POLICY_EXPR FROM %s WHERE ID=%%?",
			indentString(s.dbName, s.tblName),
		)

		rows, err := executeSQL(ctx, exec, getCheckColsSQL, timerID)
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

		updateSQL, args, err := buildUpdateTimerSQL(s.dbName, s.tblName, timerID, update)
		if err != nil {
			return err
		}

		if _, err = executeSQL(ctx, exec, updateSQL, args...); err != nil {
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

	exec := sctx.GetSQLExecutor()
	deleteSQL, args := buildDeleteTimerSQL(s.dbName, s.tblName, timerID)
	_, err = executeSQL(ctx, exec, deleteSQL, args...)
	if err != nil {
		return false, err
	}

	rows, err := executeSQL(ctx, exec, "SELECT ROW_COUNT()")
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

func (s *tableTimerStoreCore) takeSession() (_ sessionctx.Context, _ func(), err error) {
	r, err := s.pool.Get()
	if err != nil {
		return nil, nil, err
	}

	defer func() {
		if err != nil {
			s.pool.Put(r)
		}
	}()

	sctx, ok := r.(sessionctx.Context)
	if !ok {
		return nil, nil, errors.New("session is not the type sessionctx.Context")
	}

	ctx := context.Background()

	exec := sctx.GetSQLExecutor()
	// rollback first to terminate unexpected transactions
	if _, err = executeSQL(ctx, exec, "ROLLBACK"); err != nil {
		return nil, nil, err
	}

	// we should force to set time zone to UTC to make sure time operations are consistent.
	rows, err := executeSQL(ctx, exec, "SELECT @@time_zone")
	if err != nil {
		return nil, nil, err
	}

	if len(rows) == 0 || rows[0].Len() == 0 {
		return nil, nil, errors.New("failed to get original time zone of session")
	}

	if _, err = executeSQL(ctx, exec, "SET @@time_zone='UTC'"); err != nil {
		return nil, nil, err
	}

	originalTimeZone := rows[0].GetString(0)
	back := func() {
		if _, err = executeSQL(ctx, exec, "ROLLBACK"); err != nil {
			// Though this branch is rarely to be called because "ROLLBACK" will always be successfully, we still need
			// to handle it here to make sure the code is strong.
			terror.Log(err)
			// call `r.Close()` to make sure the resource is released to avoid memory leak
			r.Close()
			return
		}

		if _, err = executeSQL(ctx, exec, "SET @@time_zone=%?", originalTimeZone); err != nil {
			terror.Log(err)
			r.Close()
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

	if val, ok := update.TimeZone.Get(); ok {
		if err := api.ValidateTimeZone(val); err != nil {
			return err
		}
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

func executeSQL(ctx context.Context, exec sqlexec.SQLExecutor, sql string, args ...any) ([]chunk.Row, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalTimer)
	rs, err := exec.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	if rs == nil {
		return nil, nil
	}

	defer terror.Call(rs.Close)
	return sqlexec.DrainRecordSet(ctx, rs, 1)
}

func runInTxn(ctx context.Context, exec sqlexec.SQLExecutor, fn func() error) error {
	if _, err := executeSQL(ctx, exec, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}

	success := false
	defer func() {
		if !success {
			_, err := executeSQL(ctx, exec, "ROLLBACK")
			terror.Log(err)
		}
	}()

	if err := fn(); err != nil {
		return err
	}

	if _, err := executeSQL(ctx, exec, "COMMIT"); err != nil {
		return err
	}

	success = true
	return nil
}
