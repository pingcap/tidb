// Copyright 2021 PingCAP, Inc.
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

package local

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tipb/go-tipb"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	preUpdateServiceSafePointFactor = 3
	maxErrorRetryCount              = 3
	defaultGCLifeTime               = 100 * time.Hour
)

var (
	serviceSafePointTTL int64 = 10 * 60 // 10 min in seconds

	// MinDistSQLScanConcurrency is the minimum value of tidb_distsql_scan_concurrency.
	MinDistSQLScanConcurrency = 4

	// DefaultBackoffWeight is the default value of tidb_backoff_weight for checksum.
	// RegionRequestSender will retry within a maxSleep time, default is 2 * 20 = 40 seconds.
	// When TiKV client encounters an error of "region not leader", it will keep
	// retrying every 500 ms, if it still fails after maxSleep, it will return "region unavailable".
	// When there are many pending compaction bytes, TiKV might not respond within 1m,
	// and report "rpcError:wait recvLoop timeout,timeout:1m0s", and retry might
	// time out again.
	// so we enlarge it to 30 * 20 = 10 minutes.
	DefaultBackoffWeight = 15 * tikvstore.DefBackOffWeight
)

// RemoteChecksum represents a checksum result got from tidb.
type RemoteChecksum struct {
	Schema     string
	Table      string
	Checksum   uint64
	TotalKVs   uint64
	TotalBytes uint64
}

// IsEqual checks whether the checksum is equal to the other.
func (rc *RemoteChecksum) IsEqual(other *verification.KVChecksum) bool {
	return rc.Checksum == other.Sum() &&
		rc.TotalKVs == other.SumKVS() &&
		rc.TotalBytes == other.SumSize()
}

// ChecksumManager is a manager that manages checksums.
type ChecksumManager interface {
	Checksum(ctx context.Context, tableInfo *checkpoints.TidbTableInfo) (*RemoteChecksum, error)
}

// fetch checksum for tidb sql client
type tidbChecksumExecutor struct {
	db      *sql.DB
	manager *gcLifeTimeManager
}

var _ ChecksumManager = (*tidbChecksumExecutor)(nil)

// NewTiDBChecksumExecutor creates a new tidb checksum executor.
func NewTiDBChecksumExecutor(db *sql.DB) ChecksumManager {
	return &tidbChecksumExecutor{
		db:      db,
		manager: newGCLifeTimeManager(),
	}
}

func (e *tidbChecksumExecutor) Checksum(ctx context.Context, tableInfo *checkpoints.TidbTableInfo) (*RemoteChecksum, error) {
	var err error
	if err = e.manager.addOneJob(ctx, e.db); err != nil {
		return nil, err
	}

	// set it back finally
	defer e.manager.removeOneJob(ctx, e.db)

	tableName := common.UniqueTable(tableInfo.DB, tableInfo.Name)

	task := log.FromContext(ctx).With(zap.String("table", tableName)).Begin(zap.InfoLevel, "remote checksum")

	conn, err := e.db.Conn(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			task.Warn("close connection failed", zap.Error(err))
		}
	}()
	// ADMIN CHECKSUM TABLE <table>,<table>  example.
	// 	mysql> admin checksum table test.t;
	// +---------+------------+---------------------+-----------+-------------+
	// | Db_name | Table_name | Checksum_crc64_xor  | Total_kvs | Total_bytes |
	// +---------+------------+---------------------+-----------+-------------+
	// | test    | t          | 8520875019404689597 |   7296873 |   357601387 |
	// +---------+------------+---------------------+-----------+-------------+
	backoffWeight, err := common.GetBackoffWeightFromDB(ctx, e.db)
	if err == nil && backoffWeight < DefaultBackoffWeight {
		task.Info("increase tidb_backoff_weight", zap.Int("original", backoffWeight), zap.Int("new", DefaultBackoffWeight))
		// increase backoff weight
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION %s = '%d';", variable.TiDBBackOffWeight, DefaultBackoffWeight)); err != nil {
			task.Warn("set tidb_backoff_weight failed", zap.Error(err))
		} else {
			defer func() {
				if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION %s = '%d';", variable.TiDBBackOffWeight, backoffWeight)); err != nil {
					task.Warn("recover tidb_backoff_weight failed", zap.Error(err))
				}
			}()
		}
	}

	cs := RemoteChecksum{}
	err = common.SQLWithRetry{DB: conn, Logger: task.Logger}.QueryRow(ctx, "compute remote checksum",
		"ADMIN CHECKSUM TABLE "+tableName, &cs.Schema, &cs.Table, &cs.Checksum, &cs.TotalKVs, &cs.TotalBytes,
	)
	dur := task.End(zap.ErrorLevel, err)
	if m, ok := metric.FromContext(ctx); ok {
		m.ChecksumSecondsHistogram.Observe(dur.Seconds())
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &cs, nil
}

type gcLifeTimeManager struct {
	runningJobsLock sync.Mutex
	runningJobs     int
	oriGCLifeTime   string
}

func newGCLifeTimeManager() *gcLifeTimeManager {
	// Default values of three member are enough to initialize this struct
	return &gcLifeTimeManager{}
}

// Pre- and post-condition:
// if m.runningJobs == 0, GC life time has not been increased.
// if m.runningJobs > 0, GC life time has been increased.
// m.runningJobs won't be negative(overflow) since index concurrency is relatively small
func (m *gcLifeTimeManager) addOneJob(ctx context.Context, db *sql.DB) error {
	m.runningJobsLock.Lock()
	defer m.runningJobsLock.Unlock()

	if m.runningJobs == 0 {
		oriGCLifeTime, err := obtainGCLifeTime(ctx, db)
		if err != nil {
			return err
		}
		m.oriGCLifeTime = oriGCLifeTime
		err = increaseGCLifeTime(ctx, m, db)
		if err != nil {
			return err
		}
	}
	m.runningJobs++
	return nil
}

// Pre- and post-condition:
// if m.runningJobs == 0, GC life time has been tried to recovered. If this try fails, a warning will be printed.
// if m.runningJobs > 0, GC life time has not been recovered.
// m.runningJobs won't minus to negative since removeOneJob follows a successful addOneJob.
func (m *gcLifeTimeManager) removeOneJob(ctx context.Context, db *sql.DB) {
	m.runningJobsLock.Lock()
	defer m.runningJobsLock.Unlock()

	m.runningJobs--
	if m.runningJobs == 0 {
		err := updateGCLifeTime(ctx, db, m.oriGCLifeTime)
		if err != nil {
			query := fmt.Sprintf(
				"UPDATE mysql.tidb SET VARIABLE_VALUE = '%s' WHERE VARIABLE_NAME = 'tikv_gc_life_time'",
				m.oriGCLifeTime,
			)
			log.FromContext(ctx).Warn("revert GC lifetime failed, please reset the GC lifetime manually after Lightning completed",
				zap.String("query", query),
				log.ShortError(err),
			)
		}
	}
}

func increaseGCLifeTime(ctx context.Context, manager *gcLifeTimeManager, db *sql.DB) (err error) {
	// checksum command usually takes a long time to execute,
	// so here need to increase the gcLifeTime for single transaction.
	var increaseGCLifeTime bool
	if manager.oriGCLifeTime != "" {
		ori, err := time.ParseDuration(manager.oriGCLifeTime)
		if err != nil {
			return errors.Trace(err)
		}
		if ori < defaultGCLifeTime {
			increaseGCLifeTime = true
		}
	} else {
		increaseGCLifeTime = true
	}

	if increaseGCLifeTime {
		err = updateGCLifeTime(ctx, db, defaultGCLifeTime.String())
		if err != nil {
			return err
		}
	}

	failpoint.Inject("IncreaseGCUpdateDuration", nil)

	return nil
}

// obtainGCLifeTime obtains the current GC lifetime.
func obtainGCLifeTime(ctx context.Context, db *sql.DB) (string, error) {
	var gcLifeTime string
	err := common.SQLWithRetry{DB: db, Logger: log.FromContext(ctx)}.QueryRow(
		ctx,
		"obtain GC lifetime",
		"SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'",
		&gcLifeTime,
	)
	return gcLifeTime, err
}

// updateGCLifeTime updates the current GC lifetime.
func updateGCLifeTime(ctx context.Context, db *sql.DB, gcLifeTime string) error {
	sql := common.SQLWithRetry{
		DB:     db,
		Logger: log.FromContext(ctx).With(zap.String("gcLifeTime", gcLifeTime)),
	}
	return sql.Exec(ctx, "update GC lifetime",
		"UPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'",
		gcLifeTime,
	)
}

// TiKVChecksumManager is a manager that can compute checksum of a table using TiKV.
type TiKVChecksumManager struct {
	client                    kv.Client
	manager                   gcTTLManager
	distSQLScanConcurrency    uint
	backoffWeight             int
	resourceGroupName         string
	explicitRequestSourceType string
}

var _ ChecksumManager = &TiKVChecksumManager{}

// NewTiKVChecksumManager return a new tikv checksum manager
func NewTiKVChecksumManager(client kv.Client, pdClient pd.Client, distSQLScanConcurrency uint, backoffWeight int, resourceGroupName, explicitRequestSourceType string) *TiKVChecksumManager {
	return &TiKVChecksumManager{
		client:                    client,
		manager:                   newGCTTLManager(pdClient),
		distSQLScanConcurrency:    distSQLScanConcurrency,
		backoffWeight:             backoffWeight,
		resourceGroupName:         resourceGroupName,
		explicitRequestSourceType: explicitRequestSourceType,
	}
}

func (e *TiKVChecksumManager) checksumDB(ctx context.Context, tableInfo *checkpoints.TidbTableInfo, ts uint64) (*RemoteChecksum, error) {
	executor, err := checksum.NewExecutorBuilder(tableInfo.Core, ts).
		SetConcurrency(e.distSQLScanConcurrency).
		SetBackoffWeight(e.backoffWeight).
		SetResourceGroupName(e.resourceGroupName).
		SetExplicitRequestSourceType(e.explicitRequestSourceType).
		Build()
	if err != nil {
		return nil, errors.Trace(err)
	}

	distSQLScanConcurrency := int(e.distSQLScanConcurrency)
	for i := 0; i < maxErrorRetryCount; i++ {
		_ = executor.Each(func(request *kv.Request) error {
			request.Concurrency = distSQLScanConcurrency
			return nil
		})
		var execRes *tipb.ChecksumResponse
		execRes, err = executor.Execute(ctx, e.client, func() {})
		if err == nil {
			return &RemoteChecksum{
				Schema:     tableInfo.DB,
				Table:      tableInfo.Name,
				Checksum:   execRes.Checksum,
				TotalBytes: execRes.TotalBytes,
				TotalKVs:   execRes.TotalKvs,
			}, nil
		}

		log.FromContext(ctx).Warn("remote checksum failed", zap.String("db", tableInfo.DB),
			zap.String("table", tableInfo.Name), zap.Error(err),
			zap.Int("concurrency", distSQLScanConcurrency), zap.Int("retry", i))

		// do not retry context.Canceled error
		if !common.IsRetryableError(err) {
			break
		}
		if distSQLScanConcurrency > MinDistSQLScanConcurrency {
			distSQLScanConcurrency = max(distSQLScanConcurrency/2, MinDistSQLScanConcurrency)
		}
	}

	return nil, err
}

var retryGetTSInterval = time.Second

// Checksum implements the ChecksumManager interface.
func (e *TiKVChecksumManager) Checksum(ctx context.Context, tableInfo *checkpoints.TidbTableInfo) (*RemoteChecksum, error) {
	tbl := common.UniqueTable(tableInfo.DB, tableInfo.Name)
	var (
		physicalTS, logicalTS int64
		err                   error
		retryTime             int
	)
	physicalTS, logicalTS, err = e.manager.pdClient.GetTS(ctx)
	for err != nil {
		if !pd.IsLeaderChange(errors.Cause(err)) {
			return nil, errors.Annotate(err, "fetch tso from pd failed")
		}
		retryTime++
		if retryTime%60 == 0 {
			log.FromContext(ctx).Warn("fetch tso from pd failed and retrying",
				zap.Int("retryTime", retryTime),
				zap.Error(err))
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-time.After(retryGetTSInterval):
			physicalTS, logicalTS, err = e.manager.pdClient.GetTS(ctx)
		}
	}
	ts := oracle.ComposeTS(physicalTS, logicalTS)
	if err := e.manager.addOneJob(ctx, tbl, ts); err != nil {
		return nil, errors.Trace(err)
	}
	defer e.manager.removeOneJob(tbl)

	return e.checksumDB(ctx, tableInfo, ts)
}

type tableChecksumTS struct {
	table    string
	gcSafeTS uint64
}

// following function are for implement `heap.Interface`

func (m *gcTTLManager) Len() int {
	return len(m.tableGCSafeTS)
}

func (m *gcTTLManager) Less(i, j int) bool {
	return m.tableGCSafeTS[i].gcSafeTS < m.tableGCSafeTS[j].gcSafeTS
}

func (m *gcTTLManager) Swap(i, j int) {
	m.tableGCSafeTS[i], m.tableGCSafeTS[j] = m.tableGCSafeTS[j], m.tableGCSafeTS[i]
}

func (m *gcTTLManager) Push(x any) {
	m.tableGCSafeTS = append(m.tableGCSafeTS, x.(*tableChecksumTS))
}

func (m *gcTTLManager) Pop() any {
	i := m.tableGCSafeTS[len(m.tableGCSafeTS)-1]
	m.tableGCSafeTS = m.tableGCSafeTS[:len(m.tableGCSafeTS)-1]
	return i
}

type gcTTLManager struct {
	lock     sync.Mutex
	pdClient pd.Client
	// tableGCSafeTS is a binary heap that stored active checksum jobs GC safe point ts
	tableGCSafeTS []*tableChecksumTS
	currentTS     uint64
	serviceID     string
	// 0 for not start, otherwise started
	started atomic.Bool
}

func newGCTTLManager(pdClient pd.Client) gcTTLManager {
	return gcTTLManager{
		pdClient:  pdClient,
		serviceID: fmt.Sprintf("lightning-%s", uuid.New()),
	}
}

func (m *gcTTLManager) addOneJob(ctx context.Context, table string, ts uint64) error {
	// start gc ttl loop if not started yet.
	if m.started.CompareAndSwap(false, true) {
		m.start(ctx)
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	var curTS uint64
	if len(m.tableGCSafeTS) > 0 {
		curTS = m.tableGCSafeTS[0].gcSafeTS
	}
	m.Push(&tableChecksumTS{table: table, gcSafeTS: ts})
	heap.Fix(m, len(m.tableGCSafeTS)-1)
	m.currentTS = m.tableGCSafeTS[0].gcSafeTS
	if curTS == 0 || m.currentTS < curTS {
		return m.doUpdateGCTTL(ctx, m.currentTS)
	}
	return nil
}

func (m *gcTTLManager) removeOneJob(table string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	idx := -1
	for i := 0; i < len(m.tableGCSafeTS); i++ {
		if m.tableGCSafeTS[i].table == table {
			idx = i
			break
		}
	}

	if idx >= 0 {
		l := len(m.tableGCSafeTS)
		m.tableGCSafeTS[idx] = m.tableGCSafeTS[l-1]
		m.tableGCSafeTS = m.tableGCSafeTS[:l-1]
		if l > 1 && idx < l-1 {
			heap.Fix(m, idx)
		}
	}

	var newTS uint64
	if len(m.tableGCSafeTS) > 0 {
		newTS = m.tableGCSafeTS[0].gcSafeTS
	}
	m.currentTS = newTS
}

func (m *gcTTLManager) updateGCTTL(ctx context.Context) error {
	m.lock.Lock()
	currentTS := m.currentTS
	m.lock.Unlock()
	return m.doUpdateGCTTL(ctx, currentTS)
}

func (m *gcTTLManager) doUpdateGCTTL(ctx context.Context, ts uint64) error {
	log.FromContext(ctx).Debug("update PD safePoint limit with TTL",
		zap.Uint64("currnet_ts", ts))
	var err error
	if ts > 0 {
		_, err = m.pdClient.UpdateServiceGCSafePoint(ctx,
			m.serviceID, serviceSafePointTTL, ts)
	}
	return err
}

func (m *gcTTLManager) start(ctx context.Context) {
	// It would be OK since TTL won't be zero, so gapTime should > `0.
	updateGapTime := time.Duration(serviceSafePointTTL) * time.Second / preUpdateServiceSafePointFactor

	updateTick := time.NewTicker(updateGapTime)

	updateGCTTL := func() {
		if err := m.updateGCTTL(ctx); err != nil {
			log.FromContext(ctx).Warn("failed to update service safe point, checksum may fail if gc triggered", zap.Error(err))
		}
	}

	// trigger a service gc ttl at start
	updateGCTTL()
	go func() {
		defer updateTick.Stop()
		for {
			select {
			case <-ctx.Done():
				log.FromContext(ctx).Info("service safe point keeper exited")
				return
			case <-updateTick.C:
				updateGCTTL()
			}
		}
	}()
}
