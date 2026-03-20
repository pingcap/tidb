package ddl

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"

	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	pkdbrepl "github.com/pingcap/tidb/pkg/domain/pkdb_repl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

type unsafeDestroyWorker struct {
	ctx               context.Context
	cli               pd.Client
	httpClient        http.Client
	sessPool          *session.Pool
	minJobIDRefresher *systable.MinJobIDRefresher
	store             kv.Storage
}

func newUnsafeDestroyWorker(
	ctx context.Context,
	sessPool *session.Pool,
	minJobIDRefresher *systable.MinJobIDRefresher,
	store kv.Storage,
) (*unsafeDestroyWorker, error) {
	storeWithPD, ok := store.(kv.StorageWithPD)
	if !ok {
		return nil, fmt.Errorf("unsafe destroy worker requires tikv storage with pd client")
	}
	return &unsafeDestroyWorker{
		ctx:               ctx,
		cli:               storeWithPD.GetPDClient(),
		httpClient:        storeWithPD.GetPDHTTPClient(),
		sessPool:          sessPool,
		minJobIDRefresher: minJobIDRefresher,
		store:             store,
	}, nil
}

// unsafeDestroyNextDDLJobIDKey stores the next DDL job ID for replica cluster's
// unsafe destroy range progress. The value is human-readable int64.
const (
	unsafeDestroyNextDDLJobIDKey = "unsafe_destroy_next_ddl_job_id"
	unsafeDestroyQueryBatchSize  = 1024
)

// getNextJobID gets the next DDL job ID (inclusive) to be processed.
func (s *unsafeDestroyWorker) getNextJobID() (int64, error) {
	valStr, err := s.cli.GetLogReplMetadata(s.ctx, unsafeDestroyNextDDLJobIDKey)
	if err != nil || valStr == "" {
		return 0, err
	}
	return strconv.ParseInt(valStr, 10, 64)
}

// getUntilJobID gets the upper bound DDL job ID (exclusive) to be processed.
func (s *unsafeDestroyWorker) getUntilJobID() (int64, error) {
	// because we plan to maintain a monotonically increasing next DDL job ID, we
	// need to stop on minimum running DDL job ID. GetCurrMinJobID will promise that
	// in future there won't be jobs with ID smaller than it.
	minRunningID := s.minJobIDRefresher.GetCurrMinJobID()
	if minRunningID != 0 {
		return minRunningID, nil
	}
	return math.MaxInt64, nil
}

// UnsafeDestroyJob is exported for test.
type UnsafeDestroyJob struct {
	DDLJobID int64
	StartKey []byte
	EndKey   []byte
}

// MySQLDB is the MySQL system database name. Exported for test.
var MySQLDB = "mysql"

// PaginateListSortedSourceDoneJobs is exported for test.
func PaginateListSortedSourceDoneJobs(
	ctx context.Context,
	sessInTnx *session.Session,
	startID, endID int64,
	pageSize int,
	gcSafePoint uint64,
) (ret []UnsafeDestroyJob, err error) {
	lastJobID := startID
	lastElemID := int64(math.MinInt64)
	for lastJobID < endID {
		sql := fmt.Sprintf(`SELECT job_id, element_id, start_key, end_key, ts
			FROM %s.gc_delete_range_done
			WHERE (job_id, element_id) > (%d, %d) AND job_id < %d
			ORDER BY job_id ASC, element_id ASC
			LIMIT %d`,
			MySQLDB,
			lastJobID, lastElemID, endID,
			pageSize)
		rows, err2 := sessInTnx.Execute(ctx, sql, "list-done-unsafe-destroy-jobs")
		if err2 != nil {
			return nil, err2
		}
		for _, row := range rows {
			jobID := row.GetInt64(0)
			startKey := row.GetBytes(2)
			endKey := row.GetBytes(3)
			ts := row.GetUint64(4)
			if ts >= gcSafePoint {
				// because we process jobs in order, once we find a job that its ts
				// is not safe to delete, we need to break on it.
				break
			}
			ret = append(ret, UnsafeDestroyJob{
				DDLJobID: jobID,
				StartKey: startKey,
				EndKey:   endKey,
			})
		}
		if len(rows) < pageSize {
			break
		}

		lastJobID = rows[len(rows)-1].GetInt64(0)
		lastElemID = rows[len(rows)-1].GetInt64(1)
	}
	return ret, nil
}

// PaginateListSourceQueuingJobIDs lists job IDs from gc_delete_range table with pagination.
// It is exported for test.
func PaginateListSourceQueuingJobIDs(
	ctx context.Context,
	sessInTnx *session.Session,
	startID, endID int64,
	pageSize int,
) (ret map[int64]struct{}, err error) {
	ret = make(map[int64]struct{})
	for lastJobID := startID; lastJobID < endID; {
		sql := fmt.Sprintf(`SELECT DISTINCT job_id
			FROM %s.gc_delete_range
			WHERE job_id >= %d AND job_id < %d
			ORDER BY job_id ASC
			LIMIT %d`,
			MySQLDB,
			lastJobID, endID,
			pageSize)
		rows, err2 := sessInTnx.Execute(ctx, sql, "list-queuing-unsafe-destroy-job-ids")
		if err2 != nil {
			return nil, err2
		}
		for _, row := range rows {
			jobID := row.GetInt64(0)
			ret[jobID] = struct{}{}
		}
		if len(rows) < pageSize {
			break
		}
		lastJobID = rows[len(rows)-1].GetInt64(0) + 1
	}
	return ret, nil
}

func (s *unsafeDestroyWorker) listSortedSourceTotallyFinishedJobs(startID, endID int64) ([]UnsafeDestroyJob, error) {
	allSafePoints, err := s.httpClient.GetGCSafePoint(s.ctx)
	if err != nil {
		return nil, err
	}
	gcSafePoint := allSafePoints.GCSafePoint

	se, err := s.sessPool.Get()
	if err != nil {
		return nil, err
	}
	defer s.sessPool.Put(se)

	sess := session.NewSession(se)

	var sortedTotallyFinishedJobs []UnsafeDestroyJob
	err = sess.RunInTxn(func(sqlSess *session.Session) error {
		sortedFinishedJobs, err2 := PaginateListSortedSourceDoneJobs(
			s.ctx,
			sqlSess,
			startID,
			endID,
			unsafeDestroyQueryBatchSize,
			gcSafePoint,
		)
		if err2 != nil {
			return err2
		}

		notFinishJobIDs, err3 := PaginateListSourceQueuingJobIDs(
			s.ctx,
			sqlSess,
			startID,
			endID,
			unsafeDestroyQueryBatchSize,
		)
		if err3 != nil {
			return err3
		}

		capacity := len(sortedFinishedJobs) - len(notFinishJobIDs)
		if capacity < 0 {
			capacity = 0
		}

		sortedTotallyFinishedJobs = make([]UnsafeDestroyJob, 0, capacity)
		for _, job := range sortedFinishedJobs {
			// because we process jobs in order, once we find a not-finished job,
			// we need to break on it.
			if _, ok := notFinishJobIDs[job.DDLJobID]; ok {
				break
			}
			sortedTotallyFinishedJobs = append(sortedTotallyFinishedJobs, job)
		}
		return nil
	})

	return sortedTotallyFinishedJobs, err
}

func (s *unsafeDestroyWorker) runJobs(sortedJobs []UnsafeDestroyJob) error {
	startIdx := 0
	for endIdx := 1; endIdx < len(sortedJobs); endIdx++ {
		if sortedJobs[endIdx].DDLJobID > sortedJobs[startIdx].DDLJobID {
			if err2 := s.runJobsOfOneDDL(sortedJobs[startIdx:endIdx]); err2 != nil {
				return err2
			}
			startIdx = endIdx
		}
	}
	return s.runJobsOfOneDDL(sortedJobs[startIdx:])
}

// DoUnsafeDestroyRangeRequest will be set by gcworker package.
var DoUnsafeDestroyRangeRequest func(
	ctx context.Context,
	startKey []byte,
	endKey []byte,
	pdCli pd.Client,
	tikvStore tikv.Storage,
	uuid string,
) error

func (s *unsafeDestroyWorker) runJobsOfOneDDL(jobs []UnsafeDestroyJob) error {
	if len(jobs) == 0 {
		return nil
	}

	logutil.BgLogger().Info("will run unsafe destroy jobs of one DDL",
		zap.Int64("DDLJobID", jobs[0].DDLJobID),
		zap.Int("numJobs", len(jobs)))

	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(s.ctx)
	// TODO(lance6716): mimic (w *GCWorker) calcDeleteRangeConcurrency
	eg.SetLimit(4)
	for i, job := range jobs {
		eg.Go(func() error {
			storeWithPD := s.store.(kv.StorageWithPD)
			return DoUnsafeDestroyRangeRequest(
				egCtx,
				job.StartKey,
				job.EndKey,
				storeWithPD.GetPDClient(),
				s.store.(tikv.Storage),
				fmt.Sprintf("unsafe-destroy-%d-%d", job.DDLJobID, i),
			)
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}
	err = s.setNextJobID(jobs[0].DDLJobID + 1)
	if err != nil {
		return err
	}
	logutil.BgLogger().Info("finished unsafe destroy jobs of one DDL",
		zap.Int64("DDLJobID", jobs[0].DDLJobID),
		zap.Int("numJobs", len(jobs)))
	return nil
}

// setNextJobID sets the next DDL job ID (inclusive) to be processed.
func (s *unsafeDestroyWorker) setNextJobID(nextID int64) error {
	return s.cli.SetLogReplMetadata(s.ctx, unsafeDestroyNextDDLJobIDKey, strconv.FormatInt(nextID, 10))
}

type unsafeDestroyMgr struct {
	ctx               context.Context
	sessPool          *session.Pool
	minJobIDRefresher *systable.MinJobIDRefresher
	store             kv.Storage

	worker *unsafeDestroyWorker
	wg     util.WaitGroupWrapper
}

func newUnsafeDestroyMgr(
	ctx context.Context,
	sessPool *session.Pool,
	minJobIDRefresher *systable.MinJobIDRefresher,
	store kv.Storage,
) *unsafeDestroyMgr {
	return &unsafeDestroyMgr{
		ctx:               ctx,
		sessPool:          sessPool,
		minJobIDRefresher: minJobIDRefresher,
		store:             store,
	}
}

func (r *unsafeDestroyMgr) start() {
	r.wg.RunWithLog(func() {
		for {
			select {
			case <-r.ctx.Done():
				return
			case <-pkdbrepl.StandbyUnsafeDestroyTickCh:
				r.runOnce()
			}
		}
	})
}

func (r *unsafeDestroyMgr) runOnce() {
	replUnsafeDestroyMu.Lock()
	defer replUnsafeDestroyMu.Unlock()
	if replUnsafeDestroyShouldExit {
		return
	}

	if r.worker == nil {
		var err error
		r.worker, err = newUnsafeDestroyWorker(
			r.ctx,
			r.sessPool,
			r.minJobIDRefresher,
			r.store,
		)
		if err != nil {
			logutil.BgLogger().Error("failed to create unsafe destroy worker",
				zap.Error(err))
			return
		}
	}

	nextJobID, err := r.worker.getNextJobID()
	if err != nil {
		logutil.BgLogger().Error("failed to get next unsafe destroy job ID",
			zap.Error(err))
		return
	}
	untilJobID, err := r.worker.getUntilJobID()
	if err != nil {
		logutil.BgLogger().Error("failed to get until unsafe destroy job ID",
			zap.Error(err))
		return
	}
	// TODO(lance6716): should check self cluster's GC safepoint after respecting min
	// start TS. Filter on ts column
	sortedJobs, err := r.worker.listSortedSourceTotallyFinishedJobs(nextJobID, untilJobID)
	if err != nil {
		logutil.BgLogger().Error("failed to list finished unsafe destroy jobs",
			zap.Error(err))
		return
	}
	if err2 := r.worker.runJobs(sortedJobs); err2 != nil {
		logutil.BgLogger().Error("failed to run unsafe destroy jobs",
			zap.Error(err2))
	}
}

var (
	replUnsafeDestroyMu         sync.Mutex
	replUnsafeDestroyShouldExit bool
)

func (r *unsafeDestroyMgr) close() {
	r.wg.Wait()
}

// CleanupUnsafeDestroyData is called when replication stopped.
func CleanupUnsafeDestroyData(ctx context.Context, cli pd.Client) error {
	replUnsafeDestroyMu.Lock()
	defer replUnsafeDestroyMu.Unlock()
	replUnsafeDestroyShouldExit = true
	return cli.DeleteLogReplMetadata(ctx, unsafeDestroyNextDDLJobIDKey)
}

type mockStore4UnsafeDestroyWorker struct {
	kv.StorageWithPD
	tikv.Storage
}

func (m mockStore4UnsafeDestroyWorker) GetPDClient() pd.Client {
	return nil
}

func (m mockStore4UnsafeDestroyWorker) GetPDHTTPClient() http.Client {
	return nil
}

func (m mockStore4UnsafeDestroyWorker) Begin(...tikv.TxnOption) (kv.Transaction, error) {
	return nil, nil
}

func (m mockStore4UnsafeDestroyWorker) GetSnapshot(kv.Version) kv.Snapshot {
	return nil
}

func (m mockStore4UnsafeDestroyWorker) GetClient() kv.Client {
	return nil
}

func (m mockStore4UnsafeDestroyWorker) GetMPPClient() kv.MPPClient {
	return nil
}

func (m mockStore4UnsafeDestroyWorker) CurrentVersion(string) (kv.Version, error) {
	return kv.Version{}, nil
}

func (m mockStore4UnsafeDestroyWorker) Name() string {
	return ""
}

func (m mockStore4UnsafeDestroyWorker) Describe() string {
	return ""
}

func (m mockStore4UnsafeDestroyWorker) ShowStatus(context.Context, string) (any, error) {
	return nil, nil
}

func (m mockStore4UnsafeDestroyWorker) GetMemCache() kv.MemManager {
	return nil
}

func (m mockStore4UnsafeDestroyWorker) GetMinSafeTS(string) uint64 {
	return 0
}

func (m mockStore4UnsafeDestroyWorker) GetLockWaits() ([]*deadlockpb.WaitForEntry, error) {
	return nil, nil
}

func (m mockStore4UnsafeDestroyWorker) GetCodec() tikv.Codec {
	return nil
}

func (m mockStore4UnsafeDestroyWorker) SetOption(any, any) {}

func (m mockStore4UnsafeDestroyWorker) GetOption(any) (any, bool) {
	return nil, false
}

func (m mockStore4UnsafeDestroyWorker) GetClusterID() uint64 {
	return 0
}

func (m mockStore4UnsafeDestroyWorker) GetKeyspace() string {
	return ""
}

var _ kv.Storage = mockStore4UnsafeDestroyWorker{}

func newMockUnsafeDestroyWorker(
	ctx context.Context,
	cli pd.Client,
) *unsafeDestroyWorker {
	return &unsafeDestroyWorker{
		ctx:   ctx,
		cli:   cli,
		store: mockStore4UnsafeDestroyWorker{},
	}
}
