package restore

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	. "github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/memory"
	tmock "github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/trxevents"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

var _ = Suite(&checksumSuite{})

type checksumSuite struct{}

func MockDoChecksumCtx(db *sql.DB) context.Context {
	ctx := context.Background()
	manager := newTiDBChecksumExecutor(db)
	return context.WithValue(ctx, &checksumManagerKey, manager)
}

func (s *checksumSuite) TestDoChecksum(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectQuery("\\QSELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("100h0m0s").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("\\QADMIN CHECKSUM TABLE `test`.`t`\\E").
		WillReturnRows(
			sqlmock.NewRows([]string{"Db_name", "Table_name", "Checksum_crc64_xor", "Total_kvs", "Total_bytes"}).
				AddRow("test", "t", 8520875019404689597, 7296873, 357601387),
		)
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("10m").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)
	checksum, err := DoChecksum(ctx, &TidbTableInfo{DB: "test", Name: "t"})
	c.Assert(err, IsNil)
	c.Assert(*checksum, DeepEquals, RemoteChecksum{
		Schema:     "test",
		Table:      "t",
		Checksum:   8520875019404689597,
		TotalKVs:   7296873,
		TotalBytes: 357601387,
	})

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *checksumSuite) TestDoChecksumParallel(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectQuery("\\QSELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("100h0m0s").
		WillReturnResult(sqlmock.NewResult(1, 1))
	for i := 0; i < 5; i++ {
		mock.ExpectQuery("\\QADMIN CHECKSUM TABLE `test`.`t`\\E").
			WillDelayFor(100 * time.Millisecond).
			WillReturnRows(
				sqlmock.NewRows([]string{"Db_name", "Table_name", "Checksum_crc64_xor", "Total_kvs", "Total_bytes"}).
					AddRow("test", "t", 8520875019404689597, 7296873, 357601387),
			)
	}
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("10m").
		WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)

	// db.Close() will close all connections from its idle pool, set it 1 to expect one close
	db.SetMaxIdleConns(1)
	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			checksum, err := DoChecksum(ctx, &TidbTableInfo{DB: "test", Name: "t"})
			c.Assert(err, IsNil)
			c.Assert(*checksum, DeepEquals, RemoteChecksum{
				Schema:     "test",
				Table:      "t",
				Checksum:   8520875019404689597,
				TotalKVs:   7296873,
				TotalBytes: 357601387,
			})
		}()
	}
	wg.Wait()

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *checksumSuite) TestIncreaseGCLifeTimeFail(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	for i := 0; i < 5; i++ {
		mock.ExpectQuery("\\QSELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
			WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("10m"))
		mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
			WithArgs("100h0m0s").
			WillReturnError(errors.Annotate(context.Canceled, "update gc error"))
	}
	// This recover GC Life Time SQL should not be executed in DoChecksum
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("10m").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)
	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			_, errChecksum := DoChecksum(ctx, &TidbTableInfo{DB: "test", Name: "t"})
			c.Assert(errChecksum, ErrorMatches, "update GC lifetime failed: update gc error: context canceled")
			wg.Done()
		}()
	}
	wg.Wait()

	_, err = db.Exec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E", "10m")
	c.Assert(err, IsNil)

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (s *checksumSuite) TestDoChecksumWithTikv(c *C) {
	// set up mock tikv checksum manager
	pdClient := &testPDClient{}
	resp := tipb.ChecksumResponse{Checksum: 123, TotalKvs: 10, TotalBytes: 1000}
	kvClient := &mockChecksumKVClient{checksum: resp, respDur: time.Millisecond * 200}

	// mock a table info
	p := parser.New()
	se := tmock.NewContext()
	node, err := p.ParseOneStmt("CREATE TABLE `t1` (`c1` varchar(5) NOT NULL)", "utf8mb4", "utf8mb4_bin")
	c.Assert(err, IsNil)
	tableInfo, err := ddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 999)
	c.Assert(err, IsNil)

	for i := 0; i <= maxErrorRetryCount; i++ {
		kvClient.maxErrCount = i
		kvClient.curErrCount = 0
		var checksumTS uint64
		kvClient.onSendReq = func(req *kv.Request) {
			checksumTS = req.StartTs
		}
		checksumExec := &tikvChecksumManager{manager: newGCTTLManager(pdClient), client: kvClient}
<<<<<<< HEAD
		startTS := oracle.ComposeTS(time.Now().Unix()*1000, 0)
		ctx := context.WithValue(context.Background(), &checksumManagerKey, checksumExec)
		_, err = DoChecksum(ctx, &TidbTableInfo{DB: "test", Name: "t", Core: tableInfo})
=======
		physicalTS, logicalTS, err := pdClient.GetTS(ctx)
		require.NoError(t, err)
		subCtx := context.WithValue(ctx, &checksumManagerKey, checksumExec)
		_, err = DoChecksum(subCtx, &TidbTableInfo{DB: "test", Name: "t", Core: tableInfo})
>>>>>>> 5288efa20... lightning: use pd timestamp to update gc safepoint (#32734)
		// with max error retry < maxErrorRetryCount, the checksum can success
		if i >= maxErrorRetryCount {
			c.Assert(err, ErrorMatches, "tikv timeout")
			continue
		} else {
			c.Assert(err, IsNil)
		}

		// after checksum, safepint should be small than start ts
		ts := pdClient.currentSafePoint()
		// 1ms for the schedule deviation
<<<<<<< HEAD
		c.Assert(ts <= startTS+1, IsTrue)
		c.Assert(atomic.LoadUint32(&checksumExec.manager.started) > 0, IsTrue)
=======
		startTS := oracle.ComposeTS(physicalTS+1, logicalTS)
		require.True(t, ts <= startTS+1)
		require.GreaterOrEqual(t, checksumTS, ts)
		require.True(t, checksumExec.manager.started.Load())
		require.Zero(t, checksumExec.manager.currentTS)
		require.Equal(t, 0, len(checksumExec.manager.tableGCSafeTS))
>>>>>>> 5288efa20... lightning: use pd timestamp to update gc safepoint (#32734)
	}
}

func (s *checksumSuite) TestDoChecksumWithTikvErrRetry(c *C) {
}

func (s *checksumSuite) TestDoChecksumWithErrorAndLongOriginalLifetime(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	mock.ExpectQuery("\\QSELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("300h"))
	mock.ExpectQuery("\\QADMIN CHECKSUM TABLE `test`.`t`\\E").
		WillReturnError(errors.Annotate(context.Canceled, "mock syntax error"))
	mock.ExpectExec("\\QUPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'\\E").
		WithArgs("300h").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectClose()

	ctx := MockDoChecksumCtx(db)
	_, err = DoChecksum(ctx, &TidbTableInfo{DB: "test", Name: "t"})
	c.Assert(err, ErrorMatches, "compute remote checksum failed: mock syntax error.*")

	c.Assert(db.Close(), IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

type safePointTTL struct {
	safePoint uint64
	expiredAt int64
}

type testPDClient struct {
	sync.Mutex
	pd.Client
<<<<<<< HEAD
	count       int32
	gcSafePoint []safePointTTL
=======
	count            atomic.Int32
	gcSafePoint      []safePointTTL
	logicalTSCounter atomic.Uint64
>>>>>>> 5288efa20... lightning: use pd timestamp to update gc safepoint (#32734)
}

func (c *testPDClient) currentSafePoint() uint64 {
	ts := time.Now().Unix()
	c.Lock()
	defer c.Unlock()
	for _, s := range c.gcSafePoint {
		if s.expiredAt > ts {
			return s.safePoint
		}
	}
	return 0
}

func (c *testPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	physicalTS := time.Now().UnixNano() / 1e6
	logicalTS := oracle.ExtractLogical(c.logicalTSCounter.Inc())
	return physicalTS, logicalTS, nil
}

func (c *testPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	if !strings.HasPrefix(serviceID, "lightning") {
		panic("service ID must start with 'lightning'")
	}
	atomic.AddInt32(&c.count, 1)
	c.Lock()
	idx := sort.Search(len(c.gcSafePoint), func(i int) bool {
		return c.gcSafePoint[i].safePoint >= safePoint
	})
	sp := c.gcSafePoint
	ttlEnd := time.Now().Unix() + ttl
	spTTL := safePointTTL{safePoint: safePoint, expiredAt: ttlEnd}
	switch {
	case idx >= len(sp):
		c.gcSafePoint = append(c.gcSafePoint, spTTL)
	case sp[idx].safePoint == safePoint:
		if ttlEnd > sp[idx].expiredAt {
			sp[idx].expiredAt = ttlEnd
		}
	default:
		c.gcSafePoint = append(append(sp[:idx], spTTL), sp[idx:]...)
	}
	c.Unlock()
	return c.currentSafePoint(), nil
}

func (s *checksumSuite) TestGcTTLManagerSingle(c *C) {
	pdClient := &testPDClient{}
	manager := newGCTTLManager(pdClient)
	c.Assert(manager.serviceID, Not(Equals), "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldTTL := serviceSafePointTTL
	// set serviceSafePointTTL to 1 second, so lightning will update it in each 1/3 seconds.
	serviceSafePointTTL = 1
	defer func() {
		serviceSafePointTTL = oldTTL
	}()

	err := manager.addOneJob(ctx, "test", uint64(time.Now().Unix()))
	c.Assert(err, IsNil)

	time.Sleep(2*time.Second + 10*time.Millisecond)

	// after 2 seconds, must at least update 5 times
	val := atomic.LoadInt32(&pdClient.count)
	c.Assert(val, GreaterEqual, int32(5))

	// after remove the job, there are no job remain, gc ttl needn't to be updated
	manager.removeOneJob("test")
	time.Sleep(10 * time.Millisecond)
	val = atomic.LoadInt32(&pdClient.count)
	time.Sleep(1*time.Second + 10*time.Millisecond)
	c.Assert(atomic.LoadInt32(&pdClient.count), Equals, val)
}

func (s *checksumSuite) TestGcTTLManagerMulti(c *C) {
	manager := newGCTTLManager(&testPDClient{})
	ctx := context.Background()

	for i := uint64(1); i <= 5; i++ {
		err := manager.addOneJob(ctx, fmt.Sprintf("test%d", i), i)
		c.Assert(err, IsNil)
		c.Assert(manager.currentTS, Equals, uint64(1))
	}

	manager.removeOneJob("test2")
	c.Assert(manager.currentTS, Equals, uint64(1))

	manager.removeOneJob("test1")
	c.Assert(manager.currentTS, Equals, uint64(3))

	manager.removeOneJob("test3")
	c.Assert(manager.currentTS, Equals, uint64(4))

	manager.removeOneJob("test4")
	c.Assert(manager.currentTS, Equals, uint64(5))

	manager.removeOneJob("test5")
	c.Assert(manager.currentTS, Equals, uint64(0))
}

func (s *checksumSuite) TestPdServiceID(c *C) {
	pdCli := &testPDClient{}
	gcTTLManager1 := newGCTTLManager(pdCli)
	c.Assert(gcTTLManager1.serviceID, Matches, "lightning-.*")
	gcTTLManager2 := newGCTTLManager(pdCli)
	c.Assert(gcTTLManager2.serviceID, Matches, "lightning-.*")

	c.Assert(gcTTLManager1.serviceID != gcTTLManager2.serviceID, IsTrue)
}

type mockResponse struct {
	finished bool
	data     []byte
}

func (r *mockResponse) Next(ctx context.Context) (resultSubset kv.ResultSubset, err error) {
	if r.finished {
		return nil, nil
	}
	r.finished = true
	return &mockResultSubset{data: r.data}, nil
}

func (r *mockResponse) Close() error {
	return nil
}

type mockErrorResponse struct {
	err string
}

func (r *mockErrorResponse) Next(ctx context.Context) (resultSubset kv.ResultSubset, err error) {
	return nil, errors.New(r.err)
}

func (r *mockErrorResponse) Close() error {
	return nil
}

type mockResultSubset struct {
	data []byte
}

func (r *mockResultSubset) GetData() []byte {
	return r.data
}

func (r *mockResultSubset) GetStartKey() kv.Key {
	return []byte{}
}

func (r *mockResultSubset) MemSize() int64 {
	return 0
}

func (r *mockResultSubset) RespTime() time.Duration {
	return time.Millisecond
}

type mockChecksumKVClient struct {
	kv.Client
	checksum  tipb.ChecksumResponse
	respDur   time.Duration
	onSendReq func(req *kv.Request)
	// return error count before return success
	maxErrCount int
	curErrCount int
}

// a mock client for checksum request
<<<<<<< HEAD
func (c *mockChecksumKVClient) Send(ctx context.Context, req *kv.Request, vars interface{}, sessionMemTracker *memory.Tracker, enabledRateLimitAction bool, eventCb trxevents.EventCallback) kv.Response {
=======
func (c *mockChecksumKVClient) Send(ctx context.Context, req *kv.Request, vars interface{}, option *kv.ClientSendOption) kv.Response {
	if c.onSendReq != nil {
		c.onSendReq(req)
	}
>>>>>>> 5288efa20... lightning: use pd timestamp to update gc safepoint (#32734)
	if c.curErrCount < c.maxErrCount {
		c.curErrCount++
		return &mockErrorResponse{err: "tikv timeout"}
	}
	data, _ := c.checksum.Marshal()
	time.Sleep(c.respDur)
	return &mockResponse{data: data}
}
