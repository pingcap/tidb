package autoid

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/jamiealquiza/tachymeter"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/autoid"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/mathutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	// "github.com/jamiealquiza/tachymeter"
)

var (
	ErrAutoincReadFailed = errors.New("auto increment action failed")
)

type autoIDKey struct {
	dbID  int64
	tblID int64
}

type autoIDValue struct {
	base int64
	end  int64
	//	isUnsigned bool
	token chan struct{}
}

func (alloc *autoIDValue) alloc4Unsigned(ctx context.Context, store kv.Storage, dbID, tblID int64, isUnsigned bool,
	n uint64, increment, offset int64) (min int64, max int64, err error) {
	// Check offset rebase if necessary.
	if uint64(offset-1) > uint64(alloc.base) {
		if err := alloc.rebase4Unsigned(ctx, store, dbID, tblID, uint64(offset-1)); err != nil {
			return 0, 0, err
		}
	}
	// CalcNeededBatchSize calculates the total batch size needed.
	n1 := calcNeededBatchSize(alloc.base, int64(n), increment, offset, isUnsigned)

	// Condition alloc.base+n1 > alloc.end will overflow when alloc.base + n1 > MaxInt64. So need this.
	// if math.MaxUint64-uint64(alloc.base) <= uint64(n1) {
	// 	fmt.Println("here ???????????????????????????????/")
	// 	return 0, 0, ErrAutoincReadFailed
	// }
	// The local rest is not enough for alloc, skip it.
	if uint64(alloc.base)+uint64(n1) > uint64(alloc.end) {
		var newBase, newEnd int64
		nextStep := int64(batch)
		// Although it may skip a segment here, we still treat it as consumed.

		ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
		err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
			if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
				span1 := span.Tracer().StartSpan("alloc.alloc4Unsigned", opentracing.ChildOf(span.Context()))
				defer span1.Finish()
				opentracing.ContextWithSpan(ctx, span1)
			}
			idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).RowID()
			var err1 error
			newBase, err1 = idAcc.Get()
			if err1 != nil {
				return err1
			}
			// CalcNeededBatchSize calculates the total batch size needed on new base.
			n1 = calcNeededBatchSize(newBase, int64(n), increment, offset, isUnsigned)
			// Although the step is customized by user, we still need to make sure nextStep is big enough for insert batch.
			if nextStep < n1 {
				nextStep = n1
			}
			tmpStep := int64(mathutil.Min(math.MaxUint64-uint64(newBase), uint64(nextStep)))
			// The global rest is not enough for alloc.
			if tmpStep < n1 {
				return ErrAutoincReadFailed
			}
			newEnd, err1 = idAcc.Inc(tmpStep)
			return err1
		})
		if err != nil {
			return 0, 0, err
		}
		if uint64(newBase) == math.MaxUint64 {
			return 0, 0, ErrAutoincReadFailed
		}
		alloc.base, alloc.end = newBase, newEnd
	}
	min = alloc.base
	// Use uint64 n directly.
	alloc.base = int64(uint64(alloc.base) + uint64(n1))
	return min, alloc.base, nil
}

func (alloc *autoIDValue) alloc4Signed(ctx context.Context,
	store kv.Storage,
	dbID, tblID int64,
	isUnsigned bool,
	n uint64, increment, offset int64) (min int64, max int64, err error) {
	// Check offset rebase if necessary.
	if offset-1 > alloc.base {
		if err := alloc.rebase4Signed(ctx, store, dbID, tblID, offset-1); err != nil {
			return 0, 0, err
		}
	}
	// calcNeededBatchSize calculates the total batch size needed.
	n1 := calcNeededBatchSize(alloc.base, int64(n), increment, offset, isUnsigned)

	// Condition alloc.base+N1 > alloc.end will overflow when alloc.base + N1 > MaxInt64. So need this.
	if math.MaxInt64-alloc.base <= n1 {
		return 0, 0, ErrAutoincReadFailed
	}
	// The local rest is not enough for allocN, skip it.
	if alloc.base+n1 > alloc.end {
		var newBase, newEnd int64
		nextStep := int64(batch)

		ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
		err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
			if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
				span1 := span.Tracer().StartSpan("alloc.alloc4Signed", opentracing.ChildOf(span.Context()))
				defer span1.Finish()
				opentracing.ContextWithSpan(ctx, span1)
			}
			idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).RowID()
			var err1 error
			newBase, err1 = idAcc.Get()
			if err1 != nil {
				return err1
			}
			// calcNeededBatchSize calculates the total batch size needed on global base.
			n1 = calcNeededBatchSize(newBase, int64(n), increment, offset, isUnsigned)
			// Although the step is customized by user, we still need to make sure nextStep is big enough for insert batch.
			if nextStep < n1 {
				nextStep = n1
			}
			tmpStep := mathutil.Min(math.MaxInt64-newBase, nextStep)
			// The global rest is not enough for alloc.
			if tmpStep < n1 {
				return ErrAutoincReadFailed
			}
			newEnd, err1 = idAcc.Inc(tmpStep)
			return err1
		})
		if err != nil {
			return 0, 0, err
		}
		if newBase == math.MaxInt64 {
			return 0, 0, ErrAutoincReadFailed
		}
		alloc.base, alloc.end = newBase, newEnd
	}
	min = alloc.base
	alloc.base += n1
	return min, alloc.base, nil
}

func (alloc *autoIDValue) rebase4Unsigned(ctx context.Context,
	store kv.Storage,
	dbID, tblID int64,
	requiredBase uint64) error {
	// Satisfied by alloc.base, nothing to do.
	if requiredBase <= uint64(alloc.base) {
		return nil
	}
	// Satisfied by alloc.end, need to update alloc.base.
	if requiredBase <= uint64(alloc.end) {
		alloc.base = int64(requiredBase)
		return nil
	}

	var newBase, newEnd uint64
	startTime := time.Now()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).RowID()
		currentEnd, err1 := idAcc.Get()
		if err1 != nil {
			return err1
		}
		uCurrentEnd := uint64(currentEnd)
		//	if allocIDs {
		newBase = mathutil.Max(uCurrentEnd, requiredBase)
		newEnd = mathutil.Min(math.MaxUint64-uint64(batch), newBase) + uint64(batch)
		//	} else {
		//		if uCurrentEnd >= requiredBase {
		//			newBase = uCurrentEnd
		//			newEnd = uCurrentEnd
		//			// Required base satisfied, we don't need to update KV.
		//			return nil
		//		}
		//		// If we don't want to allocate IDs, for example when creating a table with a given base value,
		//		// We need to make sure when other TiDB server allocates ID for the first time, requiredBase + 1
		//		// will be allocated, so we need to increase the end to exactly the requiredBase.
		//		newBase = requiredBase
		//		newEnd = requiredBase
		//	}
		_, err1 = idAcc.Inc(int64(newEnd - uCurrentEnd))
		return err1
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return err
	}
	alloc.base, alloc.end = int64(newBase), int64(newEnd)
	return nil
}

func (alloc *autoIDValue) rebase4Signed(ctx context.Context, store kv.Storage, dbID, tblID int64, requiredBase int64) error {
	// Satisfied by alloc.base, nothing to do.
	if requiredBase <= alloc.base {
		return nil
	}
	// Satisfied by alloc.end, need to update alloc.base.
	if requiredBase <= alloc.end {
		alloc.base = requiredBase
		return nil
	}

	var newBase, newEnd int64
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).RowID()
		currentEnd, err1 := idAcc.Get()
		if err1 != nil {
			return err1
		}
		// if allocIDs {
		newBase = mathutil.Max(currentEnd, requiredBase)
		newEnd = mathutil.Min(math.MaxInt64-batch, newBase) + batch
		// } else {
		// 	if currentEnd >= requiredBase {
		// 		newBase = currentEnd
		// 		newEnd = currentEnd
		// 		// Required base satisfied, we don't need to update KV.
		// 		return nil
		// 	}
		// 	// If we don't want to allocate IDs, for example when creating a table with a given base value,
		// 	// We need to make sure when other TiDB server allocates ID for the first time, requiredBase + 1
		// 	// will be allocated, so we need to increase the end to exactly the requiredBase.
		// 	newBase = requiredBase
		// 	newEnd = requiredBase
		// }
		_, err1 = idAcc.Inc(newEnd - currentEnd)
		return err1
	})
	if err != nil {
		return err
	}
	alloc.base, alloc.end = newBase, newEnd
	return nil
}

type Service struct {
	autoIDLock sync.Mutex
	autoIDMap  map[autoIDKey]*autoIDValue

	*leaderShip
	store kv.Storage
}

var t *tachymeter.Tachymeter
var count int64

func New(selfAddr string, etcdAddr []string, store kv.Storage) *Service {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdAddr,
		DialTimeout: time.Second,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("=============== run here in service new!!")

	l := &leaderShip{cli: cli}
	go l.campaignLoop(context.Background(), selfAddr)

	fmt.Println("================ autoid service leader started...")

	// p := tikvPersist{Storage: store}
	// t = tachymeter.New(&tachymeter.Config{Size: 300})
	//	go func() {
	//		tick := time.NewTicker(5 * time.Second)
	//		for range tick.C {
	//			total := atomic.LoadInt64(&count)
	//			fmt.Println("qps ==", total/5)
	//			atomic.StoreInt64(&count, 0)
	//
	//			fmt.Println(t.Calc())
	//			fmt.Println()
	//		}
	//	}()

	return &Service{
		autoIDMap:  make(map[autoIDKey]*autoIDValue),
		leaderShip: l,
		store:      store,
	}
}

type mockClient struct {
	Service
}

func (m *mockClient) AllocAutoID(ctx context.Context, in *autoid.AutoIDRequest, opts ...grpc.CallOption) (*autoid.AutoIDResponse, error) {
	return m.Service.AllocAutoID(ctx, in)
}

func (m *mockClient) Rebase(ctx context.Context, in *autoid.RebaseRequest, opts ...grpc.CallOption) (*autoid.RebaseResponse, error) {
	return m.Service.Rebase(ctx, in)
}

var global = make(map[string]*mockClient)

func MockForTest(store kv.Storage) *mockClient {
	uuid := store.UUID()
	ret, ok := global[uuid]
	if !ok {
		ret = &mockClient{
			Service{
				autoIDMap:  make(map[autoIDKey]*autoIDValue),
				leaderShip: &leaderShip{mock: true},
				store:      store,
			},
		}
		global[uuid] = ret
	}
	return ret
}

func (s *Service) Close() {
}

// seekToFirstAutoIDSigned seeks to the next valid signed position.
func seekToFirstAutoIDSigned(base, increment, offset int64) int64 {
	nr := (base + increment - offset) / increment
	nr = nr*increment + offset
	return nr
}

// seekToFirstAutoIDUnSigned seeks to the next valid unsigned position.
func seekToFirstAutoIDUnSigned(base, increment, offset uint64) uint64 {
	nr := (base + increment - offset) / increment
	nr = nr*increment + offset
	return nr
}

func calcNeededBatchSize(base, n, increment, offset int64, isUnsigned bool) int64 {
	if increment == 1 {
		return n
	}
	if isUnsigned {
		// SeekToFirstAutoIDUnSigned seeks to the next unsigned valid position.
		nr := seekToFirstAutoIDUnSigned(uint64(base), uint64(increment), uint64(offset))
		// calculate the total batch size needed.
		nr += (uint64(n) - 1) * uint64(increment)
		return int64(nr - uint64(base))
	}
	nr := seekToFirstAutoIDSigned(base, increment, offset)
	// calculate the total batch size needed.
	nr += (n - 1) * increment
	return nr - base
}

const batch = 2000

// AllocID implements gRPC PDServer.
func (s *Service) AllocAutoID(ctx context.Context, req *autoid.AutoIDRequest) (*autoid.AutoIDResponse, error) {
	// fmt.Println("recieve request ==", *req)
	var res *autoid.AutoIDResponse
	for {
		var err error
		res, err = s.allocAutoID(ctx, req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if res != nil {
			break
		}
		// fmt.Println("fuck, another loop??")
	}
	// fmt.Println("handle auto id service success")
	return res, nil
}

func (s *Service) getAlloc(dbID, tblID int64) *autoIDValue {
	key := autoIDKey{dbID: dbID, tblID: tblID}
	s.autoIDLock.Lock()
	defer s.autoIDLock.Unlock()

	val, ok := s.autoIDMap[key]
	if !ok {
		val = &autoIDValue{
			token: make(chan struct{}, 1),
		}
		s.autoIDMap[key] = val
	}

	return val
}

func (s *Service) allocAutoID(ctx context.Context, req *autoid.AutoIDRequest) (*autoid.AutoIDResponse, error) {
	if !s.IsLeader() {
		fmt.Println("not leader!!!!!!!!!!!  fuck~~")
		return nil, errors.New("not leader")
	}

	val := s.getAlloc(req.DbID, req.TblID)

	if req.N == 0 {
		base := val.base
		return &autoid.AutoIDResponse{
			Min: base,
			Max: base,
		}, nil
	}

	var min, max int64
	var err error
	if req.IsUnsigned {
		min, max, err = val.alloc4Unsigned(ctx, s.store, req.DbID, req.TblID, req.IsUnsigned, req.N, req.Increment, req.Offset)
	} else {
		min, max, err = val.alloc4Signed(ctx, s.store, req.DbID, req.TblID, req.IsUnsigned, req.N, req.Increment, req.Offset)
	}

	/*
		// calcNeededBatchSize calculates the total batch size needed.
		n1 := calcNeededBatchSize(val.base, int64(req.N), req.Increment, req.Offset, true)
		// Condition alloc.base+n1 > alloc.end will overflow when alloc.base + n1 > MaxInt64. So need this.
		if math.MaxUint64-uint64(val.base) <= uint64(n1) {
			return nil, ErrAutoincReadFailed
		}
		min := val.base
		base := int64(uint64(val.base) + uint64(n1))
		end := atomic.LoadInt64(val.end)
		if base < end {
			// Safe to alloc directly
			val.base = base
			// fmt.Println("normal ... base == ", base, " and end ==", end)
		} else {
			// // Need to sync the ID first, in case the server panic and lost the ID.
			s.autoIDLock.Unlock()

			val.token <- struct{}{}
			// fmt.Println("base ==", base, "end ==", end)

			// start := time.Now()
			err := s.SyncID(ctx, req.DbID, req.TblID, uint64(base)+batch, val.end, val.token)
			if err != nil {
				fmt.Println("sync id error", err)
				return nil, errors.Trace(err)
			}
			atomic.AddInt64(&count, 1)
			// t.AddTime(time.Since(start))

			// And then retry
			return nil, nil
		}
	*/

	/*
		s.autoIDLock.Unlock()

		if end-(batch/2) < base {
			// fmt.Println("async pre-alloc id... end ==", end, "base ==", base)
			// Trigger sync in the background gorotuine, pre-alloc the ID.
			select {
			case val.token <- struct{}{}:
				go s.SyncID(ctx, req.DbID, req.TblID, uint64(base)+batch, val.end, val.token)
			default:
			}
		}
	*/

	// fmt.Println("return ..", min, base)
	return &autoid.AutoIDResponse{
		Min: min,
		Max: max,
	}, err
}

/*
func (s *Service) initKV(ctx context.Context, key autoIDKey) error {
	// Initialize the value.
	val := &autoIDValue{
		token: make(chan struct{}, 1),
	}

	val.token <- struct{}{}
	min, err := s.LoadID(ctx, key.dbID, key.tblID)
	if err != nil {
		fmt.Println("init kv err ===", err)
		return errors.Trace(err)
	}
	val.base = int64(min)
	end := min + batch
	err = s.SyncID(ctx, key.dbID, key.tblID, end, val.end, val.token)
	if err != nil {
		return errors.Trace(err)
	}

	s.autoIDLock.Lock()
	s.autoIDMap[key] = val
	fmt.Println("run into initKV!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", key, val, s)
	s.autoIDLock.Unlock()

	fmt.Println("init kv success ==", key, val)
	return nil
}
*/

func (alloc *autoIDValue) forceRebase(ctx context.Context, store kv.Storage, dbID, tblID, requiredBase int64, isUnsigned bool) error {

	fmt.Println("force rebase callsed ...", isUnsigned)

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).RowID()
		currentEnd, err1 := idAcc.Get()
		if err1 != nil {
			return err1
		}
		var step int64
		if !isUnsigned {
			step = requiredBase - currentEnd
		} else {
			uRequiredBase, uCurrentEnd := uint64(requiredBase), uint64(currentEnd)
			step = int64(uRequiredBase - uCurrentEnd)
		}
		_, err1 = idAcc.Inc(step)
		return err1
	})
	if err != nil {
		return err
	}
	alloc.base, alloc.end = requiredBase, requiredBase
	return nil
}

func (s *Service) Rebase(ctx context.Context, req *autoid.RebaseRequest) (*autoid.RebaseResponse, error) {
	if !s.IsLeader() {
		return nil, errors.New("not leader")
	}

	val := s.getAlloc(req.DbID, req.TblID)
	if req.Force {
		val.forceRebase(ctx, s.store, req.DbID, req.TblID, req.Base, req.IsUnsigned)
	}

	var err error
	if req.IsUnsigned {
		err = val.rebase4Unsigned(ctx, s.store, req.DbID, req.TblID, uint64(req.Base))
	} else {
		err = val.rebase4Signed(ctx, s.store, req.DbID, req.TblID, req.Base)
	}

	/*

		var min, max int64
		var err error
		if req.IsUnsigned {
			min, max, err = val.alloc4Unsigned(ctx, s.store, req.DbID, req.TblID, req.N, req.Increment, req.Offset)
		} else {
			min, max, err = val.alloc4Signed(ctx, s.store, req.DbID, req.TblID, req.N, req.Increment, req.Offset)
		}

		key := autoIDKey{dbID: req.DbID, tblID: req.TblID}
		s.autoIDLock.Lock()
		val, ok := s.autoIDMap[key]
		if !ok {
			s.autoIDLock.Unlock()
			err := s.initKV(ctx, key)
			if err != nil {
				fmt.Println("init kv error ==", err)
				return nil, errors.Trace(err)
			}
			return s.Rebase(ctx, req)
		}

		if req.Force {
			val.base = req.Base
			s.autoIDLock.Unlock()
			val.token <- struct{}{}
			newVal := uint64(req.Base) + batch
			if newVal <= uint64(req.Base) {
				newVal = math.MaxUint64
			}
			err := s.SyncID(ctx, req.DbID, req.TblID, newVal, val.end, val.token)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return &autoid.RebaseResponse{}, nil
		}

		fmt.Println("!!!! rebase called      ........", req.Base, val.base, *val.end)
		if req.Base < atomic.LoadInt64(val.end) {
			if req.Base > val.base {
				val.base = req.Base
				fmt.Println("now ... base === ...", val.base)
			}
		} else {
			s.autoIDLock.Unlock()
			val.token <- struct{}{}
			err := s.SyncID(ctx, req.DbID, req.TblID, uint64(req.Base)+batch, val.end, val.token)
			if err != nil {
				return nil, errors.Trace(err)
			}
			fmt.Println("sync id to ...", uint64(req.Base)+batch)
			return s.Rebase(ctx, req)
		}
		s.autoIDLock.Unlock()
	*/

	return &autoid.RebaseResponse{}, err
}
