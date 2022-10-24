package autoid

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/autoid"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/jamiealquiza/tachymeter"
)

var (
	ErrAutoincReadFailed = errors.New("auto increment action failed")
)

type autoIDKey struct {
	dbID  int64
	tblID int64
}

type autoIDValue struct {
	base  int64
	max   *int64
	token chan struct{}
}

type Service struct {
	autoIDLock sync.Mutex
	autoIDMap  map[autoIDKey]*autoIDValue

	*leaderShip
	persist
}

var t *tachymeter.Tachymeter
var count int64

func New(selfAddr string, tikvPath string) *Service {
	cfg := config.GetGlobalConfig()
	fullPath := fmt.Sprintf("tikv://%s", cfg.Path)
	store, err := store.New(fullPath)
	if err != nil {
		panic(err)
	}
	ebd, ok := store.(kv.EtcdBackend)
	if !ok {
		return nil
	}
	etcdAddr, err := ebd.EtcdAddrs()
	if err != nil {
		panic(err)
	}

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

	p := tikvPersist{Storage: store}

	t = tachymeter.New(&tachymeter.Config{Size: 300})
	go func() {
		tick := time.NewTicker(5 * time.Second)
		for range tick.C {
			total := atomic.LoadInt64(&count)
			fmt.Println("qps ==", total/5)
			atomic.StoreInt64(&count, 0)

			fmt.Println(t.Calc())
			fmt.Println()
		}
	}()

	return &Service{
		autoIDMap:  make(map[autoIDKey]*autoIDValue),
		leaderShip: l,
		persist:    p,
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

func MockForTest(uuid string) *mockClient {
	ret, ok := global[uuid]
	if !ok {
		ret = &mockClient{
			Service{
				autoIDMap:  make(map[autoIDKey]*autoIDValue),
				leaderShip: &leaderShip{mock: true},
				persist:    &mockPersist{data: make(map[autoIDKey]uint64)},
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
		// Calculate the total batch size needed.
		nr += (uint64(n) - 1) * uint64(increment)
		return int64(nr - uint64(base))
	}
	nr := seekToFirstAutoIDSigned(base, increment, offset)
	// Calculate the total batch size needed.
	nr += (n - 1) * increment
	return nr - base
}

const batch = 400

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

func (s *Service) allocAutoID(ctx context.Context, req *autoid.AutoIDRequest) (*autoid.AutoIDResponse, error) {
	if !s.IsLeader() {
		fmt.Println("not leader!!!!!!!!!!!  fuck~~")
		return nil, errors.New("not leader")
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
		fmt.Println("retry, because first init kv")
		return nil, nil // retry
	}

	// calcNeededBatchSize calculates the total batch size needed.
	n1 := calcNeededBatchSize(val.base, int64(req.N), req.Increment, req.Offset, true)
	// Condition alloc.base+n1 > alloc.end will overflow when alloc.base + n1 > MaxInt64. So need this.
	if math.MaxUint64-uint64(val.base) <= uint64(n1) {
		return nil, ErrAutoincReadFailed
	}
	min := val.base
	base := int64(uint64(val.base) + uint64(n1))
	max := atomic.LoadInt64(val.max)
	if base < max {
		// Safe to alloc directly
		val.base = base
		// fmt.Println("normal ... base == ", base, " and max ==", max)
	} else {
		// // Need to sync the ID first, in case the server panic and lost the ID.
		s.autoIDLock.Unlock()

		// val.token <- struct{}{}
		// // fmt.Println("base ==", base, "max ==", max)

		// start := time.Now()
		// err := s.syncID(ctx, req.DbID, req.TblID, uint64(base)+batch, val.max, val.token)
		// if err != nil {
		// 	fmt.Println("sync id error", err)
		// 	return nil, errors.Trace(err)
		// }
		// atomic.AddInt64(&count, 1)
		// t.AddTime(time.Since(start))

		// And then retry
		return nil, nil
	}
	s.autoIDLock.Unlock()

	// if max-(batch/2) < base {
	// 	// fmt.Println("async pre-alloc id... max ==", max, "base ==", base)
	// 	// Trigger sync in the background gorotuine, pre-alloc the ID.
	// 	select {
	// 	case val.token <- struct{}{}:
	// 		go s.syncID(ctx, req.DbID, req.TblID, uint64(base)+batch, val.max, val.token)
	// 	default:
	// 	}
	// }

	// fmt.Println("return ..", min, base)
	return &autoid.AutoIDResponse{
		Min: min,
		Max: base,
	}, nil
}

func (s *Service) initKV(ctx context.Context, key autoIDKey) error {
	// Initialize the value.
	val := &autoIDValue{
		token: make(chan struct{}, 1),
		max:   new(int64),
	}

	val.token <- struct{}{}
	min, err := s.loadID(ctx, key.dbID, key.tblID)
	if err != nil {
		fmt.Println("init kv err ===", err)
		return errors.Trace(err)
	}
	val.base = int64(min)
	max := min + batch
	err = s.syncID(ctx, key.dbID, key.tblID, max, val.max, val.token)
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

func (s *Service) Rebase(ctx context.Context, req *autoid.RebaseRequest) (*autoid.RebaseResponse, error) {
	if !s.IsLeader() {
		return nil, errors.New("not leader")
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

	fmt.Println("!!!! rebase called      ........", req.Base, val.base, *val.max)
	if req.Base < atomic.LoadInt64(val.max) {
		if req.Base > val.base {
			val.base = req.Base
			fmt.Println("now ... base === ...", val.base)
		}
	} else {
		s.autoIDLock.Unlock()
		val.token <- struct{}{}
		err := s.syncID(ctx, req.DbID, req.TblID, uint64(req.Base)+batch, val.max, val.token)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fmt.Println("sync id to ...", uint64(req.Base)+batch)
		return s.Rebase(ctx, req)
	}
	s.autoIDLock.Unlock()
	return &autoid.RebaseResponse{}, nil
}
