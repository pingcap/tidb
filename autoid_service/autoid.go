// Copyright 2022 PingCAP, Inc.
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

package autoid

import (
	"context"
	"crypto/tls"
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/autoid"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	autoid1 "github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	errAutoincReadFailed = errors.New("auto increment action failed")
)

const (
	autoIDLeaderPath = "tidb/autoid/leader"
)

type autoIDKey struct {
	dbID  int64
	tblID int64
}

type autoIDValue struct {
	sync.Mutex
	base       int64
	end        int64
	isUnsigned bool
	token      chan struct{}
}

func (alloc *autoIDValue) alloc4Unsigned(ctx context.Context, store kv.Storage, dbID, tblID int64, isUnsigned bool,
	n uint64, increment, offset int64) (min int64, max int64, err error) {
	// Check offset rebase if necessary.
	if uint64(offset-1) > uint64(alloc.base) {
		if err := alloc.rebase4Unsigned(ctx, store, dbID, tblID, uint64(offset-1)); err != nil {
			return 0, 0, err
		}
	}
	// calcNeededBatchSize calculates the total batch size needed.
	n1 := calcNeededBatchSize(alloc.base, int64(n), increment, offset, isUnsigned)

	// The local rest is not enough for alloc, skip it.
	if uint64(alloc.base)+uint64(n1) > uint64(alloc.end) || alloc.base == 0 {
		var newBase, newEnd int64
		nextStep := int64(batch)
		// Although it may skip a segment here, we still treat it as consumed.

		ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
		err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
			idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).IncrementID(model.TableInfoVersion5)
			var err1 error
			newBase, err1 = idAcc.Get()
			if err1 != nil {
				return err1
			}
			// calcNeededBatchSize calculates the total batch size needed on new base.
			n1 = calcNeededBatchSize(newBase, int64(n), increment, offset, isUnsigned)
			// Although the step is customized by user, we still need to make sure nextStep is big enough for insert batch.
			if nextStep < n1 {
				nextStep = n1
			}
			tmpStep := int64(mathutil.Min(math.MaxUint64-uint64(newBase), uint64(nextStep)))
			// The global rest is not enough for alloc.
			if tmpStep < n1 {
				return errAutoincReadFailed
			}
			newEnd, err1 = idAcc.Inc(tmpStep)
			return err1
		})
		if err != nil {
			return 0, 0, err
		}
		if uint64(newBase) == math.MaxUint64 {
			return 0, 0, errAutoincReadFailed
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
		return 0, 0, errAutoincReadFailed
	}

	// The local rest is not enough for allocN, skip it.
	// If alloc.base is 0, the alloc may not be initialized, force fetch from remote.
	if alloc.base+n1 > alloc.end || alloc.base == 0 {
		var newBase, newEnd int64
		nextStep := int64(batch)

		ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
		err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
			idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).IncrementID(model.TableInfoVersion5)
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
				return errAutoincReadFailed
			}
			newEnd, err1 = idAcc.Inc(tmpStep)
			return err1
		})
		if err != nil {
			return 0, 0, err
		}
		if newBase == math.MaxInt64 {
			return 0, 0, errAutoincReadFailed
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
		idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).IncrementID(model.TableInfoVersion5)
		currentEnd, err1 := idAcc.Get()
		if err1 != nil {
			return err1
		}
		uCurrentEnd := uint64(currentEnd)
		newBase = mathutil.Max(uCurrentEnd, requiredBase)
		newEnd = mathutil.Min(math.MaxUint64-uint64(batch), newBase) + uint64(batch)
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
		idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).IncrementID(model.TableInfoVersion5)
		currentEnd, err1 := idAcc.Get()
		if err1 != nil {
			return err1
		}
		newBase = mathutil.Max(currentEnd, requiredBase)
		newEnd = mathutil.Min(math.MaxInt64-batch, newBase) + batch
		_, err1 = idAcc.Inc(newEnd - currentEnd)
		return err1
	})
	if err != nil {
		return err
	}
	alloc.base, alloc.end = newBase, newEnd
	return nil
}

// Service implement the grpc AutoIDAlloc service, defined in kvproto/pkg/autoid.
type Service struct {
	autoIDLock sync.Mutex
	autoIDMap  map[autoIDKey]*autoIDValue

	leaderShip owner.Manager
	store      kv.Storage
}

// New return a Service instance.
func New(selfAddr string, etcdAddr []string, store kv.Storage, tlsConfig *tls.Config) *Service {
	cfg := config.GetGlobalConfig()
	etcdLogCfg := zap.NewProductionConfig()

	cli, err := clientv3.New(clientv3.Config{
		LogConfig:        &etcdLogCfg,
		Endpoints:        etcdAddr,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBackoffMaxDelay(time.Second * 3),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
				Timeout: time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
			}),
		},
		TLS: tlsConfig,
	})
	if err != nil {
		panic(err)
	}
	return newWithCli(selfAddr, cli, store)
}

func newWithCli(selfAddr string, cli *clientv3.Client, store kv.Storage) *Service {
	l := owner.NewOwnerManager(context.Background(), cli, "autoid", selfAddr, autoIDLeaderPath)
	err := l.CampaignOwner()
	if err != nil {
		panic(err)
	}

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

// MockForTest is used for testing, the UT test and unistore use this.
func MockForTest(store kv.Storage) autoid.AutoIDAllocClient {
	uuid := store.UUID()
	ret, ok := global[uuid]
	if !ok {
		ret = &mockClient{
			Service{
				autoIDMap:  make(map[autoIDKey]*autoIDValue),
				leaderShip: nil,
				store:      store,
			},
		}
		global[uuid] = ret
	}
	return ret
}

// Close closes the Service and clean up resource.
func (s *Service) Close() {
	if s.leaderShip != nil {
		for k, v := range s.autoIDMap {
			if v.base > 0 {
				err := v.forceRebase(context.Background(), s.store, k.dbID, k.tblID, v.base, v.isUnsigned)
				if err != nil {
					logutil.BgLogger().Warn("[autoid service] save cached ID fail when service exit",
						zap.Int64("db id", k.dbID),
						zap.Int64("table id", k.tblID),
						zap.Int64("value", v.base),
						zap.Error(err))
				}
			}
		}
		s.leaderShip.Cancel()
	}
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

const batch = 4000

// AllocAutoID implements gRPC AutoIDAlloc interface.
func (s *Service) AllocAutoID(ctx context.Context, req *autoid.AutoIDRequest) (*autoid.AutoIDResponse, error) {
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
	}
	return res, nil
}

func (s *Service) getAlloc(dbID, tblID int64, isUnsigned bool) *autoIDValue {
	key := autoIDKey{dbID: dbID, tblID: tblID}
	s.autoIDLock.Lock()
	defer s.autoIDLock.Unlock()

	val, ok := s.autoIDMap[key]
	if !ok {
		val = &autoIDValue{
			isUnsigned: isUnsigned,
			token:      make(chan struct{}, 1),
		}
		s.autoIDMap[key] = val
	}

	return val
}

func (s *Service) allocAutoID(ctx context.Context, req *autoid.AutoIDRequest) (*autoid.AutoIDResponse, error) {
	if s.leaderShip != nil && !s.leaderShip.IsOwner() {
		logutil.BgLogger().Info("[autoid service] Alloc AutoID fail, not leader")
		return nil, errors.New("not leader")
	}

	failpoint.Inject("mockErr", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("mock reload failed"))
		}
	})

	val := s.getAlloc(req.DbID, req.TblID, req.IsUnsigned)

	if req.N == 0 {
		if val.base != 0 {
			return &autoid.AutoIDResponse{
				Min: val.base,
				Max: val.base,
			}, nil
		}
		// This item is not initialized, get the data from remote.
		var currentEnd int64
		ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
		err := kv.RunInNewTxn(ctx, s.store, true, func(ctx context.Context, txn kv.Transaction) error {
			idAcc := meta.NewMeta(txn).GetAutoIDAccessors(req.DbID, req.TblID).RowID()
			var err1 error
			currentEnd, err1 = idAcc.Get()
			if err1 != nil {
				return err1
			}
			val.end = currentEnd
			return nil
		})
		if err != nil {
			return &autoid.AutoIDResponse{Errmsg: []byte(err.Error())}, nil
		}
		return &autoid.AutoIDResponse{
			Min: currentEnd,
			Max: currentEnd,
		}, nil
	}

	val.Lock()
	defer val.Unlock()

	var min, max int64
	var err error
	if req.IsUnsigned {
		min, max, err = val.alloc4Unsigned(ctx, s.store, req.DbID, req.TblID, req.IsUnsigned, req.N, req.Increment, req.Offset)
	} else {
		min, max, err = val.alloc4Signed(ctx, s.store, req.DbID, req.TblID, req.IsUnsigned, req.N, req.Increment, req.Offset)
	}

	if err != nil {
		return &autoid.AutoIDResponse{Errmsg: []byte(err.Error())}, nil
	}
	return &autoid.AutoIDResponse{
		Min: min,
		Max: max,
	}, nil
}

func (alloc *autoIDValue) forceRebase(ctx context.Context, store kv.Storage, dbID, tblID, requiredBase int64, isUnsigned bool) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		idAcc := meta.NewMeta(txn).GetAutoIDAccessors(dbID, tblID).IncrementID(model.TableInfoVersion5)
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

// Rebase implements gRPC AutoIDAlloc interface.
// req.N = 0 is handled specially, it is used to return the current auto ID value.
func (s *Service) Rebase(ctx context.Context, req *autoid.RebaseRequest) (*autoid.RebaseResponse, error) {
	if s.leaderShip != nil && !s.leaderShip.IsOwner() {
		logutil.BgLogger().Info("[autoid service] Rebase() fail, not leader")
		return nil, errors.New("not leader")
	}

	val := s.getAlloc(req.DbID, req.TblID, req.IsUnsigned)
	if req.Force {
		err := val.forceRebase(ctx, s.store, req.DbID, req.TblID, req.Base, req.IsUnsigned)
		if err != nil {
			return &autoid.RebaseResponse{Errmsg: []byte(err.Error())}, nil
		}
	}

	var err error
	if req.IsUnsigned {
		err = val.rebase4Unsigned(ctx, s.store, req.DbID, req.TblID, uint64(req.Base))
	} else {
		err = val.rebase4Signed(ctx, s.store, req.DbID, req.TblID, req.Base)
	}
	if err != nil {
		return &autoid.RebaseResponse{Errmsg: []byte(err.Error())}, nil
	}
	return &autoid.RebaseResponse{}, nil
}

func init() {
	autoid1.MockForTest = MockForTest
}
