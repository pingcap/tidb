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
	goerrors "errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/autoid"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/tikv/client-go/v2/tikv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var _ Allocator = &singlePointAlloc{}

type singlePointAlloc struct {
	dbID          int64
	tblID         int64
	lastAllocated int64
	isUnsigned    bool
	*ClientDiscover
	keyspaceID      uint32
	notLeaderPolicy notLeaderRetryPolicy
}

// ClientDiscover is used to get the AutoIDAllocClient, it creates the grpc connection with autoid service leader.
type ClientDiscover struct {
	// This the etcd client for service discover
	etcdCli *clientv3.Client
	// This is the real client for the AutoIDAlloc service
	mu struct {
		sync.RWMutex
		autoid.AutoIDAllocClient
		// Release the client conn to avoid resource leak!
		// See https://github.com/grpc/grpc-go/issues/5321
		*grpc.ClientConn
		leaderSnapshot autoIDLeaderSnapshot
		// version advances on every reset so stale callers cannot reset a
		// newer connection.
		version uint64
	}
}

var autoIDRequestSequence atomic.Uint64

const (
	// AutoIDLeaderPath is etcd key of auto id service leader, exported for test.
	AutoIDLeaderPath = "tidb/autoid/leader"

	defaultNotLeaderMinConsecutive = 10
	// This intentionally does not follow the 60-second owner lease. It applies
	// only when the same winning endpoint repeatedly responds "not leader".
	defaultNotLeaderMinDuration = 15 * time.Second
	autoIDNotLeaderAction       = "check for duplicate advertise-address:status-port or unexpected TiDB processes connected to the cluster"
)

type autoIDLeaderSnapshot struct {
	address        string
	electionKey    string
	createRevision int64
}

// equal includes campaign metadata because a new campaign can reuse the same
// address.
func (s autoIDLeaderSnapshot) equal(other autoIDLeaderSnapshot) bool {
	return s.address == other.address &&
		s.electionKey == other.electionKey &&
		s.createRevision == other.createRevision
}

type notLeaderRetryPolicy struct {
	minConsecutive int
	minDuration    time.Duration
}

type notLeaderRetryState struct {
	leader    autoIDLeaderSnapshot
	count     int
	firstSeen time.Time
}

type notLeaderFastFailMarker interface {
	AutoIDNotLeaderFastFail()
}

type notLeaderFastFailError struct {
	cause error
}

func (e *notLeaderFastFailError) Error() string {
	return e.cause.Error()
}

func (e *notLeaderFastFailError) Cause() error {
	return e.cause
}

func (e *notLeaderFastFailError) Unwrap() error {
	return e.cause
}

func (*notLeaderFastFailError) AutoIDNotLeaderFastFail() {}

// IsNotLeaderFastFailError reports whether err is a terminal error caused by
// repeated not-leader responses reaching the fast-fail threshold.
func IsNotLeaderFastFailError(err error) bool {
	var marker notLeaderFastFailMarker
	return goerrors.As(err, &marker)
}

func (s *notLeaderRetryState) observe(leader autoIDLeaderSnapshot, now time.Time, policy notLeaderRetryPolicy) bool {
	if s.count == 0 || !s.leader.equal(leader) {
		s.leader = leader
		s.count = 1
		s.firstSeen = now
	} else {
		s.count++
	}
	return policy.minConsecutive > 0 &&
		s.count >= policy.minConsecutive &&
		now.Sub(s.firstSeen) >= policy.minDuration
}

func (s *notLeaderRetryState) reset() {
	*s = notLeaderRetryState{}
}

// Match only the direct codes.Unknown/"not leader" wire error emitted by the
// AutoID service; all other errors retain the existing retry behavior.
func isAutoIDNotLeaderError(err error) bool {
	grpcStatus, ok := status.FromError(err)
	return ok && grpcStatus.Code() == codes.Unknown && grpcStatus.Message() == "not leader"
}

type autoIDRequestLogState struct {
	operation       string
	keyspaceID      uint32
	dbID            int64
	tableID         int64
	requestStarted  time.Time
	requestID       uint64
	leaderAddress   string
	notLeaderCount  int
	active          bool
	terminalEmitted bool
}

func newAutoIDRequestLogState(
	operation string,
	keyspaceID uint32,
	dbID, tableID int64,
	requestStarted time.Time,
) autoIDRequestLogState {
	return autoIDRequestLogState{
		operation:      operation,
		keyspaceID:     keyspaceID,
		dbID:           dbID,
		tableID:        tableID,
		requestStarted: requestStarted,
	}
}

func (s *autoIDRequestLogState) observeNotLeader(leader autoIDLeaderSnapshot) {
	s.notLeaderCount++
	s.leaderAddress = leader.address
	if s.active {
		return
	}
	s.active = true
	s.requestID = autoIDRequestSequence.Add(1)
	logutil.BgLogger().Info("autoid request entered not-leader retry",
		zap.String("category", "autoid client"),
		zap.Uint64("autoid-request-id", s.requestID),
		zap.String("operation", s.operation),
		zap.Uint32("keyspace-id", s.keyspaceID),
		zap.Int64("db-id", s.dbID),
		zap.Int64("table-id", s.tableID),
		zap.String("leader-address", s.leaderAddress),
		zap.Duration("request-elapsed", time.Since(s.requestStarted)),
		zap.Int("not-leader-count", s.notLeaderCount))
}

func (s *autoIDRequestLogState) setLeader(leader autoIDLeaderSnapshot) {
	if leader.address != "" {
		s.leaderAddress = leader.address
	}
}

func (s *autoIDRequestLogState) complete(err error) {
	if !s.active || s.terminalEmitted {
		return
	}
	outcome := "recovered"
	if err != nil {
		outcome = "failed"
		cause := errors.Cause(err)
		if cause == context.Canceled || cause == context.DeadlineExceeded {
			outcome = "context-canceled"
		}
	}
	fields := []zap.Field{
		zap.String("category", "autoid client"),
		zap.Uint64("autoid-request-id", s.requestID),
		zap.String("operation", s.operation),
		zap.Uint32("keyspace-id", s.keyspaceID),
		zap.Int64("db-id", s.dbID),
		zap.Int64("table-id", s.tableID),
		zap.String("leader-address", s.leaderAddress),
		zap.Duration("request-elapsed", time.Since(s.requestStarted)),
		zap.Int("not-leader-count", s.notLeaderCount),
		zap.String("outcome", outcome),
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	logutil.BgLogger().Info("autoid request completed after not-leader retry", fields...)
}

func (s *autoIDRequestLogState) fastFail(state notLeaderRetryState, elapsed time.Duration, err error) {
	s.terminalEmitted = true
	logutil.BgLogger().Warn("autoid request stopped after repeated not leader responses",
		zap.String("category", "autoid client"),
		zap.Uint64("autoid-request-id", s.requestID),
		zap.String("operation", s.operation),
		zap.Uint32("keyspace-id", s.keyspaceID),
		zap.Int64("db-id", s.dbID),
		zap.Int64("table-id", s.tableID),
		zap.String("leader-address", state.leader.address),
		zap.Duration("request-elapsed", time.Since(s.requestStarted)),
		zap.Duration("not-leader-elapsed", elapsed),
		zap.Int("not-leader-count", s.notLeaderCount),
		zap.Int("consecutive-not-leader-count", state.count),
		zap.String("outcome", "fast-failed"),
		zap.String("action", autoIDNotLeaderAction),
		zap.Error(err))
}

func (sp *singlePointAlloc) effectiveNotLeaderPolicy() notLeaderRetryPolicy {
	if sp.notLeaderPolicy.minConsecutive > 0 {
		return sp.notLeaderPolicy
	}
	return notLeaderRetryPolicy{
		minConsecutive: defaultNotLeaderMinConsecutive,
		minDuration:    defaultNotLeaderMinDuration,
	}
}

func (sp *singlePointAlloc) repeatedNotLeaderError(
	operation string,
	state notLeaderRetryState,
	now time.Time,
	revalidationErr error,
) error {
	elapsed := now.Sub(state.firstSeen)
	action := autoIDNotLeaderAction
	if revalidationErr != nil {
		action = fmt.Sprintf(
			"fast-fail threshold reached, but the current AutoID leader could not be revalidated: %v; "+
				"check etcd connectivity or AutoID leader election, then %s",
			revalidationErr,
			autoIDNotLeaderAction,
		)
	}
	// TODO: Consider enriching terminal errors with the remote TiDB identity from /info.
	message := fmt.Sprintf(
		"autoid %s failed after %d consecutive %q responses from leader endpoint %s over %s; keyspace_id=%d, db_id=%d, table_id=%d; %s",
		operation,
		state.count,
		"not leader",
		state.leader.address,
		elapsed.Round(time.Millisecond),
		sp.keyspaceID,
		sp.dbID,
		sp.tblID,
		action,
	)
	return errors.AddStack(&notLeaderFastFailError{cause: ErrAutoincReadFailed.FastGen(message)})
}

// handled is false for non-target errors. For handled errors, terminalErr stops
// the request; otherwise retryImmediately controls whether the caller skips backoff.
func (sp *singlePointAlloc) handleNotLeaderError(
	ctx context.Context,
	operation string,
	version uint64,
	leader autoIDLeaderSnapshot,
	rpcErr error,
	state *notLeaderRetryState,
	requestLog *autoIDRequestLogState,
) (handled bool, retryImmediately bool, terminalErr error) {
	if !isAutoIDNotLeaderError(rpcErr) {
		return false, false, nil
	}
	if ctx.Err() != nil {
		return true, false, errors.Trace(ctx.Err())
	}
	now := time.Now()
	requestLog.observeNotLeader(leader)
	reached := state.observe(leader, now, sp.effectiveNotLeaderPolicy())
	sp.resetConn(version, rpcErr)
	if !reached {
		return true, false, nil
	}

	_, _, currentLeader, err := sp.getClientWithLeader(ctx, sp.keyspaceID)
	if err != nil {
		if ctx.Err() != nil {
			return true, false, errors.Trace(ctx.Err())
		}
		terminalErr = sp.repeatedNotLeaderError(operation, *state, now, err)
		requestLog.fastFail(*state, now.Sub(state.firstSeen), terminalErr)
		return true, false, terminalErr
	}
	requestLog.setLeader(currentLeader)
	if !state.leader.equal(currentLeader) {
		state.reset()
		return true, true, nil
	}
	if ctx.Err() != nil {
		return true, false, errors.Trace(ctx.Err())
	}
	terminalErr = sp.repeatedNotLeaderError(operation, *state, now, nil)
	requestLog.fastFail(*state, now.Sub(state.firstSeen), terminalErr)
	return true, false, terminalErr
}

// NewClientDiscover creates a ClientDiscover object.
func NewClientDiscover(etcdCli *clientv3.Client) *ClientDiscover {
	return &ClientDiscover{
		etcdCli: etcdCli,
	}
}

// GetAutoIDServiceLeaderEtcdPath exported for test.
func GetAutoIDServiceLeaderEtcdPath(keyspaceID uint32) string {
	if keyspaceID == uint32(tikv.NullspaceID) {
		return AutoIDLeaderPath
	}
	return "/" + AutoIDLeaderPath
}

// GetClient gets the AutoIDAllocClient.
func (d *ClientDiscover) GetClient(ctx context.Context, keyspaceID uint32) (autoid.AutoIDAllocClient, uint64, error) {
	cli, version, _, err := d.getClientWithLeader(ctx, keyspaceID)
	return cli, version, err
}

func (d *ClientDiscover) getClientWithLeader(
	ctx context.Context,
	keyspaceID uint32,
) (autoid.AutoIDAllocClient, uint64, autoIDLeaderSnapshot, error) {
	d.mu.RLock()
	cli := d.mu.AutoIDAllocClient
	if cli != nil {
		version := d.mu.version
		leader := d.mu.leaderSnapshot
		d.mu.RUnlock()
		return cli, version, leader, nil
	}
	d.mu.RUnlock()

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.mu.AutoIDAllocClient != nil {
		return d.mu.AutoIDAllocClient, d.mu.version, d.mu.leaderSnapshot, nil
	}
	// write a for loop to retry in case of etcd connection error.
	var resp *clientv3.GetResponse
	var err error
	var bo backoffer
retry:
	resp, err = d.etcdCli.Get(ctx, GetAutoIDServiceLeaderEtcdPath(keyspaceID), clientv3.WithFirstCreate()...)
	if err != nil {
		return nil, 0, autoIDLeaderSnapshot{}, errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		// If the key is not found, it means the autoid service leader is not elected yet.
		// We can retry to get the leader.
		if err := bo.Backoff(ctx); err != nil {
			return nil, 0, autoIDLeaderSnapshot{}, errors.Trace(err)
		}
		goto retry
	}
	bo.Reset()

	addr := string(resp.Kvs[0].Value)
	leader := autoIDLeaderSnapshot{
		address:        addr,
		electionKey:    string(resp.Kvs[0].Key),
		createRevision: resp.Kvs[0].CreateRevision,
	}
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	security := config.GetGlobalConfig().Security
	if len(security.ClusterSSLCA) != 0 {
		clusterSecurity := security.ClusterSecurity()
		tlsConfig, err := clusterSecurity.ToTLSConfig()
		if err != nil {
			return nil, 0, autoIDLeaderSnapshot{}, errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}
	logutil.BgLogger().Info("connect to leader", zap.String("category", "autoid client"), zap.String("addr", addr))
	grpcConn, err := grpc.NewClient(addr, opt)
	if err != nil {
		return nil, 0, autoIDLeaderSnapshot{}, errors.Trace(err)
	}
	cli = autoid.NewAutoIDAllocClient(grpcConn)
	d.mu.AutoIDAllocClient = cli
	d.mu.ClientConn = grpcConn
	d.mu.leaderSnapshot = leader
	return cli, d.mu.version, leader, nil
}

// Alloc allocs N consecutive autoID for table with tableID, returning (min, max] of the allocated autoID batch.
// The consecutive feature is used to insert multiple rows in a statement.
// increment & offset is used to validate the start position (the allocator's base is not always the last allocated id).
// The returned range is (min, max]:
// case increment=1 & offset=1: you can derive the ids like min+1, min+2... max.
// case increment=x & offset=y: you firstly need to seek to firstID by `SeekToFirstAutoIDXXX`, then derive the IDs like firstID, firstID + increment * 2... in the caller.
func (sp *singlePointAlloc) Alloc(ctx context.Context, n uint64, increment, offset int64) (minv, maxv int64, retErr error) {
	r, ctx := tracing.StartRegionEx(ctx, "autoid.Alloc")
	defer r.End()

	logutil.BgLogger().Info("alloc autoid",
		zap.Int64("dbID", sp.dbID))
	if !validIncrementAndOffset(increment, offset) {
		return 0, 0, errInvalidIncrementAndOffset.GenWithStackByArgs(increment, offset)
	}

	var bo backoffer
	start := time.Now()
	requestLog := newAutoIDRequestLogState("alloc", sp.keyspaceID, sp.dbID, sp.tblID, start)
	defer func() {
		requestLog.complete(retErr)
	}()
	var notLeaderState notLeaderRetryState
retry:
	cli, ver, leader, err := sp.getClientWithLeader(ctx, sp.keyspaceID)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	requestLog.setLeader(leader)

	clientStart := time.Now()
	resp, err := cli.AllocAutoID(ctx, &autoid.AutoIDRequest{
		DbID:       sp.dbID,
		TblID:      sp.tblID,
		N:          n,
		Increment:  increment,
		Offset:     offset,
		IsUnsigned: sp.isUnsigned,
		KeyspaceID: sp.keyspaceID,
	})
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDAlloc, metrics.RetLabel(err)).Observe(time.Since(clientStart).Seconds())
	if err != nil {
		if ctx.Err() != nil {
			return 0, 0, errors.Trace(ctx.Err())
		}
		handled, retryImmediately, terminalErr := sp.handleNotLeaderError(
			ctx,
			"alloc",
			ver,
			leader,
			err,
			&notLeaderState,
			&requestLog,
		)
		if handled {
			if terminalErr != nil {
				return 0, 0, terminalErr
			}
			if !retryImmediately {
				if err := bo.Backoff(ctx); err != nil {
					return 0, 0, errors.Trace(err)
				}
			}
			goto retry
		}
		notLeaderState.reset()
		if strings.Contains(err.Error(), "rpc error") {
			// If the RPC error is caused by a canceled context (e.g. KILL QUERY),
			// return immediately instead of resetting the connection and retrying.
			// This prevents a canceled statement from blocking for minutes due to
			// repeated resetConn + backoff cycles.
			if ctx.Err() != nil {
				return 0, 0, errors.Trace(ctx.Err())
			}
			sp.resetConn(ver, err)
			if err := bo.Backoff(ctx); err != nil {
				return 0, 0, errors.Trace(err)
			}
			goto retry
		}
		return 0, 0, errors.Trace(err)
	}
	bo.Reset()
	if len(resp.Errmsg) != 0 {
		return 0, 0, errors.Trace(errors.New(string(resp.Errmsg)))
	}

	du := time.Since(start)
	metrics.AutoIDReqDuration.Observe(du.Seconds())
	sp.lastAllocated = resp.Min
	return resp.Min, resp.Max, err
}

const backoffMin = 5 * time.Millisecond
const backoffMax = 100 * time.Millisecond

type backoffer struct {
	time.Duration
}

func (b *backoffer) Reset() {
	b.Duration = backoffMin
}

// Backoff sleeps for the current duration. If ctx is provided and canceled during
// the sleep, it returns early with the context error. This prevents a canceled
// context from being blocked by the full backoff duration.
func (b *backoffer) Backoff(ctx ...context.Context) error {
	if b.Duration == 0 {
		b.Duration = backoffMin
	}
	b.Duration *= 2
	if b.Duration > backoffMax {
		b.Duration = backoffMax
	}
	if len(ctx) > 0 && ctx[0] != nil {
		timer := time.NewTimer(b.Duration)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx[0].Done():
			return ctx[0].Err()
		}
	}
	time.Sleep(b.Duration)
	return nil
}

func (d *ClientDiscover) resetConn(version uint64, reason error) {
	d.mu.Lock()
	if d.mu.version != version {
		d.mu.Unlock()
		return
	}
	grpcConn := d.resetConnLocked()
	d.mu.Unlock()
	d.afterReset(grpcConn, reason)
}

// ResetConn reset the AutoIDAllocClient and underlying grpc connection.
// The next GetClient() call will recreate the client connecting to the correct leader by querying etcd.
func (d *ClientDiscover) ResetConn(reason error) {
	d.mu.Lock()
	grpcConn := d.resetConnLocked()
	d.mu.Unlock()
	d.afterReset(grpcConn, reason)
}

func (d *ClientDiscover) resetConnLocked() *grpc.ClientConn {
	d.mu.version++
	grpcConn := d.mu.ClientConn
	d.mu.AutoIDAllocClient = nil
	d.mu.ClientConn = nil
	d.mu.leaderSnapshot = autoIDLeaderSnapshot{}
	return grpcConn
}

func (*ClientDiscover) afterReset(grpcConn *grpc.ClientConn, reason error) {
	if reason != nil {
		logutil.BgLogger().Info("reset grpc connection", zap.String("category", "autoid client"),
			zap.String("reason", reason.Error()))
	}

	metrics.ResetAutoIDConnCounter.Add(1)
	// Close grpc.ClientConn to release resource.
	if grpcConn != nil {
		go func() {
			// Don't close the conn immediately, in case the other sessions are still using it.
			time.Sleep(200 * time.Millisecond)
			err := grpcConn.Close()
			if err != nil {
				logutil.BgLogger().Warn("close grpc connection error", zap.String("category", "autoid client"), zap.Error(err))
			}
		}()
	}
}

func (sp *singlePointAlloc) Transfer(databaseID, tableID int64) error {
	if sp.dbID == databaseID && sp.tblID == tableID {
		return nil
	}
	sp.dbID = databaseID
	sp.tblID = tableID
	return sp.Rebase(context.Background(), sp.lastAllocated+1, false)
}

// AllocSeqCache allocs sequence batch value cached in table level（rather than in alloc), the returned range covering
// the size of sequence cache with it's increment. The returned round indicates the sequence cycle times if it is with
// cycle option.
func (*singlePointAlloc) AllocSeqCache() (a int64, b int64, c int64, err error) {
	return 0, 0, 0, errors.New("AllocSeqCache not implemented")
}

// Rebase rebases the autoID base for table with tableID and the new base value.
// If allocIDs is true, it will allocate some IDs and save to the cache.
// If allocIDs is false, it will not allocate IDs.
func (sp *singlePointAlloc) Rebase(ctx context.Context, newBase int64, _ bool) error {
	r, ctx := tracing.StartRegionEx(ctx, "autoid.Rebase")
	defer r.End()

	start := time.Now()
	err := sp.rebase(ctx, newBase, false)
	metrics.AutoIDHistogram.WithLabelValues(metrics.TableAutoIDRebase, metrics.RetLabel(err)).Observe(time.Since(start).Seconds())
	return err
}

func (sp *singlePointAlloc) rebase(ctx context.Context, newBase int64, force bool) (retErr error) {
	var bo backoffer
	start := time.Now()
	requestLog := newAutoIDRequestLogState("rebase", sp.keyspaceID, sp.dbID, sp.tblID, start)
	defer func() {
		requestLog.complete(retErr)
	}()
	var notLeaderState notLeaderRetryState
retry:
	cli, ver, leader, err := sp.getClientWithLeader(ctx, sp.keyspaceID)
	if err != nil {
		return errors.Trace(err)
	}
	requestLog.setLeader(leader)
	var resp *autoid.RebaseResponse
	resp, err = cli.Rebase(ctx, &autoid.RebaseRequest{
		DbID:       sp.dbID,
		TblID:      sp.tblID,
		Base:       newBase,
		Force:      force,
		IsUnsigned: sp.isUnsigned,
	})
	if err != nil {
		if ctx.Err() != nil {
			return errors.Trace(ctx.Err())
		}
		handled, retryImmediately, terminalErr := sp.handleNotLeaderError(
			ctx,
			"rebase",
			ver,
			leader,
			err,
			&notLeaderState,
			&requestLog,
		)
		if handled {
			if terminalErr != nil {
				return terminalErr
			}
			if !retryImmediately {
				if err := bo.Backoff(ctx); err != nil {
					return errors.Trace(err)
				}
			}
			goto retry
		}
		notLeaderState.reset()
		if strings.Contains(err.Error(), "rpc error") {
			// Same as Alloc: check ctx before resetting connection and retrying.
			if ctx.Err() != nil {
				return errors.Trace(ctx.Err())
			}
			sp.resetConn(ver, err)
			if err := bo.Backoff(ctx); err != nil {
				return errors.Trace(err)
			}
			goto retry
		}
		return errors.Trace(err)
	}
	bo.Reset()
	if len(resp.Errmsg) != 0 {
		return errors.Trace(errors.New(string(resp.Errmsg)))
	}
	sp.lastAllocated = newBase
	return nil
}

// ForceRebase set the next global auto ID to newBase.
func (sp *singlePointAlloc) ForceRebase(newBase int64) error {
	if newBase == -1 {
		return ErrAutoincReadFailed.GenWithStack("Cannot force rebase the next global ID to '0'")
	}
	return sp.rebase(context.Background(), newBase, true)
}

// RebaseSeq rebases the sequence value in number axis with tableID and the new base value.
func (*singlePointAlloc) RebaseSeq(_ int64) (int64, bool, error) {
	return 0, false, errors.New("RebaseSeq not implemented")
}

// Base return the current base of Allocator.
func (sp *singlePointAlloc) Base() int64 {
	return sp.lastAllocated
}

// End is only used for test.
func (sp *singlePointAlloc) End() int64 {
	return sp.lastAllocated
}

// NextGlobalAutoID returns the next global autoID.
// Used by 'show create table', 'alter table auto_increment = xxx'
func (sp *singlePointAlloc) NextGlobalAutoID() (int64, error) {
	_, maxv, err := sp.Alloc(context.Background(), 0, 1, 1)
	return maxv + 1, err
}

func (*singlePointAlloc) GetType() AllocatorType {
	return AutoIncrementType
}
