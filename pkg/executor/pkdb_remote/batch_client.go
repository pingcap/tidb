// Copyright 2024 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

package pkdbremote

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BatchConfig holds configuration for request batching
type BatchConfig struct {
	MaxBatchSize  int           // Maximum number of requests per batch
	DialTimeout   time.Duration // Timeout for establishing connection
	SendChanSize  int           // Size of send channel buffer
	MaxRetries    int           // Maximum number of retries for failed requests
	RetryInterval time.Duration // Interval between retries
}

// DefaultBatchConfig returns the default batch configuration
func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		MaxBatchSize:  128, // Batch up to 128 requests
		DialTimeout:   10 * time.Second,
		SendChanSize:  1000,
		MaxRetries:    2,
		RetryInterval: 50 * time.Millisecond,
	}
}

// BatchClient manages bidirectional streams to remote servers with request batching
type BatchClient struct {
	config        *BatchConfig
	streams       map[string]*streamWrapper
	streamsMu     sync.RWMutex
	nextRequestID uint64
	closed        int32
	closedCh      chan struct{}
	wg            sync.WaitGroup
}

type streamWrapper struct {
	target    string
	conn      *grpc.ClientConn
	stream    pb.RemoteExecService_BatchRemoteExecuteClient
	cancel    context.CancelFunc
	config    *BatchConfig
	pending   map[uint64]*pendingRequest
	pendingMu sync.Mutex
	sendCh    chan *sendRequest
	closed    int32
	closedCh  chan struct{}
	wg        sync.WaitGroup
	// closeErr stores the error that caused the stream to close
	// This is used to propagate errors to pending requests
	closeErr  error
	closeOnce sync.Once
	// createTime records when the stream was created for debugging
	createTime time.Time
	// Pre-allocated slices for batching to avoid repeated allocations in batchSendLoop.
	// These are only accessed by batchSendLoop, so no synchronization needed.
	batchReqs   []*pb.RemoteRequest
	batchIDs    []uint64
	batchErrChs []chan error
}

type pendingRequest struct {
	requestID uint64
	respCh    chan *pb.StreamResponse
	// doneClosed is used to track if done channel has been closed
	// 0 = not closed, 1 = closed
	doneClosed int32
	done       chan struct{}
}

// closeDone safely closes the done channel exactly once
func (pr *pendingRequest) closeDone() {
	if atomic.CompareAndSwapInt32(&pr.doneClosed, 0, 1) {
		close(pr.done)
	}
}

// Increased from 100 to 200 to reduce blocking when remote server sends responses faster
const defaultRespChSize = 200

var respChPool = sync.Pool{
	New: func() any { return make(chan *pb.StreamResponse, defaultRespChSize) },
}

func acquireRespCh() chan *pb.StreamResponse {
	ch, _ := respChPool.Get().(chan *pb.StreamResponse)
	if ch == nil {
		return make(chan *pb.StreamResponse, defaultRespChSize)
	}
	drainRespCh(ch)
	return ch
}

func releaseRespCh(ch chan *pb.StreamResponse) {
	if ch == nil {
		return
	}
	if closed := drainRespCh(ch); closed {
		return
	}
	respChPool.Put(ch)
}

func drainRespCh(ch chan *pb.StreamResponse) bool {
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return true
			}
		default:
			return false
		}
	}
}

type sendRequest struct {
	requestID uint64
	req       *pb.RemoteRequest
	errCh     chan error
}

// NewBatchClient creates a new batch client with default config
func NewBatchClient() *BatchClient {
	return NewBatchClientWithConfig(DefaultBatchConfig())
}

// NewBatchClientWithConfig creates a new batch client with custom config
func NewBatchClientWithConfig(config *BatchConfig) *BatchClient {
	if config == nil {
		config = DefaultBatchConfig()
	}
	return &BatchClient{
		config:   config,
		streams:  make(map[string]*streamWrapper),
		closedCh: make(chan struct{}),
	}
}

// DefaultBatchClient is the default batch client instance
var DefaultBatchClient *BatchClient

func init() { DefaultBatchClient = NewBatchClient() }

var unknownWarning = contextutil.SQLWarn{
	Level: contextutil.WarnLevelWarning,
	Err: &terror.TiDBError{
		MYSQLERRNO:  int64(mysql.ErrUnknown),
		MESSAGETEXT: "",
	},
}

var errChPool = sync.Pool{
	New: func() any { return make(chan error, 1) },
}

func acquireErrCh() chan error {
	ch, _ := errChPool.Get().(chan error)
	if ch == nil {
		return make(chan error, 1)
	}
	drainErrCh(ch)
	return ch
}

func releaseErrCh(ch chan error) {
	if ch == nil {
		return
	}
	drainErrCh(ch)
	errChPool.Put(ch)
}

func drainErrCh(ch chan error) {
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

// Execute executes a remote request using bidirectional streaming with batching
func (c *BatchClient) Execute(ctx context.Context, req *pb.RemoteRequest, sctx sessionctx.Context) (sqlexec.RecordSet, error) {
	if req == nil {
		return nil, errors.New("nil remote request")
	}
	defer releaseRemoteRequest(req)

	if req.Target == "" {
		return nil, errors.New("target address is required")
	}

	// Check if client is closed
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, errors.New("batch client is closed")
	}

	var lastErr error
	for retry := 0; retry <= c.config.MaxRetries; retry++ {
		if retry > 0 {
			// Wait before retry
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(c.config.RetryInterval):
			}
			logutil.BgLogger().Debug("retrying remote execute",
				zap.String("target", req.Target),
				zap.Int("retry", retry),
				zap.Error(lastErr))
		}

		rs, err := c.executeOnce(ctx, req, sctx)
		if err == nil {
			return rs, nil
		}

		lastErr = err

		// Check if error is retryable
		if !isRetryableError(err) {
			return nil, err
		}

		// Remove the failed stream so next attempt creates a new one
		c.removeStream(req.Target)
	}

	return nil, errors.Annotatef(lastErr, "failed after %d retries", c.config.MaxRetries+1)
}

// isRetryableError checks if an error is retryable
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// Context errors are not retryable
	if err == context.Canceled || err == context.DeadlineExceeded {
		return false
	}
	errStr := err.Error()
	// Check for context error messages (in case wrapped)
	if errStr == "context canceled" || errStr == "context deadline exceeded" {
		return false
	}
	// Stream closed errors are retryable
	if errStr == "stream closed" || errStr == "stream closed before receiving response" {
		return true
	}
	return true
}

// removeStream removes a stream from the cache and closes it
func (c *BatchClient) removeStream(target string) {
	c.streamsMu.Lock()
	if sw, ok := c.streams[target]; ok {
		delete(c.streams, target)
		go sw.close() // Close in background to avoid blocking
	}
	c.streamsMu.Unlock()
}

// removePending removes a pending request from the map and closes its done channel.
// This is a helper method to avoid repetitive lock/delete/unlock/closeDone patterns.
func (sw *streamWrapper) removePending(requestID uint64, pr *pendingRequest) {
	sw.pendingMu.Lock()
	delete(sw.pending, requestID)
	sw.pendingMu.Unlock()
	pr.closeDone()
}

// removePendingWithMetric removes a pending request and decrements the pending requests metric.
func (sw *streamWrapper) removePendingWithMetric(requestID uint64, pr *pendingRequest) {
	sw.removePending(requestID, pr)
	metrics.RemotePlanBatchClientPendingRequests.Dec()
}

// executeOnce executes a single request attempt
func (c *BatchClient) executeOnce(ctx context.Context, req *pb.RemoteRequest, sctx sessionctx.Context) (sqlexec.RecordSet, error) {
	sw, err := c.getOrCreateStream(ctx, req.Target)
	if err != nil {
		return nil, err
	}

	// Check if stream is already closed
	if atomic.LoadInt32(&sw.closed) == 1 {
		return nil, errors.New("stream closed")
	}

	requestID := atomic.AddUint64(&c.nextRequestID, 1)
	respCh := acquireRespCh()
	errCh := acquireErrCh()
	pr := &pendingRequest{requestID: requestID, respCh: respCh, done: make(chan struct{})}

	sw.pendingMu.Lock()
	sw.pending[requestID] = pr
	sw.pendingMu.Unlock()
	metrics.RemotePlanBatchClientPendingRequests.Inc()

	// Record time when request is submitted to sendCh for first_resp_duration
	firstRespStart := time.Now()

	// Try non-blocking send first, then blocking send if channel is full
	select {
	case sw.sendCh <- &sendRequest{requestID: requestID, req: req, errCh: errCh}:
		// Fast path: sent immediately
	default:
		// Slow path: channel is full, record metric and wait
		metrics.RemotePlanBatchClientSendChFull.Inc()
		select {
		case sw.sendCh <- &sendRequest{requestID: requestID, req: req, errCh: errCh}:
		case <-ctx.Done():
			sw.removePendingWithMetric(requestID, pr)
			releaseErrCh(errCh)
			return nil, ctx.Err()
		case <-sw.closedCh:
			sw.removePendingWithMetric(requestID, pr)
			releaseErrCh(errCh)
			return nil, errors.New("stream closed")
		}
	}

	select {
	case err := <-errCh:
		sw.removePendingWithMetric(requestID, pr)
		releaseErrCh(errCh)
		metrics.RemotePlanFirstRespErrDuration.Observe(time.Since(firstRespStart).Seconds())
		return nil, err
	case resp, ok := <-respCh:
		releaseErrCh(errCh)
		if !ok {
			// Channel was closed, stream died before first response
			metrics.RemotePlanFirstRespErrDuration.Observe(time.Since(firstRespStart).Seconds())
			return nil, errors.New("stream closed before receiving response")
		}
		// Record first response duration (excludes client data consumption time)
		if resp.Err != "" {
			metrics.RemotePlanFirstRespErrDuration.Observe(time.Since(firstRespStart).Seconds())
		} else {
			metrics.RemotePlanFirstRespOKDuration.Observe(time.Since(firstRespStart).Seconds())
		}
		return c.handleFirstResponse(resp, requestID, respCh, pr, sw, sctx)
	case <-ctx.Done():
		sw.removePendingWithMetric(requestID, pr)
		metrics.RemotePlanFirstRespErrDuration.Observe(time.Since(firstRespStart).Seconds())
		return nil, ctx.Err()
	case <-sw.closedCh:
		// Stream was closed while waiting for response
		sw.removePendingWithMetric(requestID, pr)
		metrics.RemotePlanFirstRespErrDuration.Observe(time.Since(firstRespStart).Seconds())
		return nil, errors.New("stream closed")
	}
}

func (c *BatchClient) handleFirstResponse(resp *pb.StreamResponse, requestID uint64, respCh chan *pb.StreamResponse, pr *pendingRequest, sw *streamWrapper, sctx sessionctx.Context) (sqlexec.RecordSet, error) {
	if resp.Err != "" {
		sw.removePendingWithMetric(requestID, pr)
		return nil, errors.New(resp.Err)
	}
	// The local stmt context shouldn't keep holding on to the memory/disk tracker
	// after we switch to remote execution. Detach early so even if the caller
	// forgets to close the record set, the trackers don't leak.
	if sctx != nil {
		sctx.GetSessionVars().StmtCtx.DetachMemDiskTracker()
	}
	if len(resp.ColumnInfos) == 0 {
		sw.removePendingWithMetric(requestID, pr)
		if !resp.HasMore {
			releaseRespCh(respCh)
		}
		if resp.DmlResult != nil {
			info := resp.DmlResult.Info
			var warns []contextutil.SQLWarn
			if len(resp.DmlResult.Warnings) > 0 {
				warns = make([]contextutil.SQLWarn, len(resp.DmlResult.Warnings))
				warnIdx := 0
				for _, w := range resp.DmlResult.Warnings {
					if w == nil {
						continue
					}
					warns[warnIdx] = contextutil.SQLWarn{
						Level: w.Level,
						Err: &terror.TiDBError{
							MYSQLERRNO:  int64(w.Code),
							MESSAGETEXT: w.Message,
						},
					}
					warnIdx++
				}
				warns = warns[:warnIdx]
			} else if resp.DmlResult.WarningCount > 0 {
				// Backward compatibility: preserve warning count even if warning details are not sent.
				warns = make([]contextutil.SQLWarn, int(resp.DmlResult.WarningCount))
				for i := range warns {
					warns[i] = unknownWarning
				}
			}
			dmlRS := acquireDMLRecordSet()
			dmlRS.sctx = sctx
			dmlRS.affectedRows = resp.DmlResult.AffectedRows
			dmlRS.lastInsertID = resp.DmlResult.LastInsertId
			dmlRS.statusFlags = resp.DmlResult.StatusFlags
			dmlRS.warningCount = resp.DmlResult.WarningCount
			dmlRS.info = info
			dmlRS.warnings = warns
			return dmlRS, nil
		}
		dmlRS := acquireDMLRecordSet()
		dmlRS.sctx = sctx
		return dmlRS, nil
	}
	rs := newBatchStreamingRecordSet(requestID, resp.ColumnInfos, respCh, pr, sw, sctx)
	rs.firstRespTime = time.Now() // Record when first response was received for client_consume_duration
	return rs, nil
}

func (c *BatchClient) getOrCreateStream(ctx context.Context, target string) (*streamWrapper, error) {
	// Fast path: check if stream exists and is healthy
	c.streamsMu.RLock()
	if sw, ok := c.streams[target]; ok && atomic.LoadInt32(&sw.closed) == 0 {
		c.streamsMu.RUnlock()
		return sw, nil
	}
	c.streamsMu.RUnlock()

	// Slow path: create new stream
	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()

	// Double check after acquiring write lock
	if sw, ok := c.streams[target]; ok && atomic.LoadInt32(&sw.closed) == 0 {
		return sw, nil
	}

	// Remove old closed stream if exists
	if oldSw, ok := c.streams[target]; ok {
		delete(c.streams, target)
		go oldSw.close() // Close in background
	}

	sw, err := c.createStream(ctx, target)
	if err != nil {
		return nil, err
	}
	c.streams[target] = sw
	return sw, nil
}

func (c *BatchClient) createStream(ctx context.Context, target string) (*streamWrapper, error) {
	dialCtx, dialCancel := context.WithTimeout(ctx, c.config.DialTimeout)
	defer dialCancel()

	conn, err := grpc.DialContext(dialCtx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithInitialWindowSize(16<<20),
		grpc.WithInitialConnWindowSize(16<<20),
		grpc.WithReadBufferSize(64*1024),
		grpc.WithWriteBufferSize(64*1024),
	)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to dial %s", target)
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	stream, err := pb.NewRemoteExecServiceClient(conn).BatchRemoteExecute(streamCtx)
	if err != nil {
		streamCancel()
		conn.Close()
		return nil, errors.Annotatef(err, "failed to create stream to %s", target)
	}

	sw := &streamWrapper{
		target:      target,
		conn:        conn,
		stream:      stream,
		cancel:      streamCancel,
		config:      c.config,
		pending:     make(map[uint64]*pendingRequest),
		sendCh:      make(chan *sendRequest, c.config.SendChanSize),
		closedCh:    make(chan struct{}),
		createTime:  time.Now(),
		batchReqs:   make([]*pb.RemoteRequest, 0, c.config.MaxBatchSize),
		batchIDs:    make([]uint64, 0, c.config.MaxBatchSize),
		batchErrChs: make([]chan error, 0, c.config.MaxBatchSize),
	}
	sw.wg.Add(2)
	go sw.batchSendLoop()
	go sw.recvLoop()

	metrics.RemotePlanBatchClientActiveStreams.Inc()

	logutil.BgLogger().Info("created new batch stream",
		zap.String("target", target),
	)

	return sw, nil
}

// Close closes the batch client
func (c *BatchClient) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	close(c.closedCh)

	c.streamsMu.Lock()
	for target, sw := range c.streams {
		sw.close()
		delete(c.streams, target)
	}
	c.streamsMu.Unlock()

	c.wg.Wait()
	return nil
}

// batchSendLoop sends requests immediately but tries to batch with any pending requests
// This is similar to TiKV's BatchCommands: no waiting, just opportunistic batching
func (sw *streamWrapper) batchSendLoop() {
	defer sw.wg.Done()

	// Use pre-allocated slices from streamWrapper to avoid repeated allocations
	reqs := sw.batchReqs
	ids := sw.batchIDs
	errChs := sw.batchErrChs

	for {
		// Wait for at least one request
		select {
		case <-sw.closedCh:
			return
		case first := <-sw.sendCh:
			reqs = append(reqs, first.req)
			ids = append(ids, first.requestID)
			errChs = append(errChs, first.errCh)
		}

		// Opportunistically collect more requests that are already waiting
		// No blocking wait - just drain what's available in the channel
	collectMore:
		for len(reqs) < sw.config.MaxBatchSize {
			select {
			case sr := <-sw.sendCh:
				reqs = append(reqs, sr.req)
				ids = append(ids, sr.requestID)
				errChs = append(errChs, sr.errCh)
			default:
				// No more requests waiting, send what we have
				break collectMore
			}
		}

		// Send the batch immediately
		batchSize := len(reqs)
		metrics.RemotePlanBatchClientBatchSize.Observe(float64(batchSize))
		batchReq := &pb.BatchRemoteRequest{Requests: reqs, RequestIds: ids}
		if err := sw.stream.Send(batchReq); err != nil {
			for _, errCh := range errChs {
				select {
				case errCh <- err:
				default:
				}
			}
			sw.closeWithError(err)
			return
		}

		// Clear slices for reuse (keep capacity)
		reqs = reqs[:0]
		ids = ids[:0]
		errChs = errChs[:0]
	}
}

func (sw *streamWrapper) recvLoop() {
	defer sw.wg.Done()
	for {
		select {
		case <-sw.closedCh:
			return
		default:
		}
		recvStart := time.Now()
		resp, err := sw.stream.Recv()
		metrics.RemotePlanGrpcRecvDuration.Observe(time.Since(recvStart).Seconds())
		if err != nil {
			if err != io.EOF {
				logutil.BgLogger().Warn("batch stream recv error", zap.String("target", sw.target), zap.Error(err))
				sw.closeWithError(err)
			} else {
				sw.closeWithError(errors.New("stream closed by server"))
			}
			return
		}
		for _, streamResp := range resp.Responses {
			sw.pendingMu.Lock()
			pr, ok := sw.pending[streamResp.RequestId]
			sw.pendingMu.Unlock()
			if !ok {
				// Request was already removed (e.g., by Close())
				// Skip this response
				continue
			}
			// Stream is being closed, avoid sending to channels that may be closed
			if atomic.LoadInt32(&sw.closed) == 1 {
				return
			}
			// If the stream closure started after the above check, closedCh will already be closed.
			// Bail out before attempting to send, otherwise a send on a closed respCh would panic.
			select {
			case <-sw.closedCh:
				return
			default:
			}
			// Send response to the request's channel
			// We must not drop responses as that would corrupt results or leave pending entries stuck
			// Try non-blocking send first to detect channel full condition
			select {
			case pr.respCh <- streamResp:
				// Successfully sent (fast path)
			default:
				// Channel is full, record metric and do blocking send
				metrics.RemotePlanBatchClientRespChFull.Inc()
				select {
				case pr.respCh <- streamResp:
					// Successfully sent after waiting
				case <-pr.done:
					// Request was closed, skip remaining responses for this request
					continue
				case <-sw.closedCh:
					// Stream is closing, stop processing
					return
				}
			}
			if !streamResp.HasMore {
				sw.pendingMu.Lock()
				// Check again in case it was removed by Close()
				if _, stillExists := sw.pending[streamResp.RequestId]; stillExists {
					delete(sw.pending, streamResp.RequestId)
					sw.pendingMu.Unlock()
					// Only close done if we were the one to remove it
					pr.closeDone()
					// Decrement pending requests counter
					metrics.RemotePlanBatchClientPendingRequests.Dec()
				} else {
					sw.pendingMu.Unlock()
				}
			}
		}
	}
}

// closeWithError closes the stream and propagates the error to all pending requests
func (sw *streamWrapper) closeWithError(err error) {
	sw.closeOnce.Do(func() {
		sw.closeErr = err
		sw.doClose()
	})
}

func (sw *streamWrapper) close() {
	sw.closeOnce.Do(func() {
		sw.doClose()
	})
}

func (sw *streamWrapper) doClose() {
	if !atomic.CompareAndSwapInt32(&sw.closed, 0, 1) {
		return
	}
	close(sw.closedCh)
	if sw.cancel != nil {
		sw.cancel()
	}
	if sw.conn != nil {
		sw.conn.Close()
	}
	sw.pendingMu.Lock()
	pendingCount := len(sw.pending)
	for _, pr := range sw.pending {
		// Close respCh to unblock any readers
		// The consumer (batchStreamingRecordSet) will check sw.GetCloseError() when respCh is closed
		// to get the actual error that caused the stream to close
		close(pr.respCh)
		// Close done channel to signal completion
		// Use atomic operation to ensure it's only closed once
		pr.closeDone()
	}
	sw.pending = make(map[uint64]*pendingRequest)
	sw.pendingMu.Unlock()

	// Update metrics after releasing lock
	metrics.RemotePlanBatchClientActiveStreams.Dec()
	if pendingCount > 0 {
		metrics.RemotePlanBatchClientPendingRequests.Sub(float64(pendingCount))
	}
}

// GetCloseError returns the error that caused the stream to close
func (sw *streamWrapper) GetCloseError() error {
	return sw.closeErr
}

// BatchClientStats holds statistics about the batch client
type BatchClientStats struct {
	ActiveStreams  int
	TotalRequests  uint64
	StreamsPerHost map[string]StreamStats
}

// StreamStats holds statistics about a single stream
type StreamStats struct {
	PendingRequests int
	CreateTime      time.Time
	Closed          bool
}

// Stats returns current statistics of the batch client
func (c *BatchClient) Stats() BatchClientStats {
	c.streamsMu.RLock()
	defer c.streamsMu.RUnlock()

	stats := BatchClientStats{
		TotalRequests:  atomic.LoadUint64(&c.nextRequestID),
		StreamsPerHost: make(map[string]StreamStats),
	}

	for target, sw := range c.streams {
		sw.pendingMu.Lock()
		pendingCount := len(sw.pending)
		sw.pendingMu.Unlock()

		isClosed := atomic.LoadInt32(&sw.closed) == 1
		stats.StreamsPerHost[target] = StreamStats{
			PendingRequests: pendingCount,
			CreateTime:      sw.createTime,
			Closed:          isClosed,
		}
		if !isClosed {
			stats.ActiveStreams++
		}
	}

	return stats
}
