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
	respCh := make(chan *pb.StreamResponse, 100)
	errCh := make(chan error, 1)
	pr := &pendingRequest{requestID: requestID, respCh: respCh, done: make(chan struct{})}

	sw.pendingMu.Lock()
	sw.pending[requestID] = pr
	sw.pendingMu.Unlock()

	select {
	case sw.sendCh <- &sendRequest{requestID: requestID, req: req, errCh: errCh}:
	case <-ctx.Done():
		sw.removePending(requestID, pr)
		return nil, ctx.Err()
	case <-sw.closedCh:
		sw.removePending(requestID, pr)
		return nil, errors.New("stream closed")
	}

	select {
	case err := <-errCh:
		sw.removePending(requestID, pr)
		return nil, err
	case resp, ok := <-respCh:
		if !ok {
			// Channel was closed, stream died before first response
			return nil, errors.New("stream closed before receiving response")
		}
		return c.handleFirstResponse(resp, requestID, respCh, pr, sw, sctx)
	case <-ctx.Done():
		sw.removePending(requestID, pr)
		return nil, ctx.Err()
	case <-sw.closedCh:
		// Stream was closed while waiting for response
		sw.removePending(requestID, pr)
		return nil, errors.New("stream closed")
	}
}

func (c *BatchClient) handleFirstResponse(resp *pb.StreamResponse, requestID uint64, respCh chan *pb.StreamResponse, pr *pendingRequest, sw *streamWrapper, sctx sessionctx.Context) (sqlexec.RecordSet, error) {
	if resp.Err != "" {
		sw.removePending(requestID, pr)
		return nil, errors.New(resp.Err)
	}
	// The local stmt context shouldn't keep holding on to the memory/disk tracker
	// after we switch to remote execution. Detach early so even if the caller
	// forgets to close the record set, the trackers don't leak.
	if sctx != nil {
		sctx.GetSessionVars().StmtCtx.DetachMemDiskTracker()
	}
	if len(resp.ColumnInfos) == 0 {
		sw.removePending(requestID, pr)
		if resp.DmlResult != nil {
			info := resp.DmlResult.Info
			var warns []contextutil.SQLWarn
			if len(resp.DmlResult.Warnings) > 0 {
				warns = make([]contextutil.SQLWarn, 0, len(resp.DmlResult.Warnings))
				for _, w := range resp.DmlResult.Warnings {
					if w == nil {
						continue
					}
					warns = append(warns, contextutil.SQLWarn{
						Level: w.Level,
						Err: &terror.TiDBError{
							MYSQLERRNO:  int64(w.Code),
							MESSAGETEXT: w.Message,
						},
					})
				}
			} else if resp.DmlResult.WarningCount > 0 {
				// Backward compatibility: preserve warning count even if warning details are not sent.
				warns = make([]contextutil.SQLWarn, 0, int(resp.DmlResult.WarningCount))
				for i := uint32(0); i < resp.DmlResult.WarningCount; i++ {
					warns = append(warns, contextutil.SQLWarn{
						Level: contextutil.WarnLevelWarning,
						Err: &terror.TiDBError{
							MYSQLERRNO:  int64(mysql.ErrUnknown),
							MESSAGETEXT: "",
						},
					})
				}
			}
			return &DMLRecordSet{
				sctx:         sctx,
				affectedRows: resp.DmlResult.AffectedRows,
				lastInsertID: resp.DmlResult.LastInsertId,
				statusFlags:  resp.DmlResult.StatusFlags,
				warningCount: resp.DmlResult.WarningCount,
				info:         info,
				warnings:     warns,
			}, nil
		}
		return &DMLRecordSet{sctx: sctx}, nil
	}
	return newBatchStreamingRecordSet(requestID, resp.ColumnInfos, respCh, pr, sw, sctx), nil
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
		resp, err := sw.stream.Recv()
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
			select {
			case pr.respCh <- streamResp:
				// Successfully sent
			case <-pr.done:
				// Request was closed, skip remaining responses for this request
				continue
			case <-sw.closedCh:
				// Stream is closing, stop processing
				return
			}
			if !streamResp.HasMore {
				sw.pendingMu.Lock()
				// Check again in case it was removed by Close()
				if _, stillExists := sw.pending[streamResp.RequestId]; stillExists {
					delete(sw.pending, streamResp.RequestId)
					sw.pendingMu.Unlock()
					// Only close done if we were the one to remove it
					pr.closeDone()
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
