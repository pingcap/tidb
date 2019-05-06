package tikv

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// batchClientTester is a thin wrap over the rpcClient used to test command batching
type batchClientTester struct {
	rpcClient Client
	cfg       BatchClientTestConfig
	failed    bool
	failLock  sync.Mutex
	ended     bool
	endLock   sync.Mutex
	wait      sync.WaitGroup
}

// BatchClientTestConfig is used to config batchClientTester
type BatchClientTestConfig struct {
	Interval   time.Duration
	Timeout    time.Duration
	MinDelay   uint64
	MaxDelay   uint64
	TestLength time.Duration
}

func (cfg *BatchClientTestConfig) genDelay() uint64 {
	r := rand.Int63n(int64(cfg.MaxDelay - cfg.MinDelay))
	return uint64(r) + cfg.MinDelay
}

// BatchTest start sending test messages to TiKV servers
func BatchTest(security config.Security, addrs []string, config BatchClientTestConfig) {
	c := &batchClientTester{
		rpcClient: newRPCClient(security),
		cfg:       config,
		failed:    false,
		ended:     false,
	}
	c.wait.Add(len(addrs))
	for _, addr := range addrs {
		go c.runTest(addr)
	}
	<-time.After(c.cfg.TestLength)
	c.end()
	c.wait.Wait()
	//TODO: close the client
}

type sendRequestResult struct {
	*tikvrpc.Response
	error
}

// logger get default logger
func logger() *zap.SugaredLogger {
	return logutil.Logger(context.Background()).Sugar()
}

// fail make the test as failed
func (c *batchClientTester) fail() {
	c.failLock.Lock()
	c.failed = true
	c.failLock.Unlock()
}

// isFailed get is it failed
func (c *batchClientTester) isFailed() bool {
	c.failLock.Lock()
	failed := c.failed
	c.failLock.Unlock()
	return failed
}

// end make the test end
func (c *batchClientTester) end() {
	c.endLock.Lock()
	c.ended = true
	c.endLock.Unlock()
}

// isEnded get is it ended
func (c *batchClientTester) isEnded() bool {
	c.endLock.Lock()
	ended := c.ended
	c.endLock.Unlock()
	return ended
}

// unblockedSend invoke SendRequest, but returns a channel instead of blocking the current goroutine
func (c *batchClientTester) unblockedSend(
	ctx context.Context,
	addr string,
	req *tikvrpc.Request,
	timeout time.Duration,
) <-chan sendRequestResult {
	done := make(chan sendRequestResult, 1)
	go func() {
		res, err := c.rpcClient.SendRequest(ctx, addr, req, timeout)
		done <- sendRequestResult{res, err}
	}()
	return done
}

// test sends test messages to the server and process and check respond
func (c *batchClientTester) test(addr string, id uint64) {
	defer func() { c.wait.Done() }()
	logger().Infof("Invoke test RPC %d at %v", id, addr)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdBatchTest,
		BatchTest: &tikvpb.BatchCommandTestRequest{
			TestId:    id,
			DelayTime: c.cfg.genDelay(),
		},
	}
	done := c.unblockedSend(context.Background(), addr, req, c.cfg.Timeout)
	select {
	case result := <-done:
		err := result.error
		if err != nil {
			// detected conection error on time, success
			logger().Infof("Test RPC %d at %v timeout", id, addr)
			return
		}
		res := result.Response.Test
		if res == nil {
			// wrong response type
			logger().Errorf("Test RPC %d at %v returned wrong type of response", id, addr)
			c.fail()
			return
		}
		if res.TestId != id {
			// wrong id
			logger().Errorf("Test RPC %d at %v returned wrong id %d", id, addr, res.TestId)
			c.fail()
			return
		}
		//TODO: check timing
	case <-time.After(100*time.Millisecond + c.cfg.Timeout):
		// do not finish in time
		// better timing?
		logger().Errorf("Test RPC %d at %v do not response or error on time", id, addr)
		c.fail()
	}
}

// runTest run test for one TiKV server
func (c *batchClientTester) runTest(addr string) {
	defer func() { c.wait.Done() }()
	ticker := time.NewTicker(c.cfg.Interval)
	defer func() { ticker.Stop() }()
	var id uint64
	for range ticker.C {
		if c.isEnded() {
			return
		}
		c.wait.Add(1)
		go c.test(addr, id)
		id++
	}
}
