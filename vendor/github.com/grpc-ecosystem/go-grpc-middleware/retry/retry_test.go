// Copyright 2016 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_retry_test

import (
	"io"
	"sync"
	"testing"
	"time"

	pb_testproto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"

	"github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/grpc-ecosystem/go-grpc-middleware/testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	retriableErrors = []codes.Code{codes.Unavailable, codes.DataLoss}
	goodPing        = &pb_testproto.PingRequest{Value: "something"}
	noSleep         = 0 * time.Second
	retryTimeout    = 50 * time.Millisecond
)

type failingService struct {
	pb_testproto.TestServiceServer
	reqCounter uint
	reqModulo  uint
	reqSleep   time.Duration
	reqError   codes.Code
	mu         sync.Mutex
}

func (s *failingService) resetFailingConfiguration(modulo uint, errorCode codes.Code, sleepTime time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reqCounter = 0
	s.reqModulo = modulo
	s.reqError = errorCode
	s.reqSleep = sleepTime
}

func (s *failingService) requestCount() uint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reqCounter
}

func (s *failingService) maybeFailRequest() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reqCounter += 1
	if (s.reqModulo > 0) && (s.reqCounter%s.reqModulo == 0) {
		return nil
	}
	time.Sleep(s.reqSleep)
	return grpc.Errorf(s.reqError, "maybeFailRequest: failing it")
}

func (s *failingService) Ping(ctx context.Context, ping *pb_testproto.PingRequest) (*pb_testproto.PingResponse, error) {
	if err := s.maybeFailRequest(); err != nil {
		return nil, err
	}
	return s.TestServiceServer.Ping(ctx, ping)
}

func (s *failingService) PingList(ping *pb_testproto.PingRequest, stream pb_testproto.TestService_PingListServer) error {
	if err := s.maybeFailRequest(); err != nil {
		return err
	}
	return s.TestServiceServer.PingList(ping, stream)
}

func (s *failingService) PingStream(stream pb_testproto.TestService_PingStreamServer) error {
	if err := s.maybeFailRequest(); err != nil {
		return err
	}
	return s.TestServiceServer.PingStream(stream)
}

func TestRetrySuite(t *testing.T) {
	service := &failingService{
		TestServiceServer: &grpc_testing.TestPingService{T: t},
	}
	unaryInterceptor := grpc_retry.UnaryClientInterceptor(
		grpc_retry.WithCodes(retriableErrors...),
		grpc_retry.WithMax(3),
		grpc_retry.WithBackoff(grpc_retry.BackoffLinear(retryTimeout)),
	)
	streamInterceptor := grpc_retry.StreamClientInterceptor(
		grpc_retry.WithCodes(retriableErrors...),
		grpc_retry.WithMax(3),
		grpc_retry.WithBackoff(grpc_retry.BackoffLinear(retryTimeout)),
	)
	s := &RetrySuite{
		srv: service,
		InterceptorTestSuite: &grpc_testing.InterceptorTestSuite{
			TestService: service,
			ClientOpts: []grpc.DialOption{
				grpc.WithStreamInterceptor(streamInterceptor),
				grpc.WithUnaryInterceptor(unaryInterceptor),
			},
		},
	}
	suite.Run(t, s)
}

type RetrySuite struct {
	*grpc_testing.InterceptorTestSuite
	srv *failingService
}

func (s *RetrySuite) SetupTest() {
	s.srv.resetFailingConfiguration( /* don't fail */ 0, codes.OK, noSleep)
}

func (s *RetrySuite) TestUnary_FailsOnNonRetriableError() {
	s.srv.resetFailingConfiguration(5, codes.Internal, noSleep)
	_, err := s.Client.Ping(s.SimpleCtx(), goodPing)
	require.Error(s.T(), err, "error must occur from the failing service")
	require.Equal(s.T(), codes.Internal, grpc.Code(err), "failure code must come from retrier")
}

func (s *RetrySuite) TestCallOptionsDontPanicWithoutInterceptor() {
	// Fix for https://github.com/grpc-ecosystem/go-grpc-middleware/issues/37
	// If this code doesn't panic, that's good.
	s.srv.resetFailingConfiguration(100, codes.DataLoss, noSleep) // doesn't matter all requests should fail
	nonMiddlewareClient := s.NewClient()
	_, err := nonMiddlewareClient.Ping(s.SimpleCtx(), goodPing,
		grpc_retry.WithMax(5),
		grpc_retry.WithBackoff(grpc_retry.BackoffLinear(1*time.Millisecond)),
		grpc_retry.WithCodes(codes.DataLoss),
		grpc_retry.WithPerRetryTimeout(1 * time.Millisecond),
	)
	require.Error(s.T(), err)
}

func (s *RetrySuite) TestServerStream_FailsOnNonRetriableError() {
	s.srv.resetFailingConfiguration(5, codes.Internal, noSleep)
	stream, err := s.Client.PingList(s.SimpleCtx(), goodPing)
	require.NoError(s.T(), err, "should not fail on establishing the stream")
	_, err = stream.Recv()
	require.Error(s.T(), err, "error must occur from the failing service")
	require.Equal(s.T(), codes.Internal, grpc.Code(err), "failure code must come from retrier")
}

func (s *RetrySuite) TestUnary_SucceedsOnRetriableError() {
	s.srv.resetFailingConfiguration(3, codes.DataLoss, noSleep) // see retriable_errors
	out, err := s.Client.Ping(s.SimpleCtx(), goodPing)
	require.NoError(s.T(), err, "the third invocation should succeed")
	require.NotNil(s.T(), out, "Pong must be not nill")
	require.EqualValues(s.T(), 3, s.srv.requestCount(), "three requests should have been made")
}

func (s *RetrySuite) TestUnary_OverrideFromDialOpts() {
	s.srv.resetFailingConfiguration(5, codes.ResourceExhausted, noSleep) // default is 3 and retriable_errors
	out, err := s.Client.Ping(s.SimpleCtx(), goodPing, grpc_retry.WithCodes(codes.ResourceExhausted), grpc_retry.WithMax(5))
	require.NoError(s.T(), err, "the fifth invocation should succeed")
	require.NotNil(s.T(), out, "Pong must be not nill")
	require.EqualValues(s.T(), 5, s.srv.requestCount(), "five requests should have been made")
}

func (s *RetrySuite) TestUnary_PerCallDeadline_Succeeds() {
	// This tests 5 requests, with first 4 sleeping for 10 millisecond, and the retry logic firing
	// a retry call with a 5 millisecond deadline. The 5th one doesn't sleep and succeeds.
	deadlinePerCall := 5 * time.Millisecond
	s.srv.resetFailingConfiguration(5, codes.NotFound, 2*deadlinePerCall)
	out, err := s.Client.Ping(s.SimpleCtx(), goodPing, grpc_retry.WithPerRetryTimeout(deadlinePerCall),
		grpc_retry.WithMax(5))
	require.NoError(s.T(), err, "the fifth invocation should succeed")
	require.NotNil(s.T(), out, "Pong must be not nill")
	require.EqualValues(s.T(), 5, s.srv.requestCount(), "five requests should have been made")
}

func (s *RetrySuite) TestUnary_PerCallDeadline_FailsOnParent() {
	// This tests that the parent context (passed to the invocation) takes precedence over retries.
	// The parent context has 150 milliseconds of deadline.
	// Each failed call sleeps for 100milliseconds, and there is 5 milliseconds between each one.
	// This means that unlike in TestUnary_PerCallDeadline_Succeeds, the fifth successful call won't
	// be made.
	parentDeadline := 150 * time.Millisecond
	deadlinePerCall := 50 * time.Millisecond
	// All 0-4 requests should have 10 millisecond sleeps and deadline, while the last one works.
	s.srv.resetFailingConfiguration(5, codes.NotFound, 2*deadlinePerCall)
	ctx, _ := context.WithTimeout(context.TODO(), parentDeadline)
	_, err := s.Client.Ping(ctx, goodPing, grpc_retry.WithPerRetryTimeout(deadlinePerCall),
		grpc_retry.WithMax(5))
	require.Error(s.T(), err, "the retries must fail due to context deadline exceeded")
	require.Equal(s.T(), codes.DeadlineExceeded, grpc.Code(err), "failre code must be a gRPC error of Deadline class")
}

func (s *RetrySuite) TestServerStream_SucceedsOnRetriableError() {
	s.srv.resetFailingConfiguration(3, codes.DataLoss, noSleep) // see retriable_errors
	stream, err := s.Client.PingList(s.SimpleCtx(), goodPing)
	require.NoError(s.T(), err, "establishing the connection must always succeed")
	s.assertPingListWasCorrect(stream)
	require.EqualValues(s.T(), 3, s.srv.requestCount(), "three requests should have been made")
}

func (s *RetrySuite) TestServerStream_OverrideFromContext() {
	s.srv.resetFailingConfiguration(5, codes.ResourceExhausted, noSleep) // default is 3 and retriable_errors
	stream, err := s.Client.PingList(s.SimpleCtx(), goodPing, grpc_retry.WithCodes(codes.ResourceExhausted), grpc_retry.WithMax(5))
	require.NoError(s.T(), err, "establishing the connection must always succeed")
	s.assertPingListWasCorrect(stream)
	require.EqualValues(s.T(), 5, s.srv.requestCount(), "three requests should have been made")
}

func (s *RetrySuite) TestServerStream_PerCallDeadline_Succeeds() {
	// This tests 5 requests, with first 4 sleeping for 100 millisecond, and the retry logic firing
	// a retry call with a 50 millisecond deadline. The 5th one doesn't sleep and succeeds.
	deadlinePerCall := 50 * time.Millisecond
	s.srv.resetFailingConfiguration(5, codes.NotFound, 2*deadlinePerCall)
	stream, err := s.Client.PingList(s.SimpleCtx(), goodPing, grpc_retry.WithPerRetryTimeout(deadlinePerCall),
		grpc_retry.WithMax(5))
	require.NoError(s.T(), err, "establishing the connection must always succeed")
	s.assertPingListWasCorrect(stream)
	require.EqualValues(s.T(), 5, s.srv.requestCount(), "three requests should have been made")
}

func (s *RetrySuite) TestServerStream_PerCallDeadline_FailsOnParent() {
	// This tests that the parent context (passed to the invocation) takes precedence over retries.
	// The parent context has 150 milliseconds of deadline.
	// Each failed call sleeps for 50milliseconds, and there is 25 milliseconds between each one.
	// This means that unlike in TestServerStream_PerCallDeadline_Succeeds, the fifth successful call won't
	// be made.
	parentDeadline := 150 * time.Millisecond
	deadlinePerCall := 50 * time.Millisecond
	// All 0-4 requests should have 10 millisecond sleeps and deadline, while the last one works.
	s.srv.resetFailingConfiguration(5, codes.NotFound, 2*deadlinePerCall)
	parentCtx, _ := context.WithTimeout(context.TODO(), parentDeadline)
	stream, err := s.Client.PingList(parentCtx, goodPing, grpc_retry.WithPerRetryTimeout(deadlinePerCall),
		grpc_retry.WithMax(5))
	require.NoError(s.T(), err, "establishing the connection must always succeed")
	_, err = stream.Recv()
	require.Equal(s.T(), codes.DeadlineExceeded, grpc.Code(err), "failre code must be a gRPC error of Deadline class")
}

func (s *RetrySuite) assertPingListWasCorrect(stream pb_testproto.TestService_PingListClient) {
	count := 0
	for {
		pong, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NotNil(s.T(), pong, "received values must not be nill")
		require.NoError(s.T(), err, "no errors during receive on client side")
		require.Equal(s.T(), goodPing.Value, pong.Value, "the returned pong contained the outgoing ping")
		count += 1
	}
	require.EqualValues(s.T(), grpc_testing.ListResponseCount, count, "should have received all ping items")
}
