// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_zap_test

import (
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	pb_testproto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"
	"go.uber.org/zap/zapcore"
)

func customClientCodeToLevel(c codes.Code) zapcore.Level {
	if c == codes.Unauthenticated {
		// Make this a special case for tests, and an error.
		return zapcore.ErrorLevel
	}
	level := grpc_zap.DefaultClientCodeToLevel(c)
	return level
}

func TestZapClientSuite(t *testing.T) {
	if strings.HasPrefix(runtime.Version(), "go1.7") {
		t.Skipf("Skipping due to json.RawMessage incompatibility with go1.7")
		return
	}
	opts := []grpc_zap.Option{
		grpc_zap.WithLevels(customClientCodeToLevel),
	}
	b := newBaseZapSuite(t)
	b.InterceptorTestSuite.ClientOpts = []grpc.DialOption{
		grpc.WithUnaryInterceptor(grpc_zap.UnaryClientInterceptor(b.log, opts...)),
		grpc.WithStreamInterceptor(grpc_zap.StreamClientInterceptor(b.log, opts...)),
	}
	suite.Run(t, &zapClientSuite{b})
}

type zapClientSuite struct {
	*zapBaseSuite
}

func (s *zapClientSuite) TestPing() {
	_, err := s.Client.Ping(s.SimpleCtx(), goodPing)
	assert.NoError(s.T(), err, "there must be not be an on a successful call")
	msgs := s.getOutputJSONs()
	require.Len(s.T(), msgs, 1, "one log statement should be logged")
	m := msgs[0]
	assert.Contains(s.T(), m, `"grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
	assert.Contains(s.T(), m, `"grpc.method": "Ping"`, "all lines must contain method name")
	assert.Contains(s.T(), m, `"span.kind": "client"`, "all lines must contain the kind of call (client)")
	assert.Contains(s.T(), m, `"msg": "finished client unary call"`, "interceptor message must contain string")
	assert.Contains(s.T(), m, `"level": "debug"`, "OK error codes must be logged on debug level.")
	assert.Contains(s.T(), m, `"grpc.time_ms":`, "interceptor log statement should contain execution time")
}

func (s *zapClientSuite) TestPingList() {
	stream, err := s.Client.PingList(s.SimpleCtx(), goodPing)
	require.NoError(s.T(), err, "should not fail on establishing the stream")
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(s.T(), err, "reading stream should not fail")
	}
	msgs := s.getOutputJSONs()
	require.Len(s.T(), msgs, 1, "one log statement should be logged")
	m := msgs[0]
	assert.Contains(s.T(), m, `"grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
	assert.Contains(s.T(), m, `"grpc.method": "PingList"`, "all lines must contain method name")
	assert.Contains(s.T(), m, `"span.kind": "client"`, "all lines must contain the kind of call (client)")
	assert.Contains(s.T(), m, `"msg": "finished client streaming call"`, "interceptor message must contain string")
	assert.Contains(s.T(), m, `"level": "debug"`, "OK error codes must be logged on debug level.")
	assert.Contains(s.T(), m, `"grpc.time_ms":`, "interceptor log statement should contain execution time")
}

func (s *zapClientSuite) TestPingError_WithCustomLevels() {
	for _, tcase := range []struct {
		code  codes.Code
		level zapcore.Level
		msg   string
	}{
		{
			code:  codes.Internal,
			level: zapcore.WarnLevel,
			msg:   "Internal must remap to ErrorLevel in DefaultClientCodeToLevel",
		},
		{
			code:  codes.NotFound,
			level: zapcore.DebugLevel,
			msg:   "NotFound must remap to InfoLevel in DefaultClientCodeToLevel",
		},
		{
			code:  codes.FailedPrecondition,
			level: zapcore.DebugLevel,
			msg:   "FailedPrecondition must remap to WarnLevel in DefaultClientCodeToLevel",
		},
		{
			code:  codes.Unauthenticated,
			level: zapcore.ErrorLevel,
			msg:   "Unauthenticated is overwritten to ErrorLevel with customClientCodeToLevel override, which probably didn't work",
		},
	} {
		s.SetupTest()
		_, err := s.Client.PingError(
			s.SimpleCtx(),
			&pb_testproto.PingRequest{Value: "something", ErrorCodeReturned: uint32(tcase.code)})
		assert.Error(s.T(), err, "each call here must return an error")
		msgs := s.getOutputJSONs()
		require.Len(s.T(), msgs, 1, "only the interceptor log message is printed in PingErr")
		m := msgs[0]
		assert.Contains(s.T(), m, `"grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
		assert.Contains(s.T(), m, `"grpc.method": "PingError"`, "all lines must contain method name")
		assert.Contains(s.T(), m, fmt.Sprintf(`"grpc.code": "%s"`, tcase.code.String()), "all lines must contain method name")
		assert.Contains(s.T(), m, fmt.Sprintf(`"level": "%s"`, tcase.level.String()), tcase.msg)
	}
}

func TestZapClientOverrideSuite(t *testing.T) {
	if strings.HasPrefix(runtime.Version(), "go1.7") {
		t.Skip("Skipping due to json.RawMessage incompatibility with go1.7")
		return
	}
	opts := []grpc_zap.Option{
		grpc_zap.WithDurationField(grpc_zap.DurationToDurationField),
	}
	b := newBaseZapSuite(t)
	b.InterceptorTestSuite.ClientOpts = []grpc.DialOption{
		grpc.WithUnaryInterceptor(grpc_zap.UnaryClientInterceptor(b.log, opts...)),
		grpc.WithStreamInterceptor(grpc_zap.StreamClientInterceptor(b.log, opts...)),
	}
	suite.Run(t, &zapClientOverrideSuite{b})
}

type zapClientOverrideSuite struct {
	*zapBaseSuite
}

func (s *zapClientOverrideSuite) TestPing_HasOverrides() {
	_, err := s.Client.Ping(s.SimpleCtx(), goodPing)
	assert.NoError(s.T(), err, "there must be not be an on a successful call")
	msgs := s.getOutputJSONs()
	require.Len(s.T(), msgs, 1, "one log statement should be logged")
	m := msgs[0]
	assert.Contains(s.T(), m, `"grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
	assert.Contains(s.T(), m, `"grpc.method": "Ping"`, "all lines must contain method name")
	assert.Contains(s.T(), m, `"span.kind": "client"`, "all lines must contain the kind of call (client)")
	assert.Contains(s.T(), m, `"msg": "finished client unary call"`, "interceptor message must contain string")
	assert.Contains(s.T(), m, `"level": "debug"`, "OK error codes must be logged on debug level.")
	assert.NotContains(s.T(), m, "grpc.time_ms", "interceptor message must not contain default duration")
	assert.Contains(s.T(), m, "grpc.duration", "interceptor message must contain overridden duration")
}

func (s *zapClientOverrideSuite) TestPingList_HasOverrides() {
	stream, err := s.Client.PingList(s.SimpleCtx(), goodPing)
	require.NoError(s.T(), err, "should not fail on establishing the stream")
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(s.T(), err, "reading stream should not fail")
	}
	msgs := s.getOutputJSONs()
	require.Len(s.T(), msgs, 1, "one log statement should be logged")
	m := msgs[0]
	assert.Contains(s.T(), m, `"grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
	assert.Contains(s.T(), m, `"grpc.method": "PingList"`, "all lines must contain method name")
	assert.Contains(s.T(), m, `"span.kind": "client"`, "all lines must contain the kind of call (client)")
	assert.Contains(s.T(), m, `"msg": "finished client streaming call"`, "interceptor message must contain string")
	assert.Contains(s.T(), m, `"level": "debug"`, "OK error codes must be logged on debug level.")
	assert.NotContains(s.T(), m, "grpc.time_ms", "interceptor message must not contain default duration")
	assert.Contains(s.T(), m, "grpc.duration", "interceptor message must contain overridden duration")
}
