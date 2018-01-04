// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_logrus_test

import (
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	pb_testproto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"
)

func customClientCodeToLevel(c codes.Code) logrus.Level {
	if c == codes.Unauthenticated {
		// Make this a special case for tests, and an error.
		return logrus.ErrorLevel
	}
	level := grpc_logrus.DefaultClientCodeToLevel(c)
	return level
}

func TestLogrusClientSuite(t *testing.T) {
	if strings.HasPrefix(runtime.Version(), "go1.7") {
		t.Skipf("Skipping due to json.RawMessage incompatibility with go1.7")
		return
	}
	opts := []grpc_logrus.Option{
		grpc_logrus.WithLevels(customClientCodeToLevel),
	}
	b := newLogrusBaseSuite(t)
	b.logger.Level = logrus.DebugLevel // a lot of our stuff is on debug level by default
	b.InterceptorTestSuite.ClientOpts = []grpc.DialOption{
		grpc.WithUnaryInterceptor(grpc_logrus.UnaryClientInterceptor(logrus.NewEntry(b.logger), opts...)),
		grpc.WithStreamInterceptor(grpc_logrus.StreamClientInterceptor(logrus.NewEntry(b.logger), opts...)),
	}
	suite.Run(t, &logrusClientSuite{b})
}

type logrusClientSuite struct {
	*logrusBaseSuite
}

func (s *logrusClientSuite) TestPing() {
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

func (s *logrusClientSuite) TestPingList() {
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

func (s *logrusClientSuite) TestPingError_WithCustomLevels() {
	for _, tcase := range []struct {
		code  codes.Code
		level logrus.Level
		msg   string
	}{
		{
			code:  codes.Internal,
			level: logrus.WarnLevel,
			msg:   "Internal must remap to ErrorLevel in DefaultClientCodeToLevel",
		},
		{
			code:  codes.NotFound,
			level: logrus.DebugLevel,
			msg:   "NotFound must remap to InfoLevel in DefaultClientCodeToLevel",
		},
		{
			code:  codes.FailedPrecondition,
			level: logrus.DebugLevel,
			msg:   "FailedPrecondition must remap to WarnLevel in DefaultClientCodeToLevel",
		},
		{
			code:  codes.Unauthenticated,
			level: logrus.ErrorLevel,
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

func TestLogrusClientOverrideSuite(t *testing.T) {
	if strings.HasPrefix(runtime.Version(), "go1.7") {
		t.Skip("Skipping due to json.RawMessage incompatibility with go1.7")
		return
	}
	opts := []grpc_logrus.Option{
		grpc_logrus.WithDurationField(grpc_logrus.DurationToDurationField),
	}
	b := newLogrusBaseSuite(t)
	b.logger.Level = logrus.DebugLevel // a lot of our stuff is on debug level by default
	b.InterceptorTestSuite.ClientOpts = []grpc.DialOption{
		grpc.WithUnaryInterceptor(grpc_logrus.UnaryClientInterceptor(logrus.NewEntry(b.logger), opts...)),
		grpc.WithStreamInterceptor(grpc_logrus.StreamClientInterceptor(logrus.NewEntry(b.logger), opts...)),
	}
	suite.Run(t, &logrusClientOverrideSuite{b})
}

type logrusClientOverrideSuite struct {
	*logrusBaseSuite
}

func (s *logrusClientOverrideSuite) TestPing_HasOverrides() {
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

func (s *logrusClientOverrideSuite) TestPingList_HasOverrides() {
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
