// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_zap_test

import (
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	pb_testproto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func customCodeToLevel(c codes.Code) zapcore.Level {
	if c == codes.Unauthenticated {
		// Make this a special case for tests, and an error.
		return zap.DPanicLevel
	}
	level := grpc_zap.DefaultCodeToLevel(c)
	return level
}

func TestZapLoggingSuite(t *testing.T) {
	if strings.HasPrefix(runtime.Version(), "go1.7") {
		t.Skipf("Skipping due to json.RawMessage incompatibility with go1.7")
		return
	}
	opts := []grpc_zap.Option{
		grpc_zap.WithLevels(customCodeToLevel),
	}
	b := newBaseZapSuite(t)
	b.InterceptorTestSuite.ServerOpts = []grpc.ServerOption{
		grpc_middleware.WithStreamServerChain(
			grpc_ctxtags.StreamServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
			grpc_zap.StreamServerInterceptor(b.log, opts...)),
		grpc_middleware.WithUnaryServerChain(
			grpc_ctxtags.UnaryServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
			grpc_zap.UnaryServerInterceptor(b.log, opts...)),
	}
	suite.Run(t, &zapServerSuite{b})
}

type zapServerSuite struct {
	*zapBaseSuite
}

func (s *zapServerSuite) TestPing_WithCustomTags() {
	_, err := s.Client.Ping(s.SimpleCtx(), goodPing)
	assert.NoError(s.T(), err, "there must be not be an on a successful call")
	msgs := s.getOutputJSONs()
	assert.Len(s.T(), msgs, 2, "two log statements should be logged")
	for _, m := range msgs {
		s.T()
		assert.Contains(s.T(), m, `grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
		assert.Contains(s.T(), m, `grpc.method": "Ping"`, "all lines must contain method name")
		assert.Contains(s.T(), m, `"custom_tags.string": "something"`, "all lines must contain `custom_tags.string` set by AddFields")
		assert.Contains(s.T(), m, `"custom_tags.int": 1337`, "all lines must contain `custom_tags.int` set by AddFields")
		// request field extraction
		assert.Contains(s.T(), m, `"grpc.request.value": "something"`, "all lines must contain fields extracted from goodPing because of test.manual_extractfields.pb")
	}
	assert.Contains(s.T(), msgs[0], `"msg": "some ping"`, "handler's message must contain user message")
	assert.Contains(s.T(), msgs[1], `"msg": "finished unary call"`, "interceptor message must contain string")
	assert.Contains(s.T(), msgs[1], `"level": "info"`, "OK error codes must be logged on info level.")
	assert.Contains(s.T(), msgs[1], `grpc.time_ms":`, "interceptor log statement should contain execution time")
}

func (s *zapServerSuite) TestPingError_WithCustomLevels() {
	for _, tcase := range []struct {
		code  codes.Code
		level zapcore.Level
		msg   string
	}{
		{
			code:  codes.Internal,
			level: zap.ErrorLevel,
			msg:   "Internal must remap to ErrorLevel in DefaultCodeToLevel",
		},
		{
			code:  codes.NotFound,
			level: zap.InfoLevel,
			msg:   "NotFound must remap to InfoLevel in DefaultCodeToLevel",
		},
		{
			code:  codes.FailedPrecondition,
			level: zap.WarnLevel,
			msg:   "FailedPrecondition must remap to WarnLevel in DefaultCodeToLevel",
		},
		{
			code:  codes.Unauthenticated,
			level: zap.DPanicLevel,
			msg:   "Unauthenticated is overwritten to DPanicLevel with customCodeToLevel override, which probably didn't work",
		},
	} {
		s.buffer.Reset()
		_, err := s.Client.PingError(
			s.SimpleCtx(),
			&pb_testproto.PingRequest{Value: "something", ErrorCodeReturned: uint32(tcase.code)})
		assert.Error(s.T(), err, "each call here must return an error")
		msgs := s.getOutputJSONs()
		require.Len(s.T(), msgs, 1, "only the interceptor log message is printed in PingErr")
		m := msgs[0]
		assert.Contains(s.T(), m, `grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
		assert.Contains(s.T(), m, `grpc.method": "PingError"`, "all lines must contain method name")
		assert.Contains(s.T(), m, fmt.Sprintf(`grpc.code": "%s"`, tcase.code.String()), "all lines must contain method name")
		assert.Contains(s.T(), m, fmt.Sprintf(`"level": "%s"`, tcase.level.String()), tcase.msg)
	}
}

func (s *zapServerSuite) TestPingList_WithCustomTags() {
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
	assert.Len(s.T(), msgs, 2, "two log statements should be logged")
	for _, m := range msgs {
		s.T()
		assert.Contains(s.T(), m, `grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
		assert.Contains(s.T(), m, `grpc.method": "PingList"`, "all lines must contain method name")
		assert.Contains(s.T(), m, `"custom_tags.string": "something"`, "all lines must contain `custom_tags.string` set by AddFields")
		assert.Contains(s.T(), m, `"custom_tags.int": 1337`, "all lines must contain `custom_tags.int` set by AddFields")
		// request field extraction
		assert.Contains(s.T(), m, `"grpc.request.value": "something"`, "all lines must contain fields extracted from goodPing because of test.manual_extractfields.pb")
	}
	assert.Contains(s.T(), msgs[0], `"msg": "some pinglist"`, "handler's message must contain user message")
	assert.Contains(s.T(), msgs[1], `"msg": "finished streaming call"`, "interceptor message must contain string")
	assert.Contains(s.T(), msgs[1], `"level": "info"`, "OK error codes must be logged on info level.")
	assert.Contains(s.T(), msgs[1], `grpc.time_ms":`, "interceptor log statement should contain execution time")
}

func TestZapLoggingOverrideSuite(t *testing.T) {
	if strings.HasPrefix(runtime.Version(), "go1.7") {
		t.Skip("Skipping due to json.RawMessage incompatibility with go1.7")
		return
	}
	opts := []grpc_zap.Option{
		grpc_zap.WithDurationField(grpc_zap.DurationToDurationField),
	}
	b := newBaseZapSuite(t)
	b.InterceptorTestSuite.ServerOpts = []grpc.ServerOption{
		grpc_middleware.WithStreamServerChain(
			grpc_ctxtags.StreamServerInterceptor(),
			grpc_zap.StreamServerInterceptor(b.log, opts...)),
		grpc_middleware.WithUnaryServerChain(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_zap.UnaryServerInterceptor(b.log, opts...)),
	}
	suite.Run(t, &zapServerOverrideSuite{b})
}

type zapServerOverrideSuite struct {
	*zapBaseSuite
}

func (s *zapServerOverrideSuite) TestPing_HasOverriddenDuration() {
	_, err := s.Client.Ping(s.SimpleCtx(), goodPing)
	assert.NoError(s.T(), err, "there must be not be an error on a successful call")
	msgs := s.getOutputJSONs()
	assert.Len(s.T(), msgs, 2, "two log statements should be logged")
	for _, m := range msgs {
		s.T()
		assert.Contains(s.T(), m, `grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
		assert.Contains(s.T(), m, `grpc.method": "Ping"`, "all lines must contain method name")
	}
	assert.Contains(s.T(), msgs[0], `"msg": "some ping"`, "handler's message must contain user message")
	assert.NotContains(s.T(), msgs[0], "grpc.time_ms", "handler's message must not contain default duration")
	assert.NotContains(s.T(), msgs[0], "grpc.duration", "handler's message must not contain overridden duration")
	assert.Contains(s.T(), msgs[1], `"msg": "finished unary call"`, "interceptor message must contain string")
	assert.Contains(s.T(), msgs[1], `"level": "info"`, "OK error codes must be logged on info level.")
	assert.NotContains(s.T(), msgs[1], "grpc.time_ms", "interceptor message must not contain default duration")
	assert.Contains(s.T(), msgs[1], "grpc.duration", "interceptor message must contain overridden duration")
}

func (s *zapServerOverrideSuite) TestPingList_HasOverriddenDuration() {
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
	assert.Len(s.T(), msgs, 2, "two log statements should be logged")
	for _, m := range msgs {
		s.T()
		assert.Contains(s.T(), m, `grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
		assert.Contains(s.T(), m, `grpc.method": "PingList"`, "all lines must contain method name")
	}
	assert.Contains(s.T(), msgs[0], `"msg": "some pinglist"`, "handler's message must contain user message")
	assert.NotContains(s.T(), msgs[0], "grpc.time_ms", "handler's message must not contain default duration")
	assert.NotContains(s.T(), msgs[0], "grpc.duration", "handler's message must not contain overridden duration")
	assert.Contains(s.T(), msgs[1], `"msg": "finished streaming call"`, "interceptor message must contain string")
	assert.Contains(s.T(), msgs[1], `"level": "info"`, "OK error codes must be logged on info level.")
	assert.NotContains(s.T(), msgs[1], "grpc.time_ms", "interceptor message must not contain default duration")
	assert.Contains(s.T(), msgs[1], "grpc.duration", "interceptor message must contain overridden duration")
}
