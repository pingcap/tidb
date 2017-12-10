// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_logrus_test

import (
	"fmt"
	"runtime"
	"testing"

	"io/ioutil"
	"strings"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"io"

	"github.com/sirupsen/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	pb_testproto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

var (
	nullLogger = &logrus.Logger{
		Out:       ioutil.Discard,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.PanicLevel,
	}
)

func TestLogrusPayloadSuite(t *testing.T) {
	if strings.HasPrefix(runtime.Version(), "go1.7") {
		t.Skipf("Skipping due to json.RawMessage incompatibility with go1.7")
		return
	}
	alwaysLoggingDeciderServer := func(ctx context.Context, fullMethodName string, servingObject interface{}) bool { return true }
	alwaysLoggingDeciderClient := func(ctx context.Context, fullMethodName string) bool { return true }
	b := newLogrusBaseSuite(t)
	b.InterceptorTestSuite.ClientOpts = []grpc.DialOption{
		grpc.WithUnaryInterceptor(grpc_logrus.PayloadUnaryClientInterceptor(logrus.NewEntry(b.logger), alwaysLoggingDeciderClient)),
		grpc.WithStreamInterceptor(grpc_logrus.PayloadStreamClientInterceptor(logrus.NewEntry(b.logger), alwaysLoggingDeciderClient)),
	}
	b.InterceptorTestSuite.ServerOpts = []grpc.ServerOption{
		grpc_middleware.WithStreamServerChain(
			grpc_ctxtags.StreamServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
			grpc_logrus.StreamServerInterceptor(logrus.NewEntry(nullLogger)),
			grpc_logrus.PayloadStreamServerInterceptor(logrus.NewEntry(b.logger), alwaysLoggingDeciderServer)),
		grpc_middleware.WithUnaryServerChain(
			grpc_ctxtags.UnaryServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
			grpc_logrus.UnaryServerInterceptor(logrus.NewEntry(nullLogger)),
			grpc_logrus.PayloadUnaryServerInterceptor(logrus.NewEntry(b.logger), alwaysLoggingDeciderServer)),
	}
	suite.Run(t, &logrusPayloadSuite{b})
}

type logrusPayloadSuite struct {
	*logrusBaseSuite
}

func (s *logrusPayloadSuite) getServerAndClientMessages(expectedServer int, expectedClient int) (serverMsgs []string, clientMsgs []string) {
	msgs := s.getOutputJSONs()
	for _, m := range msgs {
		if strings.Contains(m, `"span.kind": "server"`) {
			serverMsgs = append(serverMsgs, m)
		} else if strings.Contains(m, `"span.kind": "client"`) {
			clientMsgs = append(clientMsgs, m)
		}
	}
	require.Len(s.T(), serverMsgs, expectedServer, "must match expected number of server log messages")
	require.Len(s.T(), clientMsgs, expectedClient, "must match expected number of client log messages")
	return serverMsgs, clientMsgs
}

func (s *logrusPayloadSuite) TestPing_LogsBothRequestAndResponse() {
	resp, err := s.Client.Ping(s.SimpleCtx(), goodPing)
	assert.NoError(s.T(), err, "there must be not be an on a successful call")
	serverMsgs, clientMsgs := s.getServerAndClientMessages(2, 2)
	for _, m := range append(serverMsgs, clientMsgs...) {
		assert.Contains(s.T(), m, `"grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
		assert.Contains(s.T(), m, `"grpc.method": "Ping"`, "all lines must contain method name")
		assert.Contains(s.T(), m, `"level": "info"`, "all payloads must be logged on info level")
	}
	serverReq, serverResp := serverMsgs[0], serverMsgs[1]
	clientReq, clientResp := clientMsgs[0], clientMsgs[1]
	assert.Contains(s.T(), clientReq, `"grpc.request.content": {`, "request payload must be logged in a structured way")
	assert.Contains(s.T(), clientReq, fmt.Sprintf(`"value": "%s"`, goodPing.Value))
	assert.Contains(s.T(), clientReq, fmt.Sprintf(`"sleepTimeMs": %d`, goodPing.SleepTimeMs))
	assert.Contains(s.T(), serverReq, `"grpc.request.content": {`, "request payload must be logged in a structured way")
	assert.Contains(s.T(), serverReq, fmt.Sprintf(`"value": "%s"`, goodPing.Value))
	assert.Contains(s.T(), serverReq, fmt.Sprintf(`"sleepTimeMs": %d`, goodPing.SleepTimeMs))
	assert.Contains(s.T(), clientResp, `"grpc.response.content": {`, "response payload must be logged in a structured way")
	assert.Contains(s.T(), clientResp, fmt.Sprintf(`"value": "%s"`, resp.Value))
	assert.Contains(s.T(), clientResp, fmt.Sprintf(`"counter": %d`, resp.Counter))
	assert.Contains(s.T(), serverResp, `"grpc.response.content": {`, "response payload must be logged in a structured way")
	assert.Contains(s.T(), serverResp, fmt.Sprintf(`"value": "%s"`, resp.Value))
	assert.Contains(s.T(), serverResp, fmt.Sprintf(`"counter": %d`, resp.Counter))
}

func (s *logrusPayloadSuite) TestPingError_LogsOnlyRequestsOnError() {
	_, err := s.Client.PingError(s.SimpleCtx(), &pb_testproto.PingRequest{Value: "something", ErrorCodeReturned: uint32(4)})
	require.Error(s.T(), err, "there must be not be an on a successful call")
	serverMsgs, clientMsgs := s.getServerAndClientMessages(1, 1)
	for _, m := range append(serverMsgs, clientMsgs...) {
		assert.Contains(s.T(), m, `"grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
		assert.Contains(s.T(), m, `"grpc.method": "PingError"`, "all lines must contain method name")
		assert.Contains(s.T(), m, `"level": "info"`, "all payloads must be logged on info level")
	}
	assert.Contains(s.T(), clientMsgs[0], `"grpc.request.content": {`, "request payload must be logged in a structured way")
	assert.Contains(s.T(), serverMsgs[0], `"grpc.request.content": {`, "request payload must be logged in a structured way")
}

func (s *logrusPayloadSuite) TestPingStream_LogsAllRequestsAndResponses() {
	messagesExpected := 20
	stream, err := s.Client.PingStream(s.SimpleCtx())
	require.NoError(s.T(), err, "no error on stream creation")
	for i := 0; i < messagesExpected; i++ {
		require.NoError(s.T(), stream.Send(goodPing), "sending must succeed")
	}
	require.NoError(s.T(), stream.CloseSend(), "no error on send stream")
	for {
		pong := &pb_testproto.PingResponse{}
		err := stream.RecvMsg(pong)
		if err == io.EOF {
			break
		}
		require.NoError(s.T(), err, "no error on receive")
	}
	serverMsgs, clientMsgs := s.getServerAndClientMessages(2*messagesExpected, 2*messagesExpected)
	for _, m := range append(serverMsgs, clientMsgs...) {
		assert.Contains(s.T(), m, `"grpc.service": "mwitkow.testproto.TestService"`, "all lines must contain service name")
		assert.Contains(s.T(), m, `"grpc.method": "PingStream"`, "all lines must contain method name")
		assert.Contains(s.T(), m, `"level": "info"`, "all payloads must be logged on info level")
		assert.Contains(s.T(), m, `.content": {`, "all messages must contain payloads")
	}
}
