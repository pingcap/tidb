/*
 *
 * Copyright 2014, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package interop

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
)

var (
	reqSizes            = []int{27182, 8, 1828, 45904}
	respSizes           = []int{31415, 9, 2653, 58979}
	largeReqSize        = 271828
	largeRespSize       = 314159
	initialMetadataKey  = "x-grpc-test-echo-initial"
	trailingMetadataKey = "x-grpc-test-echo-trailing-bin"
)

// ClientNewPayload returns a payload of the given type and size.
func ClientNewPayload(t testpb.PayloadType, size int) *testpb.Payload {
	if size < 0 {
		grpclog.Fatalf("Requested a response with invalid length %d", size)
	}
	body := make([]byte, size)
	switch t {
	case testpb.PayloadType_COMPRESSABLE:
	case testpb.PayloadType_UNCOMPRESSABLE:
		grpclog.Fatalf("PayloadType UNCOMPRESSABLE is not supported")
	default:
		grpclog.Fatalf("Unsupported payload type: %d", t)
	}
	return &testpb.Payload{
		Type: t.Enum(),
		Body: body,
	}
}

// DoEmptyUnaryCall performs a unary RPC with empty request and response messages.
func DoEmptyUnaryCall(tc testpb.TestServiceClient, args ...grpc.CallOption) {
	reply, err := tc.EmptyCall(context.Background(), &testpb.Empty{}, args...)
	if err != nil {
		grpclog.Fatal("/TestService/EmptyCall RPC failed: ", err)
	}
	if !proto.Equal(&testpb.Empty{}, reply) {
		grpclog.Fatalf("/TestService/EmptyCall receives %v, want %v", reply, testpb.Empty{})
	}
}

// DoLargeUnaryCall performs a unary RPC with large payload in the request and response.
func DoLargeUnaryCall(tc testpb.TestServiceClient, args ...grpc.CallOption) {
	pl := ClientNewPayload(testpb.PayloadType_COMPRESSABLE, largeReqSize)
	req := &testpb.SimpleRequest{
		ResponseType: testpb.PayloadType_COMPRESSABLE.Enum(),
		ResponseSize: proto.Int32(int32(largeRespSize)),
		Payload:      pl,
	}
	reply, err := tc.UnaryCall(context.Background(), req, args...)
	if err != nil {
		grpclog.Fatal("/TestService/UnaryCall RPC failed: ", err)
	}
	t := reply.GetPayload().GetType()
	s := len(reply.GetPayload().GetBody())
	if t != testpb.PayloadType_COMPRESSABLE || s != largeRespSize {
		grpclog.Fatalf("Got the reply with type %d len %d; want %d, %d", t, s, testpb.PayloadType_COMPRESSABLE, largeRespSize)
	}
}

// DoClientStreaming performs a client streaming RPC.
func DoClientStreaming(tc testpb.TestServiceClient, args ...grpc.CallOption) {
	stream, err := tc.StreamingInputCall(context.Background(), args...)
	if err != nil {
		grpclog.Fatalf("%v.StreamingInputCall(_) = _, %v", tc, err)
	}
	var sum int
	for _, s := range reqSizes {
		pl := ClientNewPayload(testpb.PayloadType_COMPRESSABLE, s)
		req := &testpb.StreamingInputCallRequest{
			Payload: pl,
		}
		if err := stream.Send(req); err != nil {
			grpclog.Fatalf("%v has error %v while sending %v", stream, err, req)
		}
		sum += s
	}
	reply, err := stream.CloseAndRecv()
	if err != nil {
		grpclog.Fatalf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
	}
	if reply.GetAggregatedPayloadSize() != int32(sum) {
		grpclog.Fatalf("%v.CloseAndRecv().GetAggregatePayloadSize() = %v; want %v", stream, reply.GetAggregatedPayloadSize(), sum)
	}
}

// DoServerStreaming performs a server streaming RPC.
func DoServerStreaming(tc testpb.TestServiceClient, args ...grpc.CallOption) {
	respParam := make([]*testpb.ResponseParameters, len(respSizes))
	for i, s := range respSizes {
		respParam[i] = &testpb.ResponseParameters{
			Size: proto.Int32(int32(s)),
		}
	}
	req := &testpb.StreamingOutputCallRequest{
		ResponseType:       testpb.PayloadType_COMPRESSABLE.Enum(),
		ResponseParameters: respParam,
	}
	stream, err := tc.StreamingOutputCall(context.Background(), req, args...)
	if err != nil {
		grpclog.Fatalf("%v.StreamingOutputCall(_) = _, %v", tc, err)
	}
	var rpcStatus error
	var respCnt int
	var index int
	for {
		reply, err := stream.Recv()
		if err != nil {
			rpcStatus = err
			break
		}
		t := reply.GetPayload().GetType()
		if t != testpb.PayloadType_COMPRESSABLE {
			grpclog.Fatalf("Got the reply of type %d, want %d", t, testpb.PayloadType_COMPRESSABLE)
		}
		size := len(reply.GetPayload().GetBody())
		if size != int(respSizes[index]) {
			grpclog.Fatalf("Got reply body of length %d, want %d", size, respSizes[index])
		}
		index++
		respCnt++
	}
	if rpcStatus != io.EOF {
		grpclog.Fatalf("Failed to finish the server streaming rpc: %v", rpcStatus)
	}
	if respCnt != len(respSizes) {
		grpclog.Fatalf("Got %d reply, want %d", len(respSizes), respCnt)
	}
}

// DoPingPong performs ping-pong style bi-directional streaming RPC.
func DoPingPong(tc testpb.TestServiceClient, args ...grpc.CallOption) {
	stream, err := tc.FullDuplexCall(context.Background(), args...)
	if err != nil {
		grpclog.Fatalf("%v.FullDuplexCall(_) = _, %v", tc, err)
	}
	var index int
	for index < len(reqSizes) {
		respParam := []*testpb.ResponseParameters{
			{
				Size: proto.Int32(int32(respSizes[index])),
			},
		}
		pl := ClientNewPayload(testpb.PayloadType_COMPRESSABLE, reqSizes[index])
		req := &testpb.StreamingOutputCallRequest{
			ResponseType:       testpb.PayloadType_COMPRESSABLE.Enum(),
			ResponseParameters: respParam,
			Payload:            pl,
		}
		if err := stream.Send(req); err != nil {
			grpclog.Fatalf("%v has error %v while sending %v", stream, err, req)
		}
		reply, err := stream.Recv()
		if err != nil {
			grpclog.Fatalf("%v.Recv() = %v", stream, err)
		}
		t := reply.GetPayload().GetType()
		if t != testpb.PayloadType_COMPRESSABLE {
			grpclog.Fatalf("Got the reply of type %d, want %d", t, testpb.PayloadType_COMPRESSABLE)
		}
		size := len(reply.GetPayload().GetBody())
		if size != int(respSizes[index]) {
			grpclog.Fatalf("Got reply body of length %d, want %d", size, respSizes[index])
		}
		index++
	}
	if err := stream.CloseSend(); err != nil {
		grpclog.Fatalf("%v.CloseSend() got %v, want %v", stream, err, nil)
	}
	if _, err := stream.Recv(); err != io.EOF {
		grpclog.Fatalf("%v failed to complele the ping pong test: %v", stream, err)
	}
}

// DoEmptyStream sets up a bi-directional streaming with zero message.
func DoEmptyStream(tc testpb.TestServiceClient, args ...grpc.CallOption) {
	stream, err := tc.FullDuplexCall(context.Background(), args...)
	if err != nil {
		grpclog.Fatalf("%v.FullDuplexCall(_) = _, %v", tc, err)
	}
	if err := stream.CloseSend(); err != nil {
		grpclog.Fatalf("%v.CloseSend() got %v, want %v", stream, err, nil)
	}
	if _, err := stream.Recv(); err != io.EOF {
		grpclog.Fatalf("%v failed to complete the empty stream test: %v", stream, err)
	}
}

// DoTimeoutOnSleepingServer performs an RPC on a sleep server which causes RPC timeout.
func DoTimeoutOnSleepingServer(tc testpb.TestServiceClient, args ...grpc.CallOption) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	stream, err := tc.FullDuplexCall(ctx, args...)
	if err != nil {
		if grpc.Code(err) == codes.DeadlineExceeded {
			return
		}
		grpclog.Fatalf("%v.FullDuplexCall(_) = _, %v", tc, err)
	}
	pl := ClientNewPayload(testpb.PayloadType_COMPRESSABLE, 27182)
	req := &testpb.StreamingOutputCallRequest{
		ResponseType: testpb.PayloadType_COMPRESSABLE.Enum(),
		Payload:      pl,
	}
	if err := stream.Send(req); err != nil {
		if grpc.Code(err) != codes.DeadlineExceeded {
			grpclog.Fatalf("%v.Send(_) = %v", stream, err)
		}
	}
	if _, err := stream.Recv(); grpc.Code(err) != codes.DeadlineExceeded {
		grpclog.Fatalf("%v.Recv() = _, %v, want error code %d", stream, err, codes.DeadlineExceeded)
	}
}

// DoComputeEngineCreds performs a unary RPC with compute engine auth.
func DoComputeEngineCreds(tc testpb.TestServiceClient, serviceAccount, oauthScope string) {
	pl := ClientNewPayload(testpb.PayloadType_COMPRESSABLE, largeReqSize)
	req := &testpb.SimpleRequest{
		ResponseType:   testpb.PayloadType_COMPRESSABLE.Enum(),
		ResponseSize:   proto.Int32(int32(largeRespSize)),
		Payload:        pl,
		FillUsername:   proto.Bool(true),
		FillOauthScope: proto.Bool(true),
	}
	reply, err := tc.UnaryCall(context.Background(), req)
	if err != nil {
		grpclog.Fatal("/TestService/UnaryCall RPC failed: ", err)
	}
	user := reply.GetUsername()
	scope := reply.GetOauthScope()
	if user != serviceAccount {
		grpclog.Fatalf("Got user name %q, want %q.", user, serviceAccount)
	}
	if !strings.Contains(oauthScope, scope) {
		grpclog.Fatalf("Got OAuth scope %q which is NOT a substring of %q.", scope, oauthScope)
	}
}

func getServiceAccountJSONKey(keyFile string) []byte {
	jsonKey, err := ioutil.ReadFile(keyFile)
	if err != nil {
		grpclog.Fatalf("Failed to read the service account key file: %v", err)
	}
	return jsonKey
}

// DoServiceAccountCreds performs a unary RPC with service account auth.
func DoServiceAccountCreds(tc testpb.TestServiceClient, serviceAccountKeyFile, oauthScope string) {
	pl := ClientNewPayload(testpb.PayloadType_COMPRESSABLE, largeReqSize)
	req := &testpb.SimpleRequest{
		ResponseType:   testpb.PayloadType_COMPRESSABLE.Enum(),
		ResponseSize:   proto.Int32(int32(largeRespSize)),
		Payload:        pl,
		FillUsername:   proto.Bool(true),
		FillOauthScope: proto.Bool(true),
	}
	reply, err := tc.UnaryCall(context.Background(), req)
	if err != nil {
		grpclog.Fatal("/TestService/UnaryCall RPC failed: ", err)
	}
	jsonKey := getServiceAccountJSONKey(serviceAccountKeyFile)
	user := reply.GetUsername()
	scope := reply.GetOauthScope()
	if !strings.Contains(string(jsonKey), user) {
		grpclog.Fatalf("Got user name %q which is NOT a substring of %q.", user, jsonKey)
	}
	if !strings.Contains(oauthScope, scope) {
		grpclog.Fatalf("Got OAuth scope %q which is NOT a substring of %q.", scope, oauthScope)
	}
}

// DoJWTTokenCreds performs a unary RPC with JWT token auth.
func DoJWTTokenCreds(tc testpb.TestServiceClient, serviceAccountKeyFile string) {
	pl := ClientNewPayload(testpb.PayloadType_COMPRESSABLE, largeReqSize)
	req := &testpb.SimpleRequest{
		ResponseType: testpb.PayloadType_COMPRESSABLE.Enum(),
		ResponseSize: proto.Int32(int32(largeRespSize)),
		Payload:      pl,
		FillUsername: proto.Bool(true),
	}
	reply, err := tc.UnaryCall(context.Background(), req)
	if err != nil {
		grpclog.Fatal("/TestService/UnaryCall RPC failed: ", err)
	}
	jsonKey := getServiceAccountJSONKey(serviceAccountKeyFile)
	user := reply.GetUsername()
	if !strings.Contains(string(jsonKey), user) {
		grpclog.Fatalf("Got user name %q which is NOT a substring of %q.", user, jsonKey)
	}
}

// GetToken obtains an OAUTH token from the input.
func GetToken(serviceAccountKeyFile string, oauthScope string) *oauth2.Token {
	jsonKey := getServiceAccountJSONKey(serviceAccountKeyFile)
	config, err := google.JWTConfigFromJSON(jsonKey, oauthScope)
	if err != nil {
		grpclog.Fatalf("Failed to get the config: %v", err)
	}
	token, err := config.TokenSource(context.Background()).Token()
	if err != nil {
		grpclog.Fatalf("Failed to get the token: %v", err)
	}
	return token
}

// DoOauth2TokenCreds performs a unary RPC with OAUTH2 token auth.
func DoOauth2TokenCreds(tc testpb.TestServiceClient, serviceAccountKeyFile, oauthScope string) {
	pl := ClientNewPayload(testpb.PayloadType_COMPRESSABLE, largeReqSize)
	req := &testpb.SimpleRequest{
		ResponseType:   testpb.PayloadType_COMPRESSABLE.Enum(),
		ResponseSize:   proto.Int32(int32(largeRespSize)),
		Payload:        pl,
		FillUsername:   proto.Bool(true),
		FillOauthScope: proto.Bool(true),
	}
	reply, err := tc.UnaryCall(context.Background(), req)
	if err != nil {
		grpclog.Fatal("/TestService/UnaryCall RPC failed: ", err)
	}
	jsonKey := getServiceAccountJSONKey(serviceAccountKeyFile)
	user := reply.GetUsername()
	scope := reply.GetOauthScope()
	if !strings.Contains(string(jsonKey), user) {
		grpclog.Fatalf("Got user name %q which is NOT a substring of %q.", user, jsonKey)
	}
	if !strings.Contains(oauthScope, scope) {
		grpclog.Fatalf("Got OAuth scope %q which is NOT a substring of %q.", scope, oauthScope)
	}
}

// DoPerRPCCreds performs a unary RPC with per RPC OAUTH2 token.
func DoPerRPCCreds(tc testpb.TestServiceClient, serviceAccountKeyFile, oauthScope string) {
	jsonKey := getServiceAccountJSONKey(serviceAccountKeyFile)
	pl := ClientNewPayload(testpb.PayloadType_COMPRESSABLE, largeReqSize)
	req := &testpb.SimpleRequest{
		ResponseType:   testpb.PayloadType_COMPRESSABLE.Enum(),
		ResponseSize:   proto.Int32(int32(largeRespSize)),
		Payload:        pl,
		FillUsername:   proto.Bool(true),
		FillOauthScope: proto.Bool(true),
	}
	token := GetToken(serviceAccountKeyFile, oauthScope)
	kv := map[string]string{"authorization": token.TokenType + " " + token.AccessToken}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.MD{"authorization": []string{kv["authorization"]}})
	reply, err := tc.UnaryCall(ctx, req)
	if err != nil {
		grpclog.Fatal("/TestService/UnaryCall RPC failed: ", err)
	}
	user := reply.GetUsername()
	scope := reply.GetOauthScope()
	if !strings.Contains(string(jsonKey), user) {
		grpclog.Fatalf("Got user name %q which is NOT a substring of %q.", user, jsonKey)
	}
	if !strings.Contains(oauthScope, scope) {
		grpclog.Fatalf("Got OAuth scope %q which is NOT a substring of %q.", scope, oauthScope)
	}
}

var (
	testMetadata = metadata.MD{
		"key1": []string{"value1"},
		"key2": []string{"value2"},
	}
)

// DoCancelAfterBegin cancels the RPC after metadata has been sent but before payloads are sent.
func DoCancelAfterBegin(tc testpb.TestServiceClient, args ...grpc.CallOption) {
	ctx, cancel := context.WithCancel(metadata.NewOutgoingContext(context.Background(), testMetadata))
	stream, err := tc.StreamingInputCall(ctx, args...)
	if err != nil {
		grpclog.Fatalf("%v.StreamingInputCall(_) = _, %v", tc, err)
	}
	cancel()
	_, err = stream.CloseAndRecv()
	if grpc.Code(err) != codes.Canceled {
		grpclog.Fatalf("%v.CloseAndRecv() got error code %d, want %d", stream, grpc.Code(err), codes.Canceled)
	}
}

// DoCancelAfterFirstResponse cancels the RPC after receiving the first message from the server.
func DoCancelAfterFirstResponse(tc testpb.TestServiceClient, args ...grpc.CallOption) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := tc.FullDuplexCall(ctx, args...)
	if err != nil {
		grpclog.Fatalf("%v.FullDuplexCall(_) = _, %v", tc, err)
	}
	respParam := []*testpb.ResponseParameters{
		{
			Size: proto.Int32(31415),
		},
	}
	pl := ClientNewPayload(testpb.PayloadType_COMPRESSABLE, 27182)
	req := &testpb.StreamingOutputCallRequest{
		ResponseType:       testpb.PayloadType_COMPRESSABLE.Enum(),
		ResponseParameters: respParam,
		Payload:            pl,
	}
	if err := stream.Send(req); err != nil {
		grpclog.Fatalf("%v has error %v while sending %v", stream, err, req)
	}
	if _, err := stream.Recv(); err != nil {
		grpclog.Fatalf("%v.Recv() = %v", stream, err)
	}
	cancel()
	if _, err := stream.Recv(); grpc.Code(err) != codes.Canceled {
		grpclog.Fatalf("%v compleled with error code %d, want %d", stream, grpc.Code(err), codes.Canceled)
	}
}

var (
	initialMetadataValue  = "test_initial_metadata_value"
	trailingMetadataValue = "\x0a\x0b\x0a\x0b\x0a\x0b"
	customMetadata        = metadata.Pairs(
		initialMetadataKey, initialMetadataValue,
		trailingMetadataKey, trailingMetadataValue,
	)
)

func validateMetadata(header, trailer metadata.MD) {
	if len(header[initialMetadataKey]) != 1 {
		grpclog.Fatalf("Expected exactly one header from server. Received %d", len(header[initialMetadataKey]))
	}
	if header[initialMetadataKey][0] != initialMetadataValue {
		grpclog.Fatalf("Got header %s; want %s", header[initialMetadataKey][0], initialMetadataValue)
	}
	if len(trailer[trailingMetadataKey]) != 1 {
		grpclog.Fatalf("Expected exactly one trailer from server. Received %d", len(trailer[trailingMetadataKey]))
	}
	if trailer[trailingMetadataKey][0] != trailingMetadataValue {
		grpclog.Fatalf("Got trailer %s; want %s", trailer[trailingMetadataKey][0], trailingMetadataValue)
	}
}

// DoCustomMetadata checks that metadata is echoed back to the client.
func DoCustomMetadata(tc testpb.TestServiceClient, args ...grpc.CallOption) {
	// Testing with UnaryCall.
	pl := ClientNewPayload(testpb.PayloadType_COMPRESSABLE, 1)
	req := &testpb.SimpleRequest{
		ResponseType: testpb.PayloadType_COMPRESSABLE.Enum(),
		ResponseSize: proto.Int32(int32(1)),
		Payload:      pl,
	}
	ctx := metadata.NewOutgoingContext(context.Background(), customMetadata)
	var header, trailer metadata.MD
	args = append(args, grpc.Header(&header), grpc.Trailer(&trailer))
	reply, err := tc.UnaryCall(
		ctx,
		req,
		args...,
	)
	if err != nil {
		grpclog.Fatal("/TestService/UnaryCall RPC failed: ", err)
	}
	t := reply.GetPayload().GetType()
	s := len(reply.GetPayload().GetBody())
	if t != testpb.PayloadType_COMPRESSABLE || s != 1 {
		grpclog.Fatalf("Got the reply with type %d len %d; want %d, %d", t, s, testpb.PayloadType_COMPRESSABLE, 1)
	}
	validateMetadata(header, trailer)

	// Testing with FullDuplex.
	stream, err := tc.FullDuplexCall(ctx, args...)
	if err != nil {
		grpclog.Fatalf("%v.FullDuplexCall(_) = _, %v, want <nil>", tc, err)
	}
	respParam := []*testpb.ResponseParameters{
		{
			Size: proto.Int32(1),
		},
	}
	streamReq := &testpb.StreamingOutputCallRequest{
		ResponseType:       testpb.PayloadType_COMPRESSABLE.Enum(),
		ResponseParameters: respParam,
		Payload:            pl,
	}
	if err := stream.Send(streamReq); err != nil {
		grpclog.Fatalf("%v has error %v while sending %v", stream, err, streamReq)
	}
	streamHeader, err := stream.Header()
	if err != nil {
		grpclog.Fatalf("%v.Header() = %v", stream, err)
	}
	if _, err := stream.Recv(); err != nil {
		grpclog.Fatalf("%v.Recv() = %v", stream, err)
	}
	if err := stream.CloseSend(); err != nil {
		grpclog.Fatalf("%v.CloseSend() = %v, want <nil>", stream, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		grpclog.Fatalf("%v failed to complete the custom metadata test: %v", stream, err)
	}
	streamTrailer := stream.Trailer()
	validateMetadata(streamHeader, streamTrailer)
}

// DoStatusCodeAndMessage checks that the status code is propagated back to the client.
func DoStatusCodeAndMessage(tc testpb.TestServiceClient, args ...grpc.CallOption) {
	var code int32 = 2
	msg := "test status message"
	expectedErr := grpc.Errorf(codes.Code(code), msg)
	respStatus := &testpb.EchoStatus{
		Code:    proto.Int32(code),
		Message: proto.String(msg),
	}
	// Test UnaryCall.
	req := &testpb.SimpleRequest{
		ResponseStatus: respStatus,
	}
	if _, err := tc.UnaryCall(context.Background(), req, args...); err == nil || err.Error() != expectedErr.Error() {
		grpclog.Fatalf("%v.UnaryCall(_, %v) = _, %v, want _, %v", tc, req, err, expectedErr)
	}
	// Test FullDuplexCall.
	stream, err := tc.FullDuplexCall(context.Background(), args...)
	if err != nil {
		grpclog.Fatalf("%v.FullDuplexCall(_) = _, %v, want <nil>", tc, err)
	}
	streamReq := &testpb.StreamingOutputCallRequest{
		ResponseStatus: respStatus,
	}
	if err := stream.Send(streamReq); err != nil {
		grpclog.Fatalf("%v has error %v while sending %v, want <nil>", stream, err, streamReq)
	}
	if err := stream.CloseSend(); err != nil {
		grpclog.Fatalf("%v.CloseSend() = %v, want <nil>", stream, err)
	}
	if _, err = stream.Recv(); err.Error() != expectedErr.Error() {
		grpclog.Fatalf("%v.Recv() returned error %v, want %v", stream, err, expectedErr)
	}
}

// DoUnimplementedService attempts to call a method from an unimplemented service.
func DoUnimplementedService(tc testpb.UnimplementedServiceClient) {
	_, err := tc.UnimplementedCall(context.Background(), &testpb.Empty{})
	if grpc.Code(err) != codes.Unimplemented {
		grpclog.Fatalf("%v.UnimplementedCall() = _, %v, want _, %v", tc, grpc.Code(err), codes.Unimplemented)
	}
}

// DoUnimplementedMethod attempts to call an unimplemented method.
func DoUnimplementedMethod(cc *grpc.ClientConn) {
	var req, reply proto.Message
	if err := grpc.Invoke(context.Background(), "/grpc.testing.TestService/UnimplementedCall", req, reply, cc); err == nil || grpc.Code(err) != codes.Unimplemented {
		grpclog.Fatalf("grpc.Invoke(_, _, _, _, _) = %v, want error code %s", err, codes.Unimplemented)
	}
}

type testServer struct {
}

// NewTestServer creates a test server for test service.
func NewTestServer() testpb.TestServiceServer {
	return &testServer{}
}

func (s *testServer) EmptyCall(ctx context.Context, in *testpb.Empty) (*testpb.Empty, error) {
	return new(testpb.Empty), nil
}

func serverNewPayload(t testpb.PayloadType, size int32) (*testpb.Payload, error) {
	if size < 0 {
		return nil, fmt.Errorf("requested a response with invalid length %d", size)
	}
	body := make([]byte, size)
	switch t {
	case testpb.PayloadType_COMPRESSABLE:
	case testpb.PayloadType_UNCOMPRESSABLE:
		return nil, fmt.Errorf("payloadType UNCOMPRESSABLE is not supported")
	default:
		return nil, fmt.Errorf("unsupported payload type: %d", t)
	}
	return &testpb.Payload{
		Type: t.Enum(),
		Body: body,
	}, nil
}

func (s *testServer) UnaryCall(ctx context.Context, in *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	status := in.GetResponseStatus()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if initialMetadata, ok := md[initialMetadataKey]; ok {
			header := metadata.Pairs(initialMetadataKey, initialMetadata[0])
			grpc.SendHeader(ctx, header)
		}
		if trailingMetadata, ok := md[trailingMetadataKey]; ok {
			trailer := metadata.Pairs(trailingMetadataKey, trailingMetadata[0])
			grpc.SetTrailer(ctx, trailer)
		}
	}
	if status != nil && *status.Code != 0 {
		return nil, grpc.Errorf(codes.Code(*status.Code), *status.Message)
	}
	pl, err := serverNewPayload(in.GetResponseType(), in.GetResponseSize())
	if err != nil {
		return nil, err
	}
	return &testpb.SimpleResponse{
		Payload: pl,
	}, nil
}

func (s *testServer) StreamingOutputCall(args *testpb.StreamingOutputCallRequest, stream testpb.TestService_StreamingOutputCallServer) error {
	cs := args.GetResponseParameters()
	for _, c := range cs {
		if us := c.GetIntervalUs(); us > 0 {
			time.Sleep(time.Duration(us) * time.Microsecond)
		}
		pl, err := serverNewPayload(args.GetResponseType(), c.GetSize())
		if err != nil {
			return err
		}
		if err := stream.Send(&testpb.StreamingOutputCallResponse{
			Payload: pl,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *testServer) StreamingInputCall(stream testpb.TestService_StreamingInputCallServer) error {
	var sum int
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&testpb.StreamingInputCallResponse{
				AggregatedPayloadSize: proto.Int32(int32(sum)),
			})
		}
		if err != nil {
			return err
		}
		p := in.GetPayload().GetBody()
		sum += len(p)
	}
}

func (s *testServer) FullDuplexCall(stream testpb.TestService_FullDuplexCallServer) error {
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		if initialMetadata, ok := md[initialMetadataKey]; ok {
			header := metadata.Pairs(initialMetadataKey, initialMetadata[0])
			stream.SendHeader(header)
		}
		if trailingMetadata, ok := md[trailingMetadataKey]; ok {
			trailer := metadata.Pairs(trailingMetadataKey, trailingMetadata[0])
			stream.SetTrailer(trailer)
		}
	}
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			// read done.
			return nil
		}
		if err != nil {
			return err
		}
		status := in.GetResponseStatus()
		if status != nil && *status.Code != 0 {
			return grpc.Errorf(codes.Code(*status.Code), *status.Message)
		}
		cs := in.GetResponseParameters()
		for _, c := range cs {
			if us := c.GetIntervalUs(); us > 0 {
				time.Sleep(time.Duration(us) * time.Microsecond)
			}
			pl, err := serverNewPayload(in.GetResponseType(), c.GetSize())
			if err != nil {
				return err
			}
			if err := stream.Send(&testpb.StreamingOutputCallResponse{
				Payload: pl,
			}); err != nil {
				return err
			}
		}
	}
}

func (s *testServer) HalfDuplexCall(stream testpb.TestService_HalfDuplexCallServer) error {
	var msgBuf []*testpb.StreamingOutputCallRequest
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			// read done.
			break
		}
		if err != nil {
			return err
		}
		msgBuf = append(msgBuf, in)
	}
	for _, m := range msgBuf {
		cs := m.GetResponseParameters()
		for _, c := range cs {
			if us := c.GetIntervalUs(); us > 0 {
				time.Sleep(time.Duration(us) * time.Microsecond)
			}
			pl, err := serverNewPayload(m.GetResponseType(), c.GetSize())
			if err != nil {
				return err
			}
			if err := stream.Send(&testpb.StreamingOutputCallResponse{
				Payload: pl,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
