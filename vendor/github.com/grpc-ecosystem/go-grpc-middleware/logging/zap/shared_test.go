package grpc_zap_test

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/go-grpc-middleware/testing"
	pb_testproto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/context"
)

var (
	goodPing = &pb_testproto.PingRequest{Value: "something", SleepTimeMs: 9999}
)

type loggingPingService struct {
	pb_testproto.TestServiceServer
}

func (s *loggingPingService) Ping(ctx context.Context, ping *pb_testproto.PingRequest) (*pb_testproto.PingResponse, error) {
	grpc_ctxtags.Extract(ctx).Set("custom_tags.string", "something").Set("custom_tags.int", 1337)
	grpc_zap.Extract(ctx).Info("some ping")
	return s.TestServiceServer.Ping(ctx, ping)
}

func (s *loggingPingService) PingError(ctx context.Context, ping *pb_testproto.PingRequest) (*pb_testproto.Empty, error) {
	return s.TestServiceServer.PingError(ctx, ping)
}

func (s *loggingPingService) PingList(ping *pb_testproto.PingRequest, stream pb_testproto.TestService_PingListServer) error {
	grpc_ctxtags.Extract(stream.Context()).Set("custom_tags.string", "something").Set("custom_tags.int", 1337)
	grpc_zap.Extract(stream.Context()).Info("some pinglist")
	return s.TestServiceServer.PingList(ping, stream)
}

func (s *loggingPingService) PingEmpty(ctx context.Context, empty *pb_testproto.Empty) (*pb_testproto.PingResponse, error) {
	return s.TestServiceServer.PingEmpty(ctx, empty)
}

func newBaseZapSuite(t *testing.T) *zapBaseSuite {
	b := &bytes.Buffer{}
	muB := grpc_testing.NewMutexReadWriter(b)
	zap.NewDevelopmentConfig()
	jsonEncoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.EpochTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
	})
	core := zapcore.NewCore(jsonEncoder, zapcore.AddSync(muB), zap.LevelEnablerFunc(func(zapcore.Level) bool { return true }))
	log := zap.New(core)
	s := &zapBaseSuite{
		log:         log,
		buffer:      b,
		mutexBuffer: muB,
		InterceptorTestSuite: &grpc_testing.InterceptorTestSuite{
			TestService: &loggingPingService{&grpc_testing.TestPingService{T: t}},
		},
	}
	return s
}

type zapBaseSuite struct {
	*grpc_testing.InterceptorTestSuite
	mutexBuffer *grpc_testing.MutexReadWriter
	buffer      *bytes.Buffer
	log         *zap.Logger
}

func (s *zapBaseSuite) SetupTest() {
	s.mutexBuffer.Lock()
	s.buffer.Reset()
	s.mutexBuffer.Unlock()
}

func (s *zapBaseSuite) getOutputJSONs() []string {
	ret := []string{}
	dec := json.NewDecoder(s.mutexBuffer)
	for {
		var val map[string]json.RawMessage
		err := dec.Decode(&val)
		if err == io.EOF {
			break
		}
		if err != nil {
			s.T().Fatalf("failed decoding output from ZAP JSON: %v", err)
		}
		out, _ := json.MarshalIndent(val, "", "  ")
		ret = append(ret, string(out))
	}
	return ret
}
