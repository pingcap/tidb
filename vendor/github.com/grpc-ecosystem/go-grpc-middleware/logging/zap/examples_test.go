// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_zap_test

import (
	pb_testproto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"

	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

// Initialization shows a relatively complex initialization sequence.
func Example_initialization(zapLogger *zap.Logger, customFunc grpc_zap.CodeToLevel) *grpc.Server {
	// Shared options for the logger, with a custom gRPC code to log level function.
	opts := []grpc_zap.Option{
		grpc_zap.WithLevels(customFunc),
	}
	// Make sure that log statements internal to gRPC library are logged using the zapLogger as well.
	grpc_zap.ReplaceGrpcLogger(zapLogger)
	// Create a server, make sure we put the grpc_ctxtags context before everything else.
	server := grpc.NewServer(
		grpc_middleware.WithUnaryServerChain(
			grpc_ctxtags.UnaryServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
			grpc_zap.UnaryServerInterceptor(zapLogger, opts...),
		),
		grpc_middleware.WithStreamServerChain(
			grpc_ctxtags.StreamServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
			grpc_zap.StreamServerInterceptor(zapLogger, opts...),
		),
	)
	return server
}

// Initialization shows an initialization sequence with the duration field generation overridden.
func Example_initializationWithDurationFieldOverride(zapLogger *zap.Logger) *grpc.Server {
	opts := []grpc_zap.Option{
		grpc_zap.WithDurationField(func(duration time.Duration) zapcore.Field {
			return zap.Int64("grpc.time_ns", duration.Nanoseconds())
		}),
	}

	server := grpc.NewServer(
		grpc_middleware.WithUnaryServerChain(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_zap.UnaryServerInterceptor(zapLogger, opts...),
		),
		grpc_middleware.WithStreamServerChain(
			grpc_ctxtags.StreamServerInterceptor(),
			grpc_zap.StreamServerInterceptor(zapLogger, opts...),
		),
	)

	return server
}

// Simple unary handler that adds custom fields to the requests's context. These will be used for all log statements.
func Example_handlerUsageUnaryPing() interface{} {
	x := func(ctx context.Context, ping *pb_testproto.PingRequest) (*pb_testproto.PingResponse, error) {
		// Add fields the ctxtags of the request which will be added to all extracted loggers.
		grpc_ctxtags.Extract(ctx).Set("custom_tags.string", "something").Set("custom_tags.int", 1337)
		// Extract a request-scoped zap.Logger and log a message.
		grpc_zap.Extract(ctx).Info("some ping")
		return &pb_testproto.PingResponse{Value: ping.Value}, nil
	}
	return x
}
