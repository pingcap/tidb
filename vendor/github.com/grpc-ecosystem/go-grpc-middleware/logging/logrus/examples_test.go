// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_logrus_test

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	pb_testproto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Initialization shows a relatively complex initialization sequence.
func Example_initialization(logrusLogger *logrus.Logger, customFunc grpc_logrus.CodeToLevel) *grpc.Server {
	// Logrus entry is used, allowing pre-definition of certain fields by the user.
	logrusEntry := logrus.NewEntry(logrusLogger)
	// Shared options for the logger, with a custom gRPC code to log level function.
	opts := []grpc_logrus.Option{
		grpc_logrus.WithLevels(customFunc),
	}
	// Make sure that log statements internal to gRPC library are logged using the zapLogger as well.
	grpc_logrus.ReplaceGrpcLogger(logrusEntry)
	// Create a server, make sure we put the grpc_ctxtags context before everything else.
	server := grpc.NewServer(
		grpc_middleware.WithUnaryServerChain(
			grpc_ctxtags.UnaryServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
			grpc_logrus.UnaryServerInterceptor(logrusEntry, opts...),
		),
		grpc_middleware.WithStreamServerChain(
			grpc_ctxtags.StreamServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
			grpc_logrus.StreamServerInterceptor(logrusEntry, opts...),
		),
	)
	return server
}

func Example_initializationWithDurationFieldOverride(logrusLogger *logrus.Logger) *grpc.Server {
	// Logrus entry is used, allowing pre-definition of certain fields by the user.
	logrusEntry := logrus.NewEntry(logrusLogger)
	// Shared options for the logger, with a custom duration to log field function.
	opts := []grpc_logrus.Option{
		grpc_logrus.WithDurationField(func(duration time.Duration) (key string, value interface{}) {
			return "grpc.time_ns", duration.Nanoseconds()
		}),
	}
	server := grpc.NewServer(
		grpc_middleware.WithUnaryServerChain(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_logrus.UnaryServerInterceptor(logrusEntry, opts...),
		),
		grpc_middleware.WithStreamServerChain(
			grpc_ctxtags.StreamServerInterceptor(),
			grpc_logrus.StreamServerInterceptor(logrusEntry, opts...),
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
		grpc_logrus.Extract(ctx).Info("some ping")
		return &pb_testproto.PingResponse{Value: ping.Value}, nil
	}
	return x
}
