// Copyright 2016 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_zap

import (
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/context"
)

type ctxMarker struct{}

var (
	ctxMarkerKey = &ctxMarker{}
	nullLogger   = zap.NewNop()
)

// Extract takes the call-scoped Logger from grpc_zap middleware.
//
// It always returns a Logger that has all the grpc_ctxtags updated.
func Extract(ctx context.Context) *zap.Logger {
	l, ok := ctx.Value(ctxMarkerKey).(*zap.Logger)
	if !ok {
		return nullLogger
	}
	// Add grpc_ctxtags tags metadata until now.
	return l.With(tagsFieldsToZapFields(ctx)...)
}

func tagsFieldsToZapFields(ctx context.Context) []zapcore.Field {
	fields := []zapcore.Field{}
	tags := grpc_ctxtags.Extract(ctx)
	for k, v := range tags.Values() {
		fields = append(fields, zap.Any(k, v))
	}
	return fields
}

func toContext(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, ctxMarkerKey, logger)
}
