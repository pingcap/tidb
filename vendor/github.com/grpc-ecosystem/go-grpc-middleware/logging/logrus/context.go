// Copyright 2016 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_logrus

import (
	"github.com/sirupsen/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"golang.org/x/net/context"
)

type ctxMarker struct{}

var (
	ctxMarkerKey = &ctxMarker{}
)

// Extract takes the call-scoped logrus.Entry from grpc_logrus middleware.
//
// If the grpc_logrus middleware wasn't used, a no-op `logrus.Entry` is returned. This makes it safe to
// use regardless.
func Extract(ctx context.Context) *logrus.Entry {
	l, ok := ctx.Value(ctxMarkerKey).(*logrus.Entry)
	if !ok {
		return logrus.NewEntry(nullLogger)
	}
	// Add grpc_ctxtags tags metadata until now.
	return l.WithFields(logrus.Fields(grpc_ctxtags.Extract(ctx).Values()))
}

func toContext(ctx context.Context, entry *logrus.Entry) context.Context {
	return context.WithValue(ctx, ctxMarkerKey, entry)

}
