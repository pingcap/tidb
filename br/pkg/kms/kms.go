// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package kms

import "context"

// Provider is an interface for key management service providers
// implement encrypt data key in future if needed
type Provider interface {
	DecryptDataKey(ctx context.Context, dataKey []byte) ([]byte, error)
	Name() string
	Close()
}
