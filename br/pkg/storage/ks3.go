// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"github.com/ks3sdklib/aws-sdk-go/service/s3/s3iface"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
)

// KS3Storage defines some standard operations for BR/Lightning on the S3 storage.
// It implements the `ExternalStorage` interface.
type KS3Storage struct {
	svc     s3iface.S3API
	options *backuppb.S3
}
