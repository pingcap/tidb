// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"github.com/pingcap/errors"
	"github.com/spf13/pflag"
)

// DefineFlags adds flags to the flag set corresponding to all backend options.
func DefineFlags(flags *pflag.FlagSet) {
	defineS3Flags(flags)
	defineGCSFlags(flags)
}

// ParseFromFlags obtains the backend options from the flag set.
func (options *BackendOptions) ParseFromFlags(flags *pflag.FlagSet) error {
	if err := options.S3.parseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	return options.GCS.parseFromFlags(flags)
}
