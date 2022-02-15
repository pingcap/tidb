//go:build !linux && !darwin && !freebsd && !unix
// +build !linux,!darwin,!freebsd,!unix

// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import tidbutils "github.com/pingcap/tidb-tools/pkg/utils"

// StartDynamicPProfListener starts the listener that will enable pprof when received `startPProfSignal`
func StartDynamicPProfListener(tls *tidbutils.TLS) {
	// nothing to do on no posix signal supporting systems.
}
