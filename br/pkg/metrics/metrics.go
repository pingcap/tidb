// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.
// InitMetrics is used to initialize metrics.

package metrics

func init() {
	initBackupMetrics()
	initRestoreMetrics()
}
