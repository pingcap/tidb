// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

// Range record start and end key for localStoreDir.DB
// so we can write it to tikv in streaming
type Range struct {
	Start []byte
	End   []byte
}
