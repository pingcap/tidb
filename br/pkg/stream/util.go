// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"strings"
	"time"
)

const DATE_FORMAT = "2006-01-02 15:04:05.999999999 -0700"

func FormatDate(ts time.Time) string {
	return ts.Format(DATE_FORMAT)
}

func IsMetaDBKey(key []byte) bool {
	return strings.HasPrefix(string(key), "mDB")
}

func IsMetaDDLJobHistoryKey(key []byte) bool {
	return strings.HasPrefix(string(key), "mDDLJobH")
}

func MaybeDBOrDDLJobHistoryKey(key []byte) bool {
	return strings.HasPrefix(string(key), "mD")
}
