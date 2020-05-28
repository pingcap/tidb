package util

import "time"

type RecordKey struct{}

type Record struct {
	UpdateEvalExp    time.Duration
	UpdateNextSub    time.Duration
	UpdateStartWork  time.Duration
	UpdateWaitResp   int64
	UpdateFetchIndex int64
	UpdateFetchTable int64
}
