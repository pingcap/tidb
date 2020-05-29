package util

import "time"

type RecordKey struct{}

type IndexOfIndexLookup struct{}

type FetchOfIndexLookup struct{}

type SendOfIndexLookup struct{}

type Record struct {
	UpdateEvalExp    time.Duration
	UpdateNextSub    time.Duration
	UpdateStartWork  time.Duration
	UpdateWaitResp   int64
	UpdateFetchIndex int64
	UpdateFetchTable int64

	StoreFeedback        int64
	ExtractHandles       int64
	IndexResultNext      int64
	IndexGetSelectResp   int64
	IndexResultChan      int64
	FetcherNext          int64
	FetcherRetCh         int64
	CopBuildRange        int64
	CopDispatchWaitToken int64
	CopDispatchChan      int64
	CopSendIndex         int64
	CopHandleOne         int64
	CopRegionError       int64
	CopLockError         int64
	CopRetCh             int64
}
