// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"strings"
	"time"

	"github.com/pingcap/tidb/br/pkg/restore/split"
	"google.golang.org/grpc/status"
)

func checkRegionEpoch(_new, _old *split.RegionInfo) bool {
	return _new.Region.GetId() == _old.Region.GetId() &&
		_new.Region.GetRegionEpoch().GetVersion() == _old.Region.GetRegionEpoch().GetVersion() &&
		_new.Region.GetRegionEpoch().GetConfVer() == _old.Region.GetRegionEpoch().GetConfVer()
}

// exponentialBackoffer trivially retry any errors it meets.
// It's useful when the caller has handled the errors but
// only want to a more semantic backoff implementation.
type exponentialBackoffer struct {
	attempt     int
	baseBackoff time.Duration
}

func (b *exponentialBackoffer) exponentialBackoff() time.Duration {
	bo := b.baseBackoff
	b.attempt--
	if b.attempt == 0 {
		return 0
	}
	b.baseBackoff *= 2
	return bo
}

func pdErrorCanRetry(err error) bool {
	// There are 3 type of reason that PD would reject a `scatter` request:
	// (1) region %d has no leader
	// (2) region %d is hot
	// (3) region %d is not fully replicated
	//
	// (2) shouldn't happen in a recently splitted region.
	// (1) and (3) might happen, and should be retried.
	grpcErr := status.Convert(err)
	if grpcErr == nil {
		return false
	}
	return strings.Contains(grpcErr.Message(), "is not fully replicated") ||
		strings.Contains(grpcErr.Message(), "has no leader")
}

// NextBackoff returns a duration to wait before retrying again.
func (b *exponentialBackoffer) NextBackoff(error) time.Duration {
	// trivially exponential back off, because we have handled the error at upper level.
	return b.exponentialBackoff()
}

// Attempt returns the remain attempt times
func (b *exponentialBackoffer) Attempt() int {
	return b.attempt
}
