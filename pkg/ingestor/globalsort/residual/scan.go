// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package residual

import (
	"context"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const (
	prefixLimit       = 256
	prefixSampleLimit = 10
)

// Stats summarizes global-sort residual objects.
type Stats struct {
	SizeBytes             int64
	ObjectCount           int64
	SamplePrefixes        []string
	SamplePrefixesOmitted bool
}

// prefixSampler keeps a bounded sample of the smallest object prefixes seen
// during a scan. Keeping the sample sorted makes the diagnostic log
// independent of the order in which the object store lists objects.
type prefixSampler struct {
	prefixes []string
	omitted  bool
}

func (s *prefixSampler) add(prefix string) {
	index, found := slices.BinarySearch(s.prefixes, prefix)
	if found {
		return
	}

	if len(s.prefixes) == prefixSampleLimit {
		s.omitted = true
		if index == len(s.prefixes) {
			// the new prefix sorts after every retained prefix.
			return
		}
		// drop the largest retained prefix to make room for the new one.
		s.prefixes = s.prefixes[:len(s.prefixes)-1]
	}
	s.prefixes = slices.Insert(s.prefixes, index, prefix)
}

// Scan walks storage and returns global-sort residual object statistics.
func Scan(ctx context.Context, storage storeapi.Storage) (Stats, error) {
	var stats Stats
	var sampler prefixSampler
	err := storage.WalkDir(ctx, &storeapi.WalkOption{}, func(path string, size int64) error {
		stats.ObjectCount++
		sampler.add(prefix(path))
		if size < 0 {
			return nil
		}
		if stats.SizeBytes > math.MaxInt64-size {
			return errors.Errorf(
				"global sort residual size overflow: accumulated bytes %d, next object bytes %d",
				stats.SizeBytes,
				size,
			)
		}
		stats.SizeBytes += size
		return nil
	})
	if err != nil {
		return Stats{}, errors.Annotate(err, "scan global sort residual objects")
	}

	stats.SamplePrefixes = sampler.prefixes
	stats.SamplePrefixesOmitted = sampler.omitted
	return stats, nil
}

func prefix(path string) string {
	trimmedPath := strings.Trim(path, "/")
	if trimmedPath == "" {
		return "<empty>"
	}

	segments := strings.Split(trimmedPath, "/")
	firstSegment := segments[0]
	// intermediate data files are written under a partition prefix followed by
	// the task ID, see simplesst.randPartitionedPrefix, everything else is
	// written directly under the task ID.
	if len(segments) > 1 && simplesst.IsValidPartition([]byte(firstSegment)) {
		if taskID, err := strconv.ParseInt(segments[1], 10, 64); err == nil && taskID > 0 {
			return firstSegment + "/" + segments[1] + "/"
		}
	}

	if len(firstSegment) > prefixLimit {
		firstSegment = firstSegment[:prefixLimit] + "..."
	}
	return firstSegment + "/"
}
