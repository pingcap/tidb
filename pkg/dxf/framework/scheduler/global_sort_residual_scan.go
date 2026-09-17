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

package scheduler

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
	globalSortResidualPrefixLimit       = 256
	globalSortResidualPrefixSampleLimit = 10
)

type globalSortResidualScan struct {
	sizeBytes             int64
	objectCount           int64
	samplePrefixes        []string
	samplePrefixesOmitted bool
}

type globalSortResidualPrefixSampler struct {
	prefixes []string
	omitted  bool
}

func (s *globalSortResidualPrefixSampler) add(prefix string) {
	index, found := slices.BinarySearch(s.prefixes, prefix)
	if found {
		return
	}

	if len(s.prefixes) == globalSortResidualPrefixSampleLimit {
		s.omitted = true
		if index == len(s.prefixes) {
			return
		}
		copy(s.prefixes[index+1:], s.prefixes[index:len(s.prefixes)-1])
		s.prefixes[index] = prefix
		return
	}

	s.prefixes = append(s.prefixes, "")
	copy(s.prefixes[index+1:], s.prefixes[index:])
	s.prefixes[index] = prefix
}

func scanGlobalSortResidual(ctx context.Context, storage storeapi.Storage) (globalSortResidualScan, error) {
	var scan globalSortResidualScan
	var sampler globalSortResidualPrefixSampler
	err := storage.WalkDir(ctx, &storeapi.WalkOption{}, func(path string, size int64) error {
		scan.objectCount++
		sampler.add(globalSortResidualPrefix(path))
		if size < 0 {
			return nil
		}
		if scan.sizeBytes > math.MaxInt64-size {
			return errors.Errorf(
				"global sort residual size overflow: accumulated bytes %d, next object bytes %d",
				scan.sizeBytes,
				size,
			)
		}
		scan.sizeBytes += size
		return nil
	})
	if err != nil {
		return globalSortResidualScan{}, errors.Annotate(err, "scan global sort residual objects")
	}

	scan.samplePrefixes = sampler.prefixes
	scan.samplePrefixesOmitted = sampler.omitted
	return scan, nil
}

func globalSortResidualPrefix(path string) string {
	trimmedPath := strings.Trim(path, "/")
	if trimmedPath == "" {
		return "<empty>"
	}

	segments := strings.Split(trimmedPath, "/")
	firstSegment := segments[0]
	if taskID, err := strconv.ParseInt(firstSegment, 10, 64); err == nil && taskID > 0 {
		return firstSegment + "/"
	}
	if simplesst.IsValidPartition([]byte(firstSegment)) && len(segments) > 1 {
		if taskID, err := strconv.ParseInt(segments[1], 10, 64); err == nil && taskID > 0 {
			return firstSegment + "/" + segments[1] + "/"
		}
	}

	if len(firstSegment) > globalSortResidualPrefixLimit {
		firstSegment = firstSegment[:globalSortResidualPrefixLimit] + "..."
	}
	return firstSegment + "/"
}
