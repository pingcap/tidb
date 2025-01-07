// Copyright 2024 PingCAP, Inc.
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

package workloadrepo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
)

func generatePartitionDef(sb *strings.Builder, col string, now time.Time) {
	fmt.Fprintf(sb, " PARTITION BY RANGE( TO_DAYS(%s) ) (", col)
	// tbInfo is nil, retval must be false
	_, _ = generatePartitionRanges(sb, nil, now)
	fmt.Fprintf(sb, ")")
}

func generatePartitionName(t time.Time) string {
	return "p" + t.Format("20060102")
}

func parsePartitionName(part string) (time.Time, error) {
	return time.ParseInLocation("p20060102", part, time.Local)
}

func generatePartitionRanges(sb *strings.Builder, tbInfo *model.TableInfo, now time.Time) (bool, error) {
	// Set lastPart to the latest partition found in table or the date for
	// yesterday's partition if none is found. Note: The partition named for
	// today's date holds yesterday's data.
	lastPart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	if tbInfo != nil {
		pi := tbInfo.GetPartitionInfo()
		if pi != nil && pi.Definitions != nil && len(pi.Definitions) > 0 {
			ptInfos := pi.Definitions
			partDate, err := parsePartitionName(ptInfos[len(ptInfos)-1].Name.L)
			if err != nil {
				return true, err
			}

			if partDate.After(lastPart) {
				lastPart = partDate
			}
		}
	}

	// Add partitions for today and tomorrow.
	allExisted := true
	for i := range 2 {
		newPartDate := time.Date(now.Year(), now.Month(), now.Day()+i+1, 0, 0, 0, 0, time.Local)
		if newPartDate.After(lastPart) {
			if !allExisted {
				fmt.Fprintf(sb, ", ")
			}
			newPartName := generatePartitionName(newPartDate)
			fmt.Fprintf(sb, "PARTITION %s VALUES LESS THAN (TO_DAYS('%s'))", newPartName, newPartDate.Format("2006-01-02"))
			allExisted = false
		}
	}

	return allExisted, nil
}

func (w *worker) setRetentionDays(_ context.Context, d string) error {
	n, err := strconv.Atoi(d)
	if err != nil {
		return err
	}
	w.Lock()
	defer w.Unlock()
	w.retentionDays = int32(n)
	return nil
}

func validateDest(orig string) (string, error) {
	// validate S3 URL, etc...
	orig = strings.ToLower(orig)
	if orig != "" && orig != "table" {
		return "", errors.New("invalid repository destination")
	}
	return orig, nil
}
