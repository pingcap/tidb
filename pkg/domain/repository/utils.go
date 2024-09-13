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

package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/slice"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/atomic"
)

var (
	retentionDays = atomic.NewInt32(int32(variable.DefTiDBWorkloadRepositoryRetentionDays))
)

func execRetry(ctx context.Context, exec sqlexec.SQLExecutor, sql string) error {
	var errs [5]error
	var succeed bool
	for i := 0; i < len(errs); i++ {
		_, err := exec.ExecuteInternal(ctx, sql)
		if err == nil {
			succeed = true
			break
		}
		errs[i] = err
	}
	if !succeed {
		return errors.Join(errs[:]...)
	}
	return nil
}

func generatePartitionDef(sb *strings.Builder, col string) {
	fmt.Fprintf(sb, "PARTITION BY RANGE( TO_DAYS(%s) ) (", col)
	// tbInfo is nil, retval must be false
	_ = generatePartitionRanges(sb, nil)
	fmt.Fprintf(sb, ")")
}

func generatePartitionRanges(sb *strings.Builder, tbInfo *model.TableInfo) bool {
	now := time.Now()
	newPtNum := 2
	// add new partitions per day
	// if all partitions to be added existed, do nothing
	allExisted := true
	for i := 0; i < newPtNum; i++ {
		// TODO: should we make this UTC? timezone issues
		newPtTime := now.AddDate(0, 0, i+1)
		newPtName := "p" + newPtTime.Format("20060102")
		if tbInfo != nil {
			ptInfos := tbInfo.GetPartitionInfo().Definitions
			if slice.AnyOf(ptInfos, func(i int) bool {
				return ptInfos[i].Name.L == newPtName
			}) {
				continue
			}
		}
		if !allExisted && i > 0 {
			fmt.Fprintf(sb, ",")
		}
		fmt.Fprintf(sb, "PARTITION %s VALUES LESS THAN (TO_DAYS('%s'))", newPtName, newPtTime.Format("2006-01-02"))
		allExisted = false
	}
	return allExisted
}

// SetRetentionDays will set retention days.
func SetRetentionDays(_ context.Context, d string) error {
	n, err := strconv.Atoi(d)
	if err != nil {
		return err
	}
	retentionDays.Store(int32(n))
	return nil
}

// ValidateDest will validate destination url, and normalize it.
func ValidateDest(orig string) (string, error) {
	// validate S3 URL, etc...
	return strings.ToLower(orig), nil
}
