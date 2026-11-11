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

package ddl

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
)

type delRangeExecWrapperForTest struct {
	rewrite  map[int64]int64
	consumed int
	sql      string
	params   []any
	consume  func([]any) error
}

func (*delRangeExecWrapperForTest) UpdateTSOForJob() error { return nil }

func (w *delRangeExecWrapperForTest) PrepareParamsList(_ int) {
	w.params = nil
}

func (w *delRangeExecWrapperForTest) RewriteTableID(tableID int64) (int64, bool) {
	newID, ok := w.rewrite[tableID]
	return newID, ok
}

func (w *delRangeExecWrapperForTest) AppendParamsList(jobID, elemID int64, startKey, endKey string) {
	w.params = append(w.params, jobID, elemID, startKey, endKey)
}

func (w *delRangeExecWrapperForTest) ConsumeDeleteRange(_ context.Context, sql string) error {
	if w.consume != nil {
		if err := w.consume(w.params); err != nil {
			return err
		}
	}
	w.consumed++
	w.sql = sql
	return nil
}

func TestDoBatchDeleteTablesRangeSkipsRewrittenIDs(t *testing.T) {
	tests := []struct {
		name          string
		tableIDs      []int64
		rewrite       map[int64]int64
		expectedRows  int
		expectedComma bool
	}{
		{
			name:          "all IDs skipped",
			tableIDs:      []int64{1, 2},
			rewrite:       map[int64]int64{},
			expectedRows:  0,
			expectedComma: false,
		},
		{
			name:          "first ID kept",
			tableIDs:      []int64{1, 2},
			rewrite:       map[int64]int64{1: 11},
			expectedRows:  1,
			expectedComma: false,
		},
		{
			name:          "second ID kept",
			tableIDs:      []int64{1, 2},
			rewrite:       map[int64]int64{2: 22},
			expectedRows:  1,
			expectedComma: false,
		},
		{
			name:          "both IDs kept",
			tableIDs:      []int64{1, 2},
			rewrite:       map[int64]int64{1: 11, 2: 22},
			expectedRows:  2,
			expectedComma: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapper := &delRangeExecWrapperForTest{rewrite: tt.rewrite}
			err := doBatchDeleteTablesRange(context.Background(), wrapper, 100, tt.tableIDs, &elementIDAlloc{}, "test")
			require.NoError(t, err)
			require.Equal(t, bool(tt.expectedRows > 0), wrapper.consumed > 0)
			if tt.expectedRows == 0 {
				require.Empty(t, wrapper.params)
				return
			}
			require.Len(t, wrapper.params, tt.expectedRows*4)
			require.Equal(t, tt.expectedComma, strings.Contains(wrapper.sql, insertDeleteRangeSQLValue+","+insertDeleteRangeSQLValue))
			require.False(t, strings.HasSuffix(wrapper.sql, ","))
		})
	}
}

// TestColumnDeleteRangeMixedVersionRetry preserves legacy element IDs even when
// an owner changes after any committed batch in a multi-schema job.
func TestColumnDeleteRangeMixedVersionRetry(t *testing.T) {
	for _, version := range []model.JobVersion{model.JobVersion1, model.JobVersion2} {
		for _, tc := range []struct {
			name         string
			types        []model.ActionType
			partitionIDs []int64
		}{
			{"drop columns", []model.ActionType{model.ActionDropColumn, model.ActionDropColumn}, []int64{20, 21}},
			{"modify columns", []model.ActionType{model.ActionModifyColumn, model.ActionModifyColumn}, []int64{20, 21}},
			{"mixed subjobs", []model.ActionType{model.ActionDropColumn, model.ActionModifyColumn, model.ActionDropIndex}, []int64{20, 21}},
			{"nonpartitioned", []model.ActionType{model.ActionDropColumn, model.ActionModifyColumn}, nil},
		} {
			t.Run(fmt.Sprintf("v%d/%s", version, tc.name), func(t *testing.T) {
				job := &model.Job{
					ID: 100, TableID: 10, Version: version, Type: model.ActionMultiSchemaChange,
					State: model.JobStateDone, MultiSchemaInfo: &model.MultiSchemaInfo{},
				}
				var legacyRanges, extraRanges []tblIdxID
				for i, tp := range tc.types {
					indexID := int64(i + 1)
					proxy := &model.Job{Version: version, Type: tp}
					switch tp {
					case model.ActionDropColumn:
						proxy.FillArgs(&model.TableColumnArgs{
							Col: &model.ColumnInfo{}, IndexIDs: []int64{indexID}, PartitionIDs: tc.partitionIDs,
						})
					case model.ActionModifyColumn:
						proxy.FillFinishedArgs(&model.ModifyColumnArgs{IndexIDs: []int64{indexID}, PartitionIDs: tc.partitionIDs})
					case model.ActionDropIndex:
						proxy.FillFinishedArgs(&model.ModifyIndexArgs{
							OpType: model.OpDropIndex, PartitionIDs: tc.partitionIDs,
							IndexArgs: []*model.IndexArg{{IndexID: indexID}},
						})
					}
					_, err := proxy.Encode(true)
					require.NoError(t, err)
					job.MultiSchemaInfo.SubJobs = append(job.MultiSchemaInfo.SubJobs, &model.SubJob{
						Type: tp, State: model.JobStateDone, RawArgs: proxy.RawArgs,
					})
					physicalIDs := tc.partitionIDs
					if len(physicalIDs) == 0 {
						physicalIDs = []int64{job.TableID}
					}
					for _, pid := range physicalIDs {
						legacyRanges = append(legacyRanges, tblIdxID{physicalID: pid, indexID: indexID})
					}
					if len(tc.partitionIDs) > 0 && tp != model.ActionDropIndex {
						extraRanges = append(extraRanges, tblIdxID{physicalID: job.TableID, indexID: indexID})
					}
				}
				expectedRanges := append(legacyRanges[:len(legacyRanges):len(legacyRanges)], extraRanges...)
				for committed := 0; committed <= len(expectedRanges); committed++ {
					t.Run(fmt.Sprintf("committed_%d", committed), func(t *testing.T) {
						type rangeKeys struct{ start, end string }
						rows := make(map[int64]rangeKeys)
						ownerChanged := errors.New("injected owner change")
						newWrapper := func(failAfter int) *delRangeExecWrapperForTest {
							batches := 0
							return &delRangeExecWrapperForTest{
								rewrite: map[int64]int64{10: 10, 20: 20, 21: 21},
								consume: func(params []any) error {
									if batches == failAfter {
										return ownerChanged
									}
									batches++
									// Each call commits independently. Match INSERT IGNORE's
									// unique (job_id, element_id), even for conflicting keys.
									for j := 0; j < len(params); j += 4 {
										require.Equal(t, job.ID, params[j])
										id := params[j+1].(int64)
										if _, exists := rows[id]; !exists {
											rows[id] = rangeKeys{params[j+2].(string), params[j+3].(string)}
										}
									}
									return nil
								},
							}
						}
						err := AddDelRangeJobInternal(context.Background(), newWrapper(committed), job)
						if committed < len(expectedRanges) {
							require.ErrorIs(t, err, ownerChanged)
						} else {
							require.NoError(t, err)
						}
						// An older owner only schedules the original ranges and starts
						// its allocator from scratch. None may collide with new ranges.
						var oldAllocator elementIDAlloc
						for _, r := range legacyRanges {
							require.NoError(t, doBatchDeleteIndiceRange(context.Background(), newWrapper(-1),
								job.ID, r.physicalID, []int64{r.indexID}, &oldAllocator, "legacy owner retry"))
						}
						checkRanges := func(ranges []tblIdxID) {
							for j, r := range ranges {
								want := rangeKeys{
									hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(r.physicalID, r.indexID)),
									hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(r.physicalID, r.indexID+1)),
								}
								require.Equal(t, want, rows[int64(j+1)], "element ID %d", j+1)
							}
						}
						checkRanges(legacyRanges)
						// A new owner can subsequently add all logical-table ranges
						// and retry again without changing any existing assignment.
						for range 2 {
							require.NoError(t, AddDelRangeJobInternal(context.Background(), newWrapper(-1), job))
							checkRanges(expectedRanges)
							require.Len(t, rows, len(expectedRanges))
						}
					})
				}
			})
		}
	}
}
