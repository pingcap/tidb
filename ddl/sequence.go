// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"math"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	math2 "github.com/pingcap/tidb/util/math"
)

func onCreateSequence(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tbInfo := &model.TableInfo{}
	if err := job.DecodeArgs(tbInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tbInfo.State = model.StateNone
	err := checkTableNotExists(d, t, schemaID, tbInfo.Name.L)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch tbInfo.State {
	case model.StateNone:
		// none -> public
		tbInfo.State = model.StatePublic
		tbInfo.UpdateTS = t.StartTS
		err = createSequenceWithCheck(t, job, schemaID, tbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		asyncNotifyEvent(d, &util.Event{Tp: model.ActionCreateSequence, TableInfo: tbInfo})
		return ver, nil
	default:
		return ver, ErrInvalidDDLState.GenWithStackByArgs("sequence", tbInfo.State)
	}
}

func createSequenceWithCheck(t *meta.Meta, job *model.Job, schemaID int64, tbInfo *model.TableInfo) error {
	err := checkTableInfoValid(tbInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return errors.Trace(err)
	}
	var sequenceBase int64
	if tbInfo.Sequence.Increment >= 0 {
		sequenceBase = tbInfo.Sequence.Start - 1
	} else {
		sequenceBase = tbInfo.Sequence.Start + 1
	}
	return t.CreateSequenceAndSetSeqValue(schemaID, tbInfo, sequenceBase)
}

func handleSequenceOptions(SeqOptions []*ast.SequenceOption, sequenceInfo *model.SequenceInfo) {
	var (
		minSetFlag   bool
		maxSetFlag   bool
		startSetFlag bool
	)
	for _, op := range SeqOptions {
		switch op.Tp {
		case ast.SequenceOptionIncrementBy:
			sequenceInfo.Increment = op.IntValue
		case ast.SequenceStartWith:
			sequenceInfo.Start = op.IntValue
			startSetFlag = true
		case ast.SequenceMinValue:
			sequenceInfo.MinValue = op.IntValue
			minSetFlag = true
		case ast.SequenceMaxValue:
			sequenceInfo.MaxValue = op.IntValue
			maxSetFlag = true
		case ast.SequenceCache:
			sequenceInfo.CacheValue = op.IntValue
		case ast.SequenceNoCache:
			sequenceInfo.Cache = false
		case ast.SequenceCycle:
			sequenceInfo.Cycle = true
		case ast.SequenceNoCycle:
			sequenceInfo.Cycle = false
		}
	}
	// Fill the default value, min/max/start should be adjusted with the sign of sequenceInfo.Increment.
	if !(minSetFlag && maxSetFlag && startSetFlag) {
		if sequenceInfo.Increment >= 0 {
			if !minSetFlag {
				sequenceInfo.MinValue = model.DefaultPositiveSequenceMinValue
			}
			if !startSetFlag {
				sequenceInfo.Start = mathutil.MaxInt64(sequenceInfo.MinValue, model.DefaultPositiveSequenceStartValue)
			}
			if !maxSetFlag {
				sequenceInfo.MaxValue = model.DefaultPositiveSequenceMaxValue
			}
		} else {
			if !maxSetFlag {
				sequenceInfo.MaxValue = model.DefaultNegativeSequenceMaxValue
			}
			if !startSetFlag {
				sequenceInfo.Start = mathutil.MinInt64(sequenceInfo.MaxValue, model.DefaultNegativeSequenceStartValue)
			}
			if !minSetFlag {
				sequenceInfo.MinValue = model.DefaultNegativeSequenceMinValue
			}
		}
	}
}

func validateSequenceOptions(seqInfo *model.SequenceInfo) bool {
	// To ensure that cache * increment will never overflows.
	var maxIncrement int64
	if seqInfo.Increment == 0 {
		// Increment shouldn't be set as 0.
		return false
	}
	if seqInfo.CacheValue <= 0 {
		// Cache value should be bigger than 0.
		return false
	}
	maxIncrement = math2.Abs(seqInfo.Increment)

	return seqInfo.MaxValue >= seqInfo.Start &&
		seqInfo.MaxValue > seqInfo.MinValue &&
		seqInfo.Start >= seqInfo.MinValue &&
		seqInfo.MaxValue != math.MaxInt64 &&
		seqInfo.MinValue != math.MinInt64 &&
		seqInfo.CacheValue < (math.MaxInt64-maxIncrement)/maxIncrement
}

func buildSequenceInfo(stmt *ast.CreateSequenceStmt, ident ast.Ident) (*model.SequenceInfo, error) {
	sequenceInfo := &model.SequenceInfo{
		Cache:      model.DefaultSequenceCacheBool,
		Cycle:      model.DefaultSequenceCycleBool,
		CacheValue: model.DefaultSequenceCacheValue,
		Increment:  model.DefaultSequenceIncrementValue,
	}

	// Handle table comment options.
	for _, op := range stmt.TblOptions {
		switch op.Tp {
		case ast.TableOptionComment:
			sequenceInfo.Comment = op.StrValue
		case ast.TableOptionEngine:
			// TableOptionEngine will always be 'InnoDB', thus we do nothing in this branch to avoid error happening.
		default:
			return nil, ErrSequenceUnsupportedTableOption.GenWithStackByArgs(op.StrValue)
		}
	}
	handleSequenceOptions(stmt.SeqOptions, sequenceInfo)
	if !validateSequenceOptions(sequenceInfo) {
		return nil, ErrSequenceInvalidData.GenWithStackByArgs(ident.Schema.L, ident.Name.L)
	}
	return sequenceInfo, nil
}
