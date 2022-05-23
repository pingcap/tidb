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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"math"
	"reflect"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/mathutil"
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

	ver, err = updateSchemaVersion(d, t, job)
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
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("sequence", tbInfo.State)
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

func handleSequenceOptions(seqOptions []*ast.SequenceOption, sequenceInfo *model.SequenceInfo) {
	var (
		minSetFlag   bool
		maxSetFlag   bool
		startSetFlag bool
	)
	for _, op := range seqOptions {
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
			sequenceInfo.Cache = true
		case ast.SequenceNoCache:
			sequenceInfo.CacheValue = 0
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
				sequenceInfo.Start = mathutil.Max(sequenceInfo.MinValue, model.DefaultPositiveSequenceStartValue)
			}
			if !maxSetFlag {
				sequenceInfo.MaxValue = model.DefaultPositiveSequenceMaxValue
			}
		} else {
			if !maxSetFlag {
				sequenceInfo.MaxValue = model.DefaultNegativeSequenceMaxValue
			}
			if !startSetFlag {
				sequenceInfo.Start = mathutil.Min(sequenceInfo.MaxValue, model.DefaultNegativeSequenceStartValue)
			}
			if !minSetFlag {
				sequenceInfo.MinValue = model.DefaultNegativeSequenceMinValue
			}
		}
	}
}

func validateSequenceOptions(seqInfo *model.SequenceInfo) bool {
	// To ensure that cache * increment will never overflow.
	var maxIncrement int64
	if seqInfo.Increment == 0 {
		// Increment shouldn't be set as 0.
		return false
	}
	if seqInfo.Cache && seqInfo.CacheValue <= 0 {
		// Cache value should be bigger than 0.
		return false
	}
	maxIncrement = mathutil.Abs(seqInfo.Increment)

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
			return nil, dbterror.ErrSequenceUnsupportedTableOption.GenWithStackByArgs(op.StrValue)
		}
	}
	handleSequenceOptions(stmt.SeqOptions, sequenceInfo)
	if !validateSequenceOptions(sequenceInfo) {
		return nil, dbterror.ErrSequenceInvalidData.GenWithStackByArgs(ident.Schema.L, ident.Name.L)
	}
	return sequenceInfo, nil
}

func alterSequenceOptions(sequenceOptions []*ast.SequenceOption, ident ast.Ident, oldSequence *model.SequenceInfo) (bool, int64, error) {
	var (
		restartFlag     bool
		restartWithFlag bool
		restartValue    int64
	)
	// Override the old sequence value with new option.
	for _, op := range sequenceOptions {
		switch op.Tp {
		case ast.SequenceOptionIncrementBy:
			oldSequence.Increment = op.IntValue
		case ast.SequenceStartWith:
			oldSequence.Start = op.IntValue
		case ast.SequenceMinValue:
			oldSequence.MinValue = op.IntValue
		case ast.SequenceMaxValue:
			oldSequence.MaxValue = op.IntValue
		case ast.SequenceCache:
			oldSequence.CacheValue = op.IntValue
			oldSequence.Cache = true
		case ast.SequenceNoCache:
			oldSequence.CacheValue = 0
			oldSequence.Cache = false
		case ast.SequenceCycle:
			oldSequence.Cycle = true
		case ast.SequenceNoCycle:
			oldSequence.Cycle = false
		case ast.SequenceRestart:
			restartFlag = true
		case ast.SequenceRestartWith:
			restartWithFlag = true
			restartValue = op.IntValue
		}
	}
	if !validateSequenceOptions(oldSequence) {
		return false, 0, dbterror.ErrSequenceInvalidData.GenWithStackByArgs(ident.Schema.L, ident.Name.L)
	}
	if restartWithFlag {
		return true, restartValue, nil
	}
	if restartFlag {
		return true, oldSequence.Start, nil
	}
	return false, 0, nil
}

func onAlterSequence(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	var (
		sequenceOpts []*ast.SequenceOption
		ident        ast.Ident
	)
	if err := job.DecodeArgs(&ident, &sequenceOpts); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	// Get the old tableInfo.
	tblInfo, err := checkTableExistAndCancelNonExistJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// Substitute the sequence info.
	copySequenceInfo := *tblInfo.Sequence
	restart, restartValue, err := alterSequenceOptions(sequenceOpts, ident, &copySequenceInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	shouldUpdateVer := !reflect.DeepEqual(*tblInfo.Sequence, copySequenceInfo) || restart
	tblInfo.Sequence = &copySequenceInfo

	// Restart the sequence value.
	// Notice: during the alter sequence process, if there is some dml continually consumes sequence (nextval/setval),
	// the below cases will occur:
	// Since the table schema haven't been refreshed in local/other node, dml will still use old definition of sequence
	// to allocate sequence ids. Once the restart value is updated to kv here, the allocated ids in the upper layer won't
	// guarantee to be consecutive and monotonous.
	if restart {
		err := restartSequenceValue(t, schemaID, tblInfo, restartValue)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	// Store the sequence info into kv.
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, shouldUpdateVer)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Finish this job.
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

// Like setval does, restart sequence value won't affect current the step frequency. It will look backward for
// the first valid sequence valid rather than return the restart value directly.
func restartSequenceValue(t *meta.Meta, dbID int64, tblInfo *model.TableInfo, seqValue int64) error {
	var sequenceBase int64
	if tblInfo.Sequence.Increment >= 0 {
		sequenceBase = seqValue - 1
	} else {
		sequenceBase = seqValue + 1
	}
	return t.RestartSequenceValue(dbID, tblInfo, sequenceBase)
}
