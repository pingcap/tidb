// Copyright 2022 PingCAP, Inc.
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

package common

import (
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
)

var (
	ErrUnknown         = errors.Normalize("unknown error", errors.RFCCodeText("Lightning:Common:ErrUnknown"))
	ErrInvalidArgument = errors.Normalize("invalid argument", errors.RFCCodeText("Lightning:Common:ErrInvalidArgument"))
	ErrVersionMismatch = errors.Normalize("version mismatch", errors.RFCCodeText("Lightning:Common:ErrVersionMismatch"))

	ErrReadConfigFile     = errors.Normalize("cannot read config file '%s'", errors.RFCCodeText("Lightning:Config:ErrReadConfigFile"))
	ErrParseConfigFile    = errors.Normalize("cannot parse config file '%s'", errors.RFCCodeText("Lightning:Config:ErrParseConfigFile"))
	ErrInvalidConfig      = errors.Normalize("invalid config", errors.RFCCodeText("Lightning:Config:ErrInvalidConfig"))
	ErrInvalidTLSConfig   = errors.Normalize("invalid tls config", errors.RFCCodeText("Lightning:Config:ErrInvalidTLSConfig"))
	ErrInvalidSortedKVDir = errors.Normalize("invalid sorted-kv-dir '%s' for local backend, please change the config or delete the path", errors.RFCCodeText("Lightning:Config:ErrInvalidSortedKVDir"))

	ErrStorageUnknown       = errors.Normalize("unknown storage error", errors.RFCCodeText("Lightning:Storage:ErrStorageUnknown"))
	ErrInvalidPermission    = errors.Normalize("invalid permission", errors.RFCCodeText("Lightning:Storage:ErrInvalidPermission"))
	ErrInvalidStorageConfig = errors.Normalize("invalid data-source-dir", errors.RFCCodeText("Lightning:Storage:ErrInvalidStorageConfig"))
	ErrEmptySourceDir       = errors.Normalize("data-source-dir '%s' doesn't exist or contains no files", errors.RFCCodeText("Lightning:Storage:ErrEmptySourceDir"))

	ErrTableRoute        = errors.Normalize("table route error", errors.RFCCodeText("Lightning:Loader:ErrTableRoute"))
	ErrInvalidSchemaFile = errors.Normalize("invalid schema file", errors.RFCCodeText("Lightning:Loader:ErrInvalidSchemaFile"))

	ErrSystemRequirementNotMet  = errors.Normalize("system requirement not met", errors.RFCCodeText("Lightning:PreCheck:ErrSystemRequirementNotMet"))
	ErrCheckpointSchemaConflict = errors.Normalize("checkpoint schema conflict", errors.RFCCodeText("Lightning:PreCheck:ErrCheckpointSchemaConflict"))
	ErrPreCheckFailed           = errors.Normalize("tidb-lightning pre-check failed: %s", errors.RFCCodeText("Lightning:PreCheck:ErrPreCheckFailed"))
	ErrCheckClusterRegion       = errors.Normalize("check tikv cluster region error", errors.RFCCodeText("Lightning:PreCheck:ErrCheckClusterRegion"))
	ErrCheckLocalResource       = errors.Normalize("check local storage resource error", errors.RFCCodeText("Lightning:PreCheck:ErrCheckLocalResource"))
	ErrCheckTableEmpty          = errors.Normalize("check table empty error", errors.RFCCodeText("Lightning:PreCheck:ErrCheckTableEmpty"))
	ErrCheckCSVHeader           = errors.Normalize("check csv header error", errors.RFCCodeText("Lightning:PreCheck:ErrCheckCSVHeader"))
	ErrCheckDataSource          = errors.Normalize("check data source error", errors.RFCCodeText("Lightning:PreCheck:ErrCheckDataSource"))

	ErrOpenCheckpoint          = errors.Normalize("open checkpoint error", errors.RFCCodeText("Lightning:Checkpoint:ErrOpenCheckpoint"))
	ErrReadCheckpoint          = errors.Normalize("read checkpoint error", errors.RFCCodeText("Lightning:Checkpoint:ErrReadCheckpoint"))
	ErrUpdateCheckpoint        = errors.Normalize("update checkpoint error", errors.RFCCodeText("Lightning:Checkpoint:ErrUpdateCheckpoint"))
	ErrUnknownCheckpointDriver = errors.Normalize("unknown checkpoint driver '%s'", errors.RFCCodeText("Lightning:Checkpoint:ErrUnknownCheckpointDriver"))
	ErrInvalidCheckpoint       = errors.Normalize("invalid checkpoint", errors.RFCCodeText("Lightning:Checkpoint:ErrInvalidCheckpoint"))
	ErrCheckpointNotFound      = errors.Normalize("checkpoint not found", errors.RFCCodeText("Lightning:Checkpoint:ErrCheckpointNotFound"))
	ErrInitCheckpoint          = errors.Normalize("init checkpoint error", errors.RFCCodeText("Lightning:Checkpoint:ErrInitCheckpoint"))
	ErrCleanCheckpoint         = errors.Normalize("clean checkpoint error", errors.RFCCodeText("Lightning:Checkpoint:ErrCleanCheckpoint"))

	ErrMetaMgrUnknown = errors.Normalize("unknown error occur on meta manager", errors.RFCCodeText("Lightning:MetaMgr:ErrMetaMgrUnknown"))

	ErrDBConnect       = errors.Normalize("failed to connect database", errors.RFCCodeText("Lightning:DB:ErrDBConnect"))
	ErrInitErrManager  = errors.Normalize("init error manager error", errors.RFCCodeText("Lightning:DB:ErrInitErrManager"))
	ErrInitMetaManager = errors.Normalize("init meta manager error", errors.RFCCodeText("Lightning:DB:ErrInitMetaManager"))

	ErrUpdatePD       = errors.Normalize("update pd error", errors.RFCCodeText("Lightning:PD:ErrUpdatePD"))
	ErrCreatePDClient = errors.Normalize("create pd client error", errors.RFCCodeText("Lightning:PD:ErrCreatePDClient"))
	ErrPauseGC        = errors.Normalize("pause gc error", errors.RFCCodeText("Lightning:PD:ErrPauseGC"))

	ErrCheckKVVersion   = errors.Normalize("check tikv version error", errors.RFCCodeText("Lightning:KV:ErrCheckKVVersion"))
	ErrCreateKVClient   = errors.Normalize("create kv client error", errors.RFCCodeText("Lightning:KV:ErrCreateKVClient"))
	ErrCheckMultiIngest = errors.Normalize("check multi-ingest support error", errors.RFCCodeText("Lightning:KV:ErrCheckMultiIngest"))
	ErrKVEpochNotMatch  = errors.Normalize("epoch not match", errors.RFCCodeText("Lightning:KV:EpochNotMatch"))
	ErrKVNotLeader      = errors.Normalize("not leader", errors.RFCCodeText("Lightning:KV:NotLeader"))
	ErrKVServerIsBusy   = errors.Normalize("server is busy", errors.RFCCodeText("Lightning:KV:ServerIsBusy"))
	ErrKVRegionNotFound = errors.Normalize("region not found", errors.RFCCodeText("Lightning:KV:RegionNotFound"))

	ErrUnknownBackend     = errors.Normalize("unknown backend %s", errors.RFCCodeText("Lightning:Restore:ErrUnknownBackend"))
	ErrCheckLocalFile     = errors.Normalize("cannot find local file for table: %s engineDir: %s", errors.RFCCodeText("Lightning:Restore:ErrCheckLocalFile"))
	ErrOpenDuplicateDB    = errors.Normalize("open duplicate db error", errors.RFCCodeText("Lightning:Restore:ErrOpenDuplicateDB"))
	ErrSchemaNotExists    = errors.Normalize("table `%s`.`%s` schema not found", errors.RFCCodeText("Lightning:Restore:ErrSchemaNotExists"))
	ErrInvalidSchemaStmt  = errors.Normalize("invalid schema statement: '%s'", errors.RFCCodeText("Lightning:Restore:ErrInvalidSchemaStmt"))
	ErrCreateSchema       = errors.Normalize("create schema failed, table: %s, stmt: %s", errors.RFCCodeText("Lightning:Restore:ErrCreateSchema"))
	ErrUnknownColumns     = errors.Normalize("unknown columns in header (%s) for table %s", errors.RFCCodeText("Lightning:Restore:ErrUnknownColumns"))
	ErrChecksumMismatch   = errors.Normalize("checksum mismatched remote vs local => (checksum: %d vs %d) (total_kvs: %d vs %d) (total_bytes:%d vs %d)", errors.RFCCodeText("Lighting:Restore:ErrChecksumMismatch"))
	ErrRestoreTable       = errors.Normalize("restore table %s failed", errors.RFCCodeText("Lightning:Restore:ErrRestoreTable"))
	ErrEncodeKV           = errors.Normalize("encode kv error in file %s at offset %d", errors.RFCCodeText("Lightning:Restore:ErrEncodeKV"))
	ErrAllocTableRowIDs   = errors.Normalize("allocate table row id error", errors.RFCCodeText("Lightning:Restore:ErrAllocTableRowIDs"))
	ErrInvalidMetaStatus  = errors.Normalize("invalid meta status: '%s'", errors.RFCCodeText("Lightning:Restore:ErrInvalidMetaStatus"))
	ErrTableIsChecksuming = errors.Normalize("table '%s' is checksuming", errors.RFCCodeText("Lightning:Restore:ErrTableIsChecksuming"))
)

type withStack struct {
	error
	errors.StackTracer
}

func (w *withStack) Cause() error {
	return w.error
}

func (w *withStack) Unwrap() error {
	return w.error
}

func (w *withStack) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", w.Cause())
			w.StackTrace().Format(s, verb)
			return
		}
		fallthrough
	case 's':
		_, _ = io.WriteString(s, w.Error())
	case 'q':
		fmt.Fprintf(s, "%q", w.Error())
	}
}

// NormalizeError converts an arbitrary error to *errors.Error based above predefined errors.
// If the underlying err is already an *error.Error which is prefixed by "Lightning:", leave
// error ID unchanged. Otherwise, converts the error ID to Lightning's predefined error IDs.
func NormalizeError(err error) error {
	if err == nil {
		return nil
	}
	if IsContextCanceledError(err) {
		return err
	}

	// Retain the original stack tracer.
	stackTracker, hasStack := err.(errors.StackTracer)
	maybeAddStack := func(err error) error {
		if !hasStack {
			return err
		}
		return &withStack{error: err, StackTracer: stackTracker}
	}

	// Find the underlying expectErr.
	var normalizedErr *errors.Error
	foundErr := errors.Find(err, func(e error) bool {
		_, ok := e.(*errors.Error)
		return ok
	})
	if foundErr != nil {
		normalizedErr = foundErr.(*errors.Error)
		errMsg := err.Error()
		nErrMsg := normalizedErr.Error()
		// A workaround for https://github.com/pingcap/tidb/issues/32133.
		// Ensure that error code description is always placed at the beginning of the error string.
		if errMsg != nErrMsg && strings.HasSuffix(errMsg, ": "+nErrMsg) {
			errMsg = errMsg[:len(errMsg)-len(nErrMsg)-2]
			causeErr := normalizedErr.Unwrap()
			normalizedErr = errors.Normalize(errMsg, errors.RFCCodeText(string(normalizedErr.RFCCode())))
			if causeErr != nil {
				normalizedErr = normalizedErr.Wrap(causeErr)
			}
			err = maybeAddStack(normalizedErr)
		}
	}

	if normalizedErr != nil {
		if strings.HasPrefix(string(normalizedErr.ID()), "Lightning:") {
			return err
		}
		// Convert BR error id to Lightning error id.
		var errID errors.ErrorID
		switch normalizedErr.ID() {
		case berrors.ErrStorageUnknown.ID():
			errID = ErrStorageUnknown.ID()
		case berrors.ErrStorageInvalidConfig.ID():
			errID = ErrInvalidStorageConfig.ID()
		case berrors.ErrStorageInvalidPermission.ID():
			errID = ErrInvalidPermission.ID()
		case berrors.ErrPDUpdateFailed.ID():
			errID = ErrUpdatePD.ID()
		case berrors.ErrVersionMismatch.ID():
			errID = ErrVersionMismatch.ID()
		default:
			errID = ErrUnknown.ID()
		}
		causeErr := normalizedErr.Unwrap()
		normalizedErr = errors.Normalize(normalizedErr.GetMsg(), errors.RFCCodeText(string(errID)))
		if causeErr != nil {
			normalizedErr = normalizedErr.Wrap(causeErr)
		}
		err = maybeAddStack(normalizedErr)
	} else {
		err = ErrUnknown.Wrap(err)
	}
	// TODO: Do we need to optimize the output error messages for aws errors or gRPC errors.
	return err
}

// NormalizeOrWrapErr tries to normalize err. If the returned error is ErrUnknown, wraps it with the given rfcErr.
func NormalizeOrWrapErr(rfcErr *errors.Error, err error, args ...interface{}) error {
	if err == nil {
		return nil
	}
	if IsContextCanceledError(err) {
		return err
	}
	normalizedErr := NormalizeError(err)
	if berrors.Is(normalizedErr, ErrUnknown) {
		return rfcErr.Wrap(err).GenWithStackByArgs(args...)
	} else {
		return normalizedErr
	}
}
