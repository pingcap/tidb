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

package config

import (
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/atomic"
)

// PostOpLevel represents the level of post-operation.
type PostOpLevel int

// PostOpLevel constants.
const (
	OpLevelOff PostOpLevel = iota
	OpLevelOptional
	OpLevelRequired
)

// UnmarshalTOML implements toml.Unmarshaler interface.
func (t *PostOpLevel) UnmarshalTOML(v any) error {
	switch val := v.(type) {
	case bool:
		if val {
			*t = OpLevelRequired
		} else {
			*t = OpLevelOff
		}
	case string:
		return t.FromStringValue(val)
	default:
		return errors.Errorf("invalid op level '%v', please choose valid option between ['off', 'optional', 'required']", v)
	}
	return nil
}

// MarshalText implements encoding.TextMarshaler interface.
func (t PostOpLevel) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

// FromStringValue parse command line parameter.
func (t *PostOpLevel) FromStringValue(s string) error {
	switch strings.ToLower(s) {
	//nolint:goconst // This 'false' and other 'false's aren't the same.
	case "off", "false":
		*t = OpLevelOff
	case "required", "true":
		*t = OpLevelRequired
	case "optional":
		*t = OpLevelOptional
	default:
		return errors.Errorf("invalid op level '%s', please choose valid option between ['off', 'optional', 'required']", s)
	}
	return nil
}

// MarshalJSON implements json.Marshaler interface.
func (t *PostOpLevel) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (t *PostOpLevel) UnmarshalJSON(data []byte) error {
	return t.FromStringValue(strings.Trim(string(data), `"`))
}

// String returns the string representation of the level.
func (t PostOpLevel) String() string {
	switch t {
	case OpLevelOff:
		return "off"
	case OpLevelOptional:
		return "optional"
	case OpLevelRequired:
		return "required"
	default:
		panic(fmt.Sprintf("invalid post process type '%d'", t))
	}
}

// CheckpointKeepStrategy represents the strategy to keep checkpoint data.
type CheckpointKeepStrategy int

const (
	// CheckpointRemove remove checkpoint data
	CheckpointRemove CheckpointKeepStrategy = iota
	// CheckpointRename keep by rename checkpoint file/db according to task id
	CheckpointRename
	// CheckpointOrigin keep checkpoint data unchanged
	CheckpointOrigin
)

// UnmarshalTOML implements toml.Unmarshaler interface.
func (t *CheckpointKeepStrategy) UnmarshalTOML(v any) error {
	switch val := v.(type) {
	case bool:
		if val {
			*t = CheckpointRename
		} else {
			*t = CheckpointRemove
		}
	case string:
		return t.FromStringValue(val)
	default:
		return errors.Errorf("invalid checkpoint keep strategy '%v', please choose valid option between ['remove', 'rename', 'origin']", v)
	}
	return nil
}

// MarshalText implements encoding.TextMarshaler interface.
func (t CheckpointKeepStrategy) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

// FromStringValue parser command line parameter.
func (t *CheckpointKeepStrategy) FromStringValue(s string) error {
	switch strings.ToLower(s) {
	//nolint:goconst // This 'false' and other 'false's aren't the same.
	case "remove", "false":
		*t = CheckpointRemove
	case "rename", "true":
		*t = CheckpointRename
	case "origin":
		*t = CheckpointOrigin
	default:
		return errors.Errorf("invalid checkpoint keep strategy '%s', please choose valid option between ['remove', 'rename', 'origin']", s)
	}
	return nil
}

// MarshalJSON implements json.Marshaler interface.
func (t *CheckpointKeepStrategy) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (t *CheckpointKeepStrategy) UnmarshalJSON(data []byte) error {
	return t.FromStringValue(strings.Trim(string(data), `"`))
}

// String implements fmt.Stringer interface.
func (t CheckpointKeepStrategy) String() string {
	switch t {
	case CheckpointRemove:
		return "remove"
	case CheckpointRename:
		return "rename"
	case CheckpointOrigin:
		return "origin"
	default:
		panic(fmt.Sprintf("invalid post process type '%d'", t))
	}
}

// MaxError configures the maximum number of acceptable errors per kind.
type MaxError struct {
	// Syntax is the maximum number of syntax errors accepted.
	// When tolerated, the file chunk causing syntax error will be skipped, and adds 1 to the counter.
	// TODO Currently this is hard-coded to zero.
	Syntax atomic.Int64 `toml:"syntax" json:"-"`

	// Charset is the maximum number of character-set conversion errors accepted.
	// When tolerated, and `data-invalid-char-replace` is not changed from "\ufffd",
	// every invalid byte in the source file will be converted to U+FFFD and adds 1 to the counter.
	// Note that a failed conversion a column's character set (e.g. UTF8-to-GBK conversion)
	// is counted as a type error, not a charset error.
	// TODO character-set conversion is not yet implemented.
	Charset atomic.Int64 `toml:"charset" json:"-"`

	// Type is the maximum number of type errors accepted.
	// This includes strict-mode errors such as zero in dates, integer overflow, character string too long, etc.
	// In TiDB backend, this also includes all possible SQL errors raised from INSERT,
	// such as unique key conflict when `on-duplicate` is set to `error`.
	// When tolerated, the row causing the error will be skipped, and adds 1 to the counter.
	// The default value is zero, which means that such errors are not tolerated.
	Type atomic.Int64 `toml:"type" json:"type"`

	// deprecated, use `conflict.threshold` instead.
	// Conflict is the maximum number of unique key conflicts in local backend accepted.
	// When tolerated, every pair of conflict adds 1 to the counter.
	// Those pairs will NOT be deleted from the target. Conflict resolution is performed separately.
	// The default value is max int64, which means conflict errors will be recorded as much as possible.
	// Sometime the actual number of conflict record logged will be greater than the value configured here,
	// because conflict error data are recorded batch by batch.
	// If the limit is reached in a single batch, the entire batch of records will be persisted before an error is reported.
	Conflict atomic.Int64 `toml:"conflict" json:"conflict"`
}

// UnmarshalTOML implements toml.Unmarshaler interface.
func (cfg *MaxError) UnmarshalTOML(v any) error {
	defaultValMap := map[string]int64{
		"syntax":  0,
		"charset": math.MaxInt64,
		"type":    0,
	}
	// set default value first
	cfg.Syntax.Store(defaultValMap["syntax"])
	cfg.Charset.Store(defaultValMap["charset"])
	cfg.Type.Store(defaultValMap["type"])
	switch val := v.(type) {
	case int64:
		// ignore val that is smaller than 0
		if val >= 0 {
			// only set type error
			cfg.Type.Store(val)
		}
		return nil
	case map[string]any:
		// support stuff like `max-error = { charset = 1000, type = 1000 }`.
		getVal := func(k string, v any) int64 {
			defaultVal, ok := defaultValMap[k]
			if !ok {
				return 0
			}
			iVal, ok := v.(int64)
			if !ok || iVal < 0 {
				return defaultVal
			}
			return iVal
		}
		for k, v := range val {
			if k == "type" {
				cfg.Type.Store(getVal(k, v))
			}
		}
		return nil
	default:
		return errors.Errorf("invalid max-error '%v', should be an integer or a map of string:int64", v)
	}
}

// PausePDSchedulerScope the scope when pausing pd schedulers.
type PausePDSchedulerScope string

// constants for PausePDSchedulerScope.
const (
	// PausePDSchedulerScopeTable pause scheduler by adding schedule=deny label to target key range of the table.
	PausePDSchedulerScopeTable PausePDSchedulerScope = "table"
	// PausePDSchedulerScopeGlobal pause scheduler by remove global schedulers.
	// schedulers removed includes:
	// 	- balance-leader-scheduler
	// 	- balance-hot-region-scheduler
	// 	- balance-region-scheduler
	// 	- shuffle-leader-scheduler
	// 	- shuffle-region-scheduler
	// 	- shuffle-hot-region-scheduler
	// and we also set configs below:
	// 	- max-merge-region-keys = 0
	// 	- max-merge-region-size = 0
	// 	- leader-schedule-limit = min(40, <store-count> * <current value of leader-schedule-limit>)
	// 	- region-schedule-limit = min(40, <store-count> * <current value of region-schedule-limit>)
	// 	- max-snapshot-count = min(40, <store-count> * <current value of max-snapshot-count>)
	// 	- enable-location-replacement = false
	// 	- max-pending-peer-count = math.MaxInt32
	// see br/pkg/pdutil/pd.go for more detail.
	PausePDSchedulerScopeGlobal PausePDSchedulerScope = "global"
)

// DuplicateResolutionAlgorithm is the config type of how to resolve duplicates.
type DuplicateResolutionAlgorithm int

const (
	// NoneOnDup does nothing when detecting duplicate.
	NoneOnDup DuplicateResolutionAlgorithm = iota
	// ReplaceOnDup indicates using REPLACE INTO to insert data for TiDB backend.
	// ReplaceOnDup records all duplicate records, remove some rows with conflict
	// and reserve other rows that can be kept and not cause conflict anymore for local backend.
	// Users need to analyze the lightning_task_info.conflict_view table to check whether the reserved data
	// cater to their need and check whether they need to add back the correct rows.
	ReplaceOnDup
	// IgnoreOnDup indicates using INSERT IGNORE INTO to insert data for TiDB backend.
	// Local backend does not support IgnoreOnDup.
	IgnoreOnDup
	// ErrorOnDup indicates using INSERT INTO to insert data for TiDB backend, which would violate PK or UNIQUE constraint when detecting duplicate.
	// ErrorOnDup reports an error after detecting the first conflict and stops the import process for local backend.
	ErrorOnDup
)

// UnmarshalTOML implements the toml.Unmarshaler interface.
func (dra *DuplicateResolutionAlgorithm) UnmarshalTOML(v any) error {
	if val, ok := v.(string); ok {
		return dra.FromStringValue(val)
	}
	return errors.Errorf("invalid conflict.strategy '%v', please choose valid option between ['', 'replace', 'ignore', 'error']", v)
}

// MarshalText implements the encoding.TextMarshaler interface.
func (dra DuplicateResolutionAlgorithm) MarshalText() ([]byte, error) {
	return []byte(dra.String()), nil
}

// FromStringValue parses the string value to the DuplicateResolutionAlgorithm.
func (dra *DuplicateResolutionAlgorithm) FromStringValue(s string) error {
	switch strings.ToLower(s) {
	case "", "none":
		*dra = NoneOnDup
	case "replace":
		*dra = ReplaceOnDup
	case "ignore":
		*dra = IgnoreOnDup
	case "error":
		*dra = ErrorOnDup
	case "remove", "record":
		log.L().Warn("\"conflict.strategy '%s' is no longer supported, has been converted to 'replace'")
		*dra = ReplaceOnDup
	default:
		return errors.Errorf("invalid conflict.strategy '%s', please choose valid option between ['', 'replace', 'ignore', 'error']", s)
	}
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (dra *DuplicateResolutionAlgorithm) MarshalJSON() ([]byte, error) {
	return []byte(`"` + dra.String() + `"`), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (dra *DuplicateResolutionAlgorithm) UnmarshalJSON(data []byte) error {
	return dra.FromStringValue(strings.Trim(string(data), `"`))
}

// String implements the fmt.Stringer interface.
func (dra DuplicateResolutionAlgorithm) String() string {
	switch dra {
	case NoneOnDup:
		return ""
	case ReplaceOnDup:
		return "replace"
	case IgnoreOnDup:
		return "ignore"
	case ErrorOnDup:
		return "error"
	default:
		panic(fmt.Sprintf("invalid conflict.strategy type '%d'", dra))
	}
}

// CompressionType is the config type of compression algorithm.
type CompressionType int

const (
	// CompressionNone means no compression.
	CompressionNone CompressionType = iota
	// CompressionGzip means gzip compression.
	CompressionGzip
)

// UnmarshalTOML implements toml.Unmarshaler.
func (t *CompressionType) UnmarshalTOML(v any) error {
	if val, ok := v.(string); ok {
		return t.FromStringValue(val)
	}
	return errors.Errorf("invalid compression-type '%v', please choose valid option between ['gzip']", v)
}

// MarshalText implements encoding.TextMarshaler.
func (t CompressionType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

// FromStringValue parses a string to CompressionType.
func (t *CompressionType) FromStringValue(s string) error {
	switch strings.ToLower(s) {
	case "":
		*t = CompressionNone
	case "gz", "gzip":
		*t = CompressionGzip
	default:
		return errors.Errorf("invalid compression-type '%s', please choose valid option between ['gzip']", s)
	}
	return nil
}

// MarshalJSON implements json.Marshaler.
func (t *CompressionType) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *CompressionType) UnmarshalJSON(data []byte) error {
	return t.FromStringValue(strings.Trim(string(data), `"`))
}

// String implements fmt.Stringer.
func (t CompressionType) String() string {
	switch t {
	case CompressionGzip:
		return "gzip"
	case CompressionNone:
		return ""
	default:
		panic(fmt.Sprintf("invalid compression type '%d'", t))
	}
}
