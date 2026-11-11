// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parsedef

import (
	"github.com/pingcap/errors"
)

// ErrRowTooLarge is returned when the source text or the encoded size of a
// single row exceeds the max txn entry size limit. It is shared by the CSV,
// SQL and parquet parsers.
var ErrRowTooLarge = errors.NewNoStackError("size of row cannot exceed the max value of txn-entry-size-limit")
