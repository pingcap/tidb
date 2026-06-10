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

package errmsg

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// Extend appends a configured suffix to selected SQL errors in place.
// It reads the immutable error message extension cache prepared from global
// config, and applies only the first matching regexp.
func Extend(m *mysql.SQLError) {
	if m == nil {
		return
	}

	extensions := config.GetErrorMessageExtensions()
	if len(extensions) == 0 {
		return
	}

	for _, extension := range extensions {
		if extension.Suffix == "" || extension.Regexp == nil {
			continue
		}
		if extension.Regexp.MatchString(m.Message) {
			extendErrorMessage(m, extension.Suffix)
			return
		}
	}
}

func extendErrorMessage(m *mysql.SQLError, msg string) {
	m.Message = fmt.Sprintf("%s, %s.", strings.TrimSuffix(m.Message, "."), strings.TrimSuffix(msg, "."))
}
