// Copyright 2025 PingCAP, Inc.
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

// Package compat provides compatibility functions for different versions of SEM
package compat

import (
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sem"
	semv2 "github.com/pingcap/tidb/pkg/util/sem/v2"
)

// IsEnabled checks if either SEM v1 or SEM v2 is enabled.
func IsEnabled() bool {
	intest.Assert(!(sem.IsEnabled() && semv2.IsEnabled()), "SEM v1 and v2 cannot be enabled at the same time")

	return sem.IsEnabled() || semv2.IsEnabled()
}

// IsInvisibleSchema is a compatibility wrapper for SEM v1 and v2.
func IsInvisibleSchema(dbName string) bool {
	intest.Assert(!(sem.IsEnabled() && semv2.IsEnabled()), "SEM v1 and v2 cannot be enabled at the same time")

	if sem.IsEnabled() && sem.IsInvisibleSchema(dbName) {
		return true
	} else if semv2.IsEnabled() && semv2.IsInvisibleSchema(dbName) {
		return true
	}

	return false
}

// IsInvisibleTable is a compatibility wrapper for SEM v1 and v2.
func IsInvisibleTable(dbLowerName, tblLowerName string) bool {
	intest.Assert(!(sem.IsEnabled() && semv2.IsEnabled()), "SEM v1 and v2 cannot be enabled at the same time")

	if sem.IsEnabled() && sem.IsInvisibleTable(dbLowerName, tblLowerName) {
		return true
	} else if semv2.IsEnabled() && semv2.IsInvisibleTable(dbLowerName, tblLowerName) {
		return true
	}

	return false
}

// IsInvisibleStatusVar is a compatibility wrapper for SEM v1 and v2.
func IsInvisibleStatusVar(varName string) bool {
	intest.Assert(!(sem.IsEnabled() && semv2.IsEnabled()), "SEM v1 and v2 cannot be enabled at the same time")

	if sem.IsEnabled() && sem.IsInvisibleStatusVar(varName) {
		return true
	} else if semv2.IsEnabled() && semv2.IsInvisibleStatusVar(varName) {
		return true
	}

	return false
}

// IsInvisibleSysVar is a compatibility wrapper for SEM v1 and v2.
func IsInvisibleSysVar(varName string) bool {
	intest.Assert(!(sem.IsEnabled() && semv2.IsEnabled()), "SEM v1 and v2 cannot be enabled at the same time")

	if sem.IsEnabled() && sem.IsInvisibleSysVar(varName) {
		return true
	} else if semv2.IsEnabled() && semv2.IsInvisibleSysVar(varName) {
		return true
	}

	return false
}

// IsRestrictedPrivilege is a compatibility wrapper for SEM v1 and v2.
func IsRestrictedPrivilege(privilege string) bool {
	intest.Assert(!(sem.IsEnabled() && semv2.IsEnabled()), "SEM v1 and v2 cannot be enabled at the same time")

	if sem.IsEnabled() && sem.IsRestrictedPrivilege(privilege) {
		return true
	} else if semv2.IsEnabled() && semv2.IsRestrictedPrivilege(privilege) {
		return true
	}

	return false
}
