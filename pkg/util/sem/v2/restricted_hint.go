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

package sem

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

// hintGuardVars maps an optimizer hint that overrides a system variable to that
// variable. Such a hint is restricted only while its variable is hidden or
// read-only under SEM, so a hint that mirrors a still-tunable variable stays
// available. Other restricted hints are stripped unconditionally.
var hintGuardVars = map[string]string{
	"memory_quota":            vardef.TiDBMemQuotaQuery,
	"read_consistent_replica": vardef.TiDBReplicaRead,
	"max_execution_time":      vardef.MaxExecutionTime,
}

// IsRestrictedHint returns a non-nil error when the optimizer hint is
// restricted by the SEM configuration. A restricted hint is stripped from the
// statement with a warning rather than rejected outright. hintNameLower is the
// lower-case hint name.
func IsRestrictedHint(hintNameLower string) error {
	sem := globalSem.Load()
	if sem == nil {
		return nil
	}
	return sem.isRestrictedHint(hintNameLower)
}

func (s *semImpl) isRestrictedHint(hintNameLower string) error {
	if _, ok := s.restrictedHints[hintNameLower]; !ok {
		return nil
	}
	// A variable-overriding hint is only restricted while its variable is, so a
	// hint mirroring a still-tunable variable stays available.
	if v, ok := hintGuardVars[hintNameLower]; ok && !s.isInvisibleSysVar(v) && !s.isReadOnlyVariable(v) {
		return nil
	}
	return fmt.Errorf("the %s() optimizer hint is restricted under the current security policy and is ignored", strings.ToUpper(hintNameLower))
}
