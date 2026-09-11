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

package task

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
)

func (cfg *RestoreConfig) validateRestoreRegionConfig(cmdName string) error {
	if !cfg.RestoreRegion {
		return nil
	}
	if !kerneltype.IsNextGen() {
		return errors.New("experimental RestoreRegion requires a nextgen BR binary and a CSE target")
	}
	if IsStreamRestore(cmdName) || cfg.RestorePhase != 0 || cfg.FullBackupType == FullBackupTypeEBS {
		return errors.New("experimental RestoreRegion only supports full snapshot restore")
	}
	if cfg.Online || cfg.NoSchema {
		return errors.New("experimental RestoreRegion requires offline restore into newly created tables")
	}
	if cfg.RateLimit != 0 {
		return errors.New("experimental RestoreRegion does not support BR rate-limit; configure limits on the Worker")
	}
	return nil
}
