// Copyright 2025 PingCAP, Inc.
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

package session

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/stretchr/testify/require"
)

func getFunctionName(f func(sessionapi.Session, int64)) (string, error) {
	if f == nil {
		return "", errors.New("function is nil")
	}

	funcPtr := reflect.ValueOf(f).Pointer()
	if funcPtr == 0 {
		return "", errors.New("invalid function pointer")
	}

	fullName := runtime.FuncForPC(funcPtr).Name()
	if fullName == "" {
		return "", errors.New("unable to retrieve function name")
	}

	parts := strings.Split(fullName, ".")
	if len(parts) == 0 {
		return "", errors.New("invalid function name structure")
	}

	return parts[len(parts)-1], nil
}

func TestUpgradeToVerFunctionsCheck(t *testing.T) {
	var lastVer int64
	for _, verFn := range upgradeToVerFunctions {
		require.Greater(t, verFn.version, lastVer, "upgradeToVerFunctions should be in ascending order")
		lastVer = verFn.version
		require.NotNil(t, verFn.fn, "upgradeToVerFunctions should not have nil function")
		name, err := getFunctionName(verFn.fn)
		require.NoError(t, err, "getFunctionName should not return an error")
		require.Regexp(t, fmt.Sprintf(`^upgradeToVer%d$`, verFn.version), name, "function name should match upgradeToVer pattern")
	}
	require.Equal(t, currentBootstrapVersion, lastVer, "last version in upgradeToVerFunctions should match currentBootstrapVersion")
}
