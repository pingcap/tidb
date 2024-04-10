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

package validator

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestValidateDictionaryPassword(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	mock := variable.NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	err := mock.SetGlobalSysVar(context.Background(), variable.ValidatePasswordDictionary, "abc;123;1234;5678;HIJK;中文测试;。，；！")
	require.NoError(t, err)
	testcases := []struct {
		pwd    string
		result bool
	}{
		{"abcdefg", true},
		{"abcd123efg", true},
		{"abcd1234efg", false},
		{"abcd12345efg", false},
		{"abcd123efghij", true},
		{"abcd123efghijk", false},
		{"abcd123efghij中文测试", false},
		{"abcd123。，；！", false},
	}
	for _, testcase := range testcases {
		ok, err := ValidateDictionaryPassword(testcase.pwd, &vars.GlobalVarsAccessor)
		require.NoError(t, err)
		require.Equal(t, testcase.result, ok, testcase.pwd)
	}
}

func TestValidateUserNameInPassword(t *testing.T) {
	sessionVars := variable.NewSessionVars(nil)
	sessionVars.User = &auth.UserIdentity{Username: "user", AuthUsername: "authuser"}
	sessionVars.GlobalVarsAccessor = variable.NewMockGlobalAccessor4Tests()
	testcases := []struct {
		pwd  string
		warn string
	}{
		{"", ""},
		{"user", "Password Contains User Name"},
		{"authuser", "Password Contains User Name"},
		{"resu000", "Password Contains Reversed User Name"},
		{"resuhtua", "Password Contains Reversed User Name"},
		{"User", ""},
		{"authUser", ""},
		{"Resu", ""},
		{"Resuhtua", ""},
	}
	// Enable check_user_name
	err := sessionVars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.ValidatePasswordCheckUserName, "ON")
	require.NoError(t, err)
	for _, testcase := range testcases {
		warn, err := ValidateUserNameInPassword(testcase.pwd, sessionVars.User, &sessionVars.GlobalVarsAccessor)
		require.NoError(t, err)
		require.Equal(t, testcase.warn, warn, testcase.pwd)
	}

	// Disable check_user_name
	err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.ValidatePasswordCheckUserName, "OFF")
	require.NoError(t, err)
	for _, testcase := range testcases {
		warn, err := ValidateUserNameInPassword(testcase.pwd, sessionVars.User, &sessionVars.GlobalVarsAccessor)
		require.NoError(t, err)
		require.Equal(t, "", warn, testcase.pwd)
	}
}

func TestValidatePasswordLowPolicy(t *testing.T) {
	sessionVars := variable.NewSessionVars(nil)
	sessionVars.GlobalVarsAccessor = variable.NewMockGlobalAccessor4Tests()
	sessionVars.GlobalVarsAccessor.(*variable.MockGlobalAccessor).SessionVars = sessionVars
	err := sessionVars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.ValidatePasswordLength, "8")
	require.NoError(t, err)

	warn, err := ValidatePasswordLowPolicy("1234", &sessionVars.GlobalVarsAccessor)
	require.NoError(t, err)
	require.Equal(t, "Require Password Length: 8", warn)
	warn, err = ValidatePasswordLowPolicy("12345678", &sessionVars.GlobalVarsAccessor)
	require.NoError(t, err)
	require.Equal(t, "", warn)

	err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.ValidatePasswordLength, "12")
	require.NoError(t, err)
	warn, err = ValidatePasswordLowPolicy("12345678", &sessionVars.GlobalVarsAccessor)
	require.NoError(t, err)
	require.Equal(t, "Require Password Length: 12", warn)
}

func TestValidatePasswordMediumPolicy(t *testing.T) {
	sessionVars := variable.NewSessionVars(nil)
	sessionVars.GlobalVarsAccessor = variable.NewMockGlobalAccessor4Tests()
	sessionVars.GlobalVarsAccessor.(*variable.MockGlobalAccessor).SessionVars = sessionVars

	err := sessionVars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.ValidatePasswordMixedCaseCount, "1")
	require.NoError(t, err)
	err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.ValidatePasswordSpecialCharCount, "2")
	require.NoError(t, err)
	err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.ValidatePasswordNumberCount, "3")
	require.NoError(t, err)

	warn, err := ValidatePasswordMediumPolicy("!@A123", &sessionVars.GlobalVarsAccessor)
	require.NoError(t, err)
	require.Equal(t, "Require Password Lowercase Count: 1", warn)
	warn, err = ValidatePasswordMediumPolicy("!@a123", &sessionVars.GlobalVarsAccessor)
	require.NoError(t, err)
	require.Equal(t, "Require Password Uppercase Count: 1", warn)
	warn, err = ValidatePasswordMediumPolicy("!@Aa12", &sessionVars.GlobalVarsAccessor)
	require.NoError(t, err)
	require.Equal(t, "Require Password Digit Count: 3", warn)
	warn, err = ValidatePasswordMediumPolicy("!Aa123", &sessionVars.GlobalVarsAccessor)
	require.NoError(t, err)
	require.Equal(t, "Require Password Non-alphanumeric Count: 2", warn)
	warn, err = ValidatePasswordMediumPolicy("!@Aa123", &sessionVars.GlobalVarsAccessor)
	require.NoError(t, err)
	require.Equal(t, "", warn)
}

func TestValidatePassword(t *testing.T) {
	sessionVars := variable.NewSessionVars(nil)
	sessionVars.GlobalVarsAccessor = variable.NewMockGlobalAccessor4Tests()
	sessionVars.GlobalVarsAccessor.(*variable.MockGlobalAccessor).SessionVars = sessionVars
	sessionVars.User = &auth.UserIdentity{Username: "user", AuthUsername: "authuser"}

	err := sessionVars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.ValidatePasswordPolicy, "LOW")
	require.NoError(t, err)
	err = ValidatePassword(sessionVars, "1234")
	require.Error(t, err)
	err = ValidatePassword(sessionVars, "user1234")
	require.Error(t, err)
	err = ValidatePassword(sessionVars, "authuser1234")
	require.Error(t, err)
	err = ValidatePassword(sessionVars, "User1234")
	require.NoError(t, err)

	err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.ValidatePasswordPolicy, "MEDIUM")
	require.NoError(t, err)
	err = ValidatePassword(sessionVars, "User1234")
	require.Error(t, err)
	err = ValidatePassword(sessionVars, "!User1234")
	require.NoError(t, err)
	err = ValidatePassword(sessionVars, "！User1234")
	require.NoError(t, err)

	err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.ValidatePasswordPolicy, "STRONG")
	require.NoError(t, err)
	err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.ValidatePasswordDictionary, "User")
	require.NoError(t, err)
	err = ValidatePassword(sessionVars, "!User1234")
	require.Error(t, err)
	err = ValidatePassword(sessionVars, "!ABcd1234")
	require.NoError(t, err)
}
