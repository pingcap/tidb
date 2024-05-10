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
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/hack"
)

const maxPwdValidationLength int = 100

const minPwdValidationLength int = 4

// ValidateDictionaryPassword checks if the password contains words in the dictionary.
func ValidateDictionaryPassword(pwd string, globalVars *variable.GlobalVarAccessor) (bool, error) {
	dictionary, err := (*globalVars).GetGlobalSysVar(variable.ValidatePasswordDictionary)
	if err != nil {
		return false, err
	}
	words := strings.Split(dictionary, ";")
	if len(words) == 0 {
		return true, nil
	}
	pwd = strings.ToLower(pwd)
	for _, word := range words {
		if len(word) >= minPwdValidationLength && len(word) <= maxPwdValidationLength {
			if strings.Contains(pwd, strings.ToLower(word)) {
				return false, nil
			}
		}
	}
	return true, nil
}

// ValidateUserNameInPassword checks whether pwd exists in the dictionary.
func ValidateUserNameInPassword(pwd string, currentUser *auth.UserIdentity, globalVars *variable.GlobalVarAccessor) (string, error) {
	pwdBytes := hack.Slice(pwd)
	if checkUserName, err := (*globalVars).GetGlobalSysVar(variable.ValidatePasswordCheckUserName); err != nil {
		return "", err
	} else if currentUser != nil && variable.TiDBOptOn(checkUserName) {
		for _, username := range []string{currentUser.AuthUsername, currentUser.Username} {
			usernameBytes := hack.Slice(username)
			userNameLen := len(usernameBytes)
			if userNameLen == 0 {
				continue
			}
			if bytes.Contains(pwdBytes, usernameBytes) {
				return "Password Contains User Name", nil
			}
			usernameReversedBytes := make([]byte, userNameLen)
			for i := range usernameBytes {
				usernameReversedBytes[i] = usernameBytes[userNameLen-1-i]
			}
			if bytes.Contains(pwdBytes, usernameReversedBytes) {
				return "Password Contains Reversed User Name", nil
			}
		}
	}
	return "", nil
}

// ValidatePasswordLowPolicy checks whether pwd satisfies the low policy of password validation.
func ValidatePasswordLowPolicy(pwd string, globalVars *variable.GlobalVarAccessor) (string, error) {
	if validateLengthStr, err := (*globalVars).GetGlobalSysVar(variable.ValidatePasswordLength); err != nil {
		return "", err
	} else if validateLength, err := strconv.ParseInt(validateLengthStr, 10, 64); err != nil {
		return "", err
	} else if (int64)(len([]rune(pwd))) < validateLength {
		return fmt.Sprintf("Require Password Length: %d", validateLength), nil
	}
	return "", nil
}

// ValidatePasswordMediumPolicy checks whether pwd satisfies the medium policy of password validation.
func ValidatePasswordMediumPolicy(pwd string, globalVars *variable.GlobalVarAccessor) (string, error) {
	var lowerCaseCount, upperCaseCount, numberCount, specialCharCount int64
	runes := []rune(pwd)
	for i := 0; i < len(runes); i++ {
		if unicode.IsUpper(runes[i]) {
			upperCaseCount++
		} else if unicode.IsLower(runes[i]) {
			lowerCaseCount++
		} else if unicode.IsDigit(runes[i]) {
			numberCount++
		} else {
			specialCharCount++
		}
	}
	if mixedCaseCountStr, err := (*globalVars).GetGlobalSysVar(variable.ValidatePasswordMixedCaseCount); err != nil {
		return "", err
	} else if mixedCaseCount, err := strconv.ParseInt(mixedCaseCountStr, 10, 64); err != nil {
		return "", err
	} else if lowerCaseCount < mixedCaseCount {
		return fmt.Sprintf("Require Password Lowercase Count: %d", mixedCaseCount), nil
	} else if upperCaseCount < mixedCaseCount {
		return fmt.Sprintf("Require Password Uppercase Count: %d", mixedCaseCount), nil
	}
	if requireNumberCountStr, err := (*globalVars).GetGlobalSysVar(variable.ValidatePasswordNumberCount); err != nil {
		return "", err
	} else if requireNumberCount, err := strconv.ParseInt(requireNumberCountStr, 10, 64); err != nil {
		return "", err
	} else if numberCount < requireNumberCount {
		return fmt.Sprintf("Require Password Digit Count: %d", requireNumberCount), nil
	}
	if requireSpecialCharCountStr, err := (*globalVars).GetGlobalSysVar(variable.ValidatePasswordSpecialCharCount); err != nil {
		return "", err
	} else if requireSpecialCharCount, err := strconv.ParseInt(requireSpecialCharCountStr, 10, 64); err != nil {
		return "", err
	} else if specialCharCount < requireSpecialCharCount {
		return fmt.Sprintf("Require Password Non-alphanumeric Count: %d", requireSpecialCharCount), nil
	}
	return "", nil
}

// ValidatePassword checks whether the pwd can be used.
func ValidatePassword(sessionVars *variable.SessionVars, pwd string) error {
	globalVars := sessionVars.GlobalVarsAccessor

	validatePolicy, err := globalVars.GetGlobalSysVar(variable.ValidatePasswordPolicy)
	if err != nil {
		return err
	}
	if warn, err := ValidateUserNameInPassword(pwd, sessionVars.User, &sessionVars.GlobalVarsAccessor); err != nil {
		return err
	} else if len(warn) > 0 {
		return variable.ErrNotValidPassword.GenWithStackByArgs(warn)
	}
	if warn, err := ValidatePasswordLowPolicy(pwd, &globalVars); err != nil {
		return err
	} else if len(warn) > 0 {
		return variable.ErrNotValidPassword.GenWithStackByArgs(warn)
	}
	// LOW
	if validatePolicy == "LOW" {
		return nil
	}

	// MEDIUM
	if warn, err := ValidatePasswordMediumPolicy(pwd, &globalVars); err != nil {
		return err
	} else if len(warn) > 0 {
		return variable.ErrNotValidPassword.GenWithStackByArgs(warn)
	}
	if validatePolicy == "MEDIUM" {
		return nil
	}

	// STRONG
	if ok, err := ValidateDictionaryPassword(pwd, &globalVars); err != nil {
		return err
	} else if !ok {
		return variable.ErrNotValidPassword.GenWithStackByArgs("Password contains word in the dictionary")
	}
	return nil
}
