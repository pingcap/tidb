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

package variable

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/mathutil"
)

// PasswordDictionaryImpl is the dictionary for validating password.
type PasswordDictionaryImpl struct {
	cache map[string]struct{}
	m     sync.RWMutex
}

const maxPwdLength int = 100
const minPwdLength int = 4

var passwordDictionary = PasswordDictionaryImpl{cache: make(map[string]struct{})}

// CleanPasswordDictionary removes all the words in the dictionary.
func CleanPasswordDictionary() {
	passwordDictionary.m.Lock()
	defer passwordDictionary.m.Unlock()
	passwordDictionary.cache = make(map[string]struct{})
}

// UpdatePasswordDictionary update the dictionary for validating password.
func UpdatePasswordDictionary(filePath string) error {
	passwordDictionary.m.Lock()
	defer passwordDictionary.m.Unlock()
	newDictionary := make(map[string]struct{})
	file, err := os.Open(filepath.Clean(filePath))
	if err != nil {
		return err
	}
	if fileInfo, err := file.Stat(); err != nil {
		return err
	} else if fileInfo.Size() > 1*1024*1024 {
		return errors.New("Too Large Dictionary. The maximum permitted file size is 1MB")
	}
	s := bufio.NewScanner(file)
	for s.Scan() {
		line := strings.ToLower(string(hack.String(s.Bytes())))
		if len(line) >= minPwdLength && len(line) <= maxPwdLength {
			newDictionary[line] = struct{}{}
		}
	}
	if err := s.Err(); err != nil {
		return err
	}
	passwordDictionary.cache = newDictionary
	return file.Close()
}

// ValidateDictionaryPassword checks if the password contains words in the dictionary.
func ValidateDictionaryPassword(pwd string) bool {
	passwordDictionary.m.RLock()
	defer passwordDictionary.m.RUnlock()
	if len(passwordDictionary.cache) == 0 {
		return true
	}
	pwdLength := len(pwd)
	for subStrLen := mathutil.Min(maxPwdLength, pwdLength); subStrLen >= minPwdLength; subStrLen-- {
		for subStrPos := 0; subStrPos+subStrLen <= pwdLength; subStrPos++ {
			subStr := pwd[subStrPos : subStrPos+subStrLen]
			if _, ok := passwordDictionary.cache[subStr]; ok {
				return false
			}
		}
	}
	return true
}

// CreateTmpDictWithSize is only used for test.
func CreateTmpDictWithSize(filename string, size int) (string, error) {
	filename = filepath.Join(os.TempDir(), filename)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return "", err
	}
	if size > 0 {
		n, err := file.Write(make([]byte, size))
		if err != nil {
			return "", err
		} else if n != size {
			return "", errors.New("")
		}
	}
	return filename, file.Close()
}

// CreateTmpDictWithContent is only used for test.
func CreateTmpDictWithContent(filename string, content []byte) (string, error) {
	filename = filepath.Join(os.TempDir(), filename)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return "", err
	}
	if len(content) > 0 {
		n, err := file.Write(content)
		if err != nil {
			return "", err
		} else if n != len(content) {
			return "", errors.New("")
		}
	}
	return filename, file.Close()
}

// ValidateUserNameInPassword checks whether pwd exists in the dictionary.
func ValidateUserNameInPassword(pwd string, sessionVars *SessionVars) (string, error) {
	currentUser := sessionVars.User
	globalVars := sessionVars.GlobalVarsAccessor
	pwdBytes := hack.Slice(pwd)
	if checkUserName, err := globalVars.GetGlobalSysVar(ValidatePasswordCheckUserName); err != nil {
		return "", err
	} else if currentUser != nil && TiDBOptOn(checkUserName) {
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
func ValidatePasswordLowPolicy(pwd string, globalVars *GlobalVarAccessor) (string, error) {
	if validateLengthStr, err := (*globalVars).GetGlobalSysVar(ValidatePasswordLength); err != nil {
		return "", err
	} else if validateLength, err := strconv.ParseInt(validateLengthStr, 10, 64); err != nil {
		return "", err
	} else if (int64)(len([]rune(pwd))) < validateLength {
		return fmt.Sprintf("Require Password Length: %d", validateLength), nil
	}
	return "", nil
}

// ValidatePasswordMediumPolicy checks whether pwd satisfies the medium policy of password validation.
func ValidatePasswordMediumPolicy(pwd string, globalVars *GlobalVarAccessor) (string, error) {
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
	if mixedCaseCountStr, err := (*globalVars).GetGlobalSysVar(ValidatePasswordMixedCaseCount); err != nil {
		return "", err
	} else if mixedCaseCount, err := strconv.ParseInt(mixedCaseCountStr, 10, 64); err != nil {
		return "", err
	} else if lowerCaseCount < mixedCaseCount {
		return fmt.Sprintf("Require Password Lowercase Count: %d", mixedCaseCount), nil
	} else if upperCaseCount < mixedCaseCount {
		return fmt.Sprintf("Require Password Uppercase Count: %d", mixedCaseCount), nil
	}
	if requireNumberCountStr, err := (*globalVars).GetGlobalSysVar(ValidatePasswordNumberCount); err != nil {
		return "", err
	} else if requireNumberCount, err := strconv.ParseInt(requireNumberCountStr, 10, 64); err != nil {
		return "", err
	} else if numberCount < requireNumberCount {
		return fmt.Sprintf("Require Password Digit Count: %d", requireNumberCount), nil
	}
	if requireSpecialCharCountStr, err := (*globalVars).GetGlobalSysVar(ValidatePasswordSpecialCharCount); err != nil {
		return "", err
	} else if requireSpecialCharCount, err := strconv.ParseInt(requireSpecialCharCountStr, 10, 64); err != nil {
		return "", err
	} else if specialCharCount < requireSpecialCharCount {
		return fmt.Sprintf("Require Password Non-alphanumeric Count: %d", requireSpecialCharCount), nil
	}
	return "", nil
}

// ValidatePassword checks whether the pwd can be used.
func ValidatePassword(sessionVars *SessionVars, pwd string) error {
	globalVars := sessionVars.GlobalVarsAccessor

	validatePolicy, err := globalVars.GetGlobalSysVar(ValidatePasswordPolicy)
	if err != nil {
		return err
	}
	if warn, err := ValidateUserNameInPassword(pwd, sessionVars); err != nil {
		return err
	} else if len(warn) > 0 {
		return ErrNotValidPassword.GenWithStack(warn)
	}
	if warn, err := ValidatePasswordLowPolicy(pwd, &globalVars); err != nil {
		return err
	} else if len(warn) > 0 {
		return ErrNotValidPassword.GenWithStack(warn)
	}
	// LOW
	if validatePolicy == "LOW" {
		return nil
	}

	// MEDIUM
	if warn, err := ValidatePasswordMediumPolicy(pwd, &globalVars); err != nil {
		return err
	} else if len(warn) > 0 {
		return ErrNotValidPassword.GenWithStack(warn)
	}
	if validatePolicy == "MEDIUM" {
		return nil
	}

	// STRONG
	if !ValidateDictionaryPassword(pwd) {
		return ErrNotValidPassword.GenWithStack("Password contains word in the dictionary")
	}
	return nil
}
