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
	"bufio"
	"bytes"
	"github.com/pingcap/tidb/sessionctx/variable"
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

type dictionaryImpl struct {
	cache map[string]struct{}
	m     sync.RWMutex
}

const maxPwdLength int = 100
const minPwdLength int = 4

var dictionary = dictionaryImpl{cache: make(map[string]struct{})}

// Clean removes all the words in the dictionary.
func Clean() {
	dictionary.m.Lock()
	defer dictionary.m.Unlock()
	dictionary.cache = make(map[string]struct{})
}

// UpdateDictionaryFile update the dictionary for validating password.
func UpdateDictionaryFile(filePath string) error {
	dictionary.m.Lock()
	defer dictionary.m.Unlock()
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
	dictionary.cache = newDictionary
	return file.Close()
}

// validateDictionaryPassword checks if the password contains words in the dictionary.
func validateDictionaryPassword(pwd string) bool {
	dictionary.m.RLock()
	defer dictionary.m.RUnlock()
	if len(dictionary.cache) == 0 {
		return true
	}
	pwdLength := len(pwd)
	for subStrLen := mathutil.Min(maxPwdLength, pwdLength); subStrLen >= minPwdLength; subStrLen-- {
		for subStrPos := 0; subStrPos+subStrLen <= pwdLength; subStrPos++ {
			subStr := pwd[subStrPos : subStrPos+subStrLen]
			if _, ok := dictionary.cache[subStr]; ok {
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

func ValidateUserNameInPassword(pwd string, sessionVars *variable.SessionVars) (bool, error) {
	currentUser := sessionVars.User
	globalVars := sessionVars.GlobalVarsAccessor
	pwdBytes := hack.Slice(pwd)
	if checkUserName, err := globalVars.GetGlobalSysVar(variable.ValidatePasswordCheckUserName); err != nil {
		return false, err
	} else if currentUser != nil && variable.TiDBOptOn(checkUserName) {
		for _, username := range []string{currentUser.AuthUsername, currentUser.Username} {
			usernameBytes := hack.Slice(username)
			userNameLen := len(usernameBytes)
			if userNameLen == 0 {
				continue
			}
			if bytes.Contains(pwdBytes, usernameBytes) {
				return false, nil
			}
			usernameReversedBytes := make([]byte, userNameLen)
			for i := range usernameBytes {
				usernameReversedBytes[i] = usernameBytes[userNameLen-1-i]
			}
			if bytes.Contains(pwdBytes, usernameReversedBytes) {
				return false, nil
			}
		}
	}
	return true, nil
}

func ValidateLow(pwd string, globalVars *variable.GlobalVarAccessor) (bool, error) {
	if validateLengthStr, err := (*globalVars).GetGlobalSysVar(variable.ValidatePasswordLength); err != nil {
		return false, err
	} else if validateLength, err := strconv.ParseInt(validateLengthStr, 10, 64); err != nil {
		return false, err
	} else if (int64)(len([]rune(pwd))) < validateLength {
		return false, nil
	}
	return true, nil
}

func ValidatePassword(sessionVars *variable.SessionVars, pwd string) error {
	globalVars := sessionVars.GlobalVarsAccessor

	runes := []rune(pwd)
	validatePolicy, err := globalVars.GetGlobalSysVar(variable.ValidatePasswordPolicy)
	if err != nil {
		return err
	}
	if ok, err := ValidateUserNameInPassword(pwd, sessionVars); err != nil {
		return err
	} else if !ok {
		return ErrNotValidPassword.GenWithStack("Password Contains (Reversed) User Name")
	}
	if ok, err := ValidateLow(pwd, &globalVars); err != nil {
		return err
	} else if !ok {
		return ErrNotValidPassword.GenWithStack("Require Password Length")
	}

	// LOW
	if validatePolicy == "LOW" {
		return nil
	}

	// MEDIUM
	var lowerCaseCount, upperCaseCount, numberCount, specialCharCount int64
	for _, r := range runes {
		if unicode.IsUpper(r) {
			upperCaseCount++
		} else if unicode.IsLower(r) {
			lowerCaseCount++
		} else if unicode.IsDigit(r) {
			numberCount++
		} else {
			specialCharCount++
		}
	}
	if mixedCaseCountStr, err := globalVars.GetGlobalSysVar(variable.ValidatePasswordMixedCaseCount); err != nil {
		return err
	} else if mixedCaseCount, err := strconv.ParseInt(mixedCaseCountStr, 10, 64); err != nil {
		return err
	} else if lowerCaseCount < mixedCaseCount {
		return ErrNotValidPassword.GenWithStack("Require Password Lowercase Count: %d", mixedCaseCount)
	} else if upperCaseCount < mixedCaseCount {
		return ErrNotValidPassword.GenWithStack("Require Password Uppercase Count: %d", mixedCaseCount)
	}
	if requireNumberCountStr, err := globalVars.GetGlobalSysVar(variable.ValidatePasswordNumberCount); err != nil {
		return err
	} else if requireNumberCount, err := strconv.ParseInt(requireNumberCountStr, 10, 64); err != nil {
		return err
	} else if numberCount < requireNumberCount {
		return ErrNotValidPassword.GenWithStack("Require Password Digit Count: %d", requireNumberCount)
	}
	if requireSpecialCharCountStr, err := globalVars.GetGlobalSysVar(variable.ValidatePasswordSpecialCharCount); err != nil {
		return err
	} else if requireSpecialCharCount, err := strconv.ParseInt(requireSpecialCharCountStr, 10, 64); err != nil {
		return err
	} else if specialCharCount < requireSpecialCharCount {
		return ErrNotValidPassword.GenWithStack("Require Password Non-alphanumeric Count: %d", requireSpecialCharCount)
	}
	if validatePolicy == "MEDIUM" {
		return nil
	}

	// STRONG
	if !validateDictionaryPassword(pwd) {
		return ErrNotValidPassword.GenWithStack("Password contains word in the dictionary")
	}
	return nil
}
