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
	"os"
	"path/filepath"
	"strings"
	"sync"

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

// ValidateDictionaryPassword checks if the password contains words in the dictionary.
func ValidateDictionaryPassword(pwd string) bool {
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
