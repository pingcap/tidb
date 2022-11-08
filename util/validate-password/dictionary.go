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

package validate_password

import (
	"bufio"
	"os"
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

var dictionary = dictionaryImpl{cache: make(map[string]struct{})}

func UpdateDictionaryFile(filePath string) error {
	dictionary.m.Lock()
	defer dictionary.m.Unlock()
	newDictionary := make(map[string]struct{})
	file, err := os.Open(filePath)
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
		if len(line) >= 4 && len(line) <= 100 {
			newDictionary[line] = struct{}{}
		}
	}
	if err := s.Err(); err != nil {
		return err
	}
	dictionary.cache = newDictionary
	return file.Close()
}

func ValidateDictionaryPassword(pwd string) bool {
	dictionary.m.RLock()
	dictionary.m.RUnlock()
	if len(dictionary.cache) == 0 {
		return true
	}
	pwdLen := len(pwd)
	for subStrLen := mathutil.Min(100, pwdLen); subStrLen >= 4; subStrLen-- {
		for subStrPos := 0; subStrPos+subStrLen <= pwdLen; subStrPos++ {
			subStr := pwd[subStrPos : subStrPos+subStrLen]
			if _, ok := dictionary.cache[subStr]; ok {
				return false
			}
		}
	}
	return true
}
