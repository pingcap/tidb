// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2016 CoreOS, Inc.
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

package failpoint

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrNoExist represents can not found a failpoint by specified name
	ErrNoExist = fmt.Errorf("failpoint: failpoint does not exist")
	// ErrDisabled represents a failpoint is be disabled
	ErrDisabled = fmt.Errorf("failpoint: failpoint is disabled")

	failpoints struct {
		mu  sync.RWMutex
		reg map[string]*failpoint
	}
)

func init() {
	failpoints.reg = make(map[string]*failpoint)
	if s := os.Getenv("GO_FAILPOINTS"); len(s) > 0 {
		// format is <FAILPOINT>=<TERMS>[;<FAILPOINT>=<TERMS>;...]
		for _, fp := range strings.Split(s, ";") {
			fpTerms := strings.Split(fp, "=")
			if len(fpTerms) != 2 {
				fmt.Printf("bad failpoint %q\n", fp)
				os.Exit(1)
			}
			err := Enable(fpTerms[0], fpTerms[1])
			if err != nil {
				fmt.Printf("bad failpoint %s\n", err)
				os.Exit(1)
			}
		}
	}
	if s := os.Getenv("GO_FAILPOINTS_HTTP"); len(s) > 0 {
		if err := serve(s); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}

// Enable sets a failpoint to a given failpoint description.
func Enable(failpath, inTerms string) error {
	unlock, err := enableAndLock(failpath, inTerms)
	if unlock != nil {
		unlock()
	}
	return err
}

// enableAndLock enables a failpoint and returns a function to unlock it
func enableAndLock(failpath, inTerms string) (func(), error) {
	failpoints.mu.RLock()
	fp := failpoints.reg[failpath]
	failpoints.mu.RUnlock()
	if fp == nil {
		fp = &failpoint{waitChan: make(chan struct{})}
		failpoints.mu.Lock()
		failpoints.reg[failpath] = fp
		failpoints.mu.Unlock()
	}
	t, err := newTerms(failpath, inTerms, fp)
	if err != nil {
		fmt.Printf("failed to enable \"%s=%s\" (%v)\n", failpath, inTerms, err)
		return nil, err
	}
	fp.mu.Lock()
	fp.t = t
	fp.waitChan = make(chan struct{})
	return func() { fp.mu.Unlock() }, nil
}

// Disable stops a failpoint from firing.
func Disable(failpath string) error {
	failpoints.mu.RLock()
	fp := failpoints.reg[failpath]
	failpoints.mu.RUnlock()
	if fp == nil {
		return ErrNoExist
	}
	select {
	case <-fp.waitChan:
		return ErrDisabled
	default:
		close(fp.waitChan)
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if fp.t == nil {
		return ErrDisabled
	}
	fp.t = nil
	return nil
}

// Status gives the current setting for the failpoint
func Status(failpath string) (string, error) {
	failpoints.mu.RLock()
	fp := failpoints.reg[failpath]
	failpoints.mu.RUnlock()
	if fp == nil {
		return "", ErrNoExist
	}
	fp.mu.RLock()
	t := fp.t
	fp.mu.RUnlock()
	if t == nil {
		return "", ErrDisabled
	}
	return t.desc, nil
}

// List returns all the failpoints information
func List() []string {
	failpoints.mu.RLock()
	ret := make([]string, 0, len(failpoints.reg))
	for fp := range failpoints.reg {
		ret = append(ret, fp)
	}
	failpoints.mu.RUnlock()
	sort.Strings(ret)
	return ret
}
