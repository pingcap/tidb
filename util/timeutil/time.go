// Copyright 2017 PingCAP, Inc.
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

package timeutil

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

// init initializes `locCache`.
func init() {
	LocCache = &locCache{}
	LocCache.locMap = make(map[string]*time.Location)
}

// LocCache is a simple cache policy to improve the performance of 'time.LoadLocation'.
var LocCache *locCache
var localStr string
var localOnce sync.Once
var zoneSources = []string{
	"/usr/share/zoneinfo/",
	"/usr/share/lib/zoneinfo/",
	"/usr/lib/locale/TZ/",
}

// locCache is a simple map with lock. It stores all used timezone during the lifetime of tidb instance.
// Talked with Golang team about whether they can have some forms of cache policy available for programmer,
// they suggests that only programmers knows which one is best for their use case.
// For detail, please refer to: https://github.com/golang/go/issues/26106
type locCache struct {
	sync.RWMutex
	// locMap stores locations used in past and can be retrieved by a timezone's name.
	locMap map[string]*time.Location
}

func initLocalStr() {
	// consult $TZ to find the time zone to use.
	// no $TZ means use the system default /etc/localtime.
	// $TZ="" means use UTC.
	// $TZ="foo" means use /usr/share/zoneinfo/foo.
	tz, ok := syscall.Getenv("TZ")
	if ok && tz != "" {
		for _, source := range zoneSources {
			if _, err := os.Stat(source + tz); os.IsExist(err) {
				localStr = tz
				break
			}
		}
		// } else if tz == "" {
		// localStr = "UTC"
	} else {
		path, err := filepath.EvalSymlinks("/etc/localtime")
		if err == nil {
			localStr, err = getTZNameFromFileName(path)
			if err == nil {
				return
			}
			log.Errorln(err)
		}
		log.Errorln(err)
	}
	localStr = "System"
}

// getTZNameFromFileName gets IANA timezone name from zoneinfo path.
// TODO It will be refined later. This is just a quick fix.
func getTZNameFromFileName(path string) (string, error) {
	// phase1 only support read /etc/localtime which is a softlink to zoneinfo file
	substr := "zoneinfo"
	if strings.Contains(path, substr) {
		idx := strings.Index(path, substr)
		return string(path[idx+len(substr)+1:]), nil
	}
	return "", errors.New(fmt.Sprintf("path %s is not supported", path))
}

// Local returns time.Local's IANA timezone name.
func Local() *time.Location {
	localOnce.Do(initLocalStr)
	fmt.Printf("local time zone name is %s\n", localStr)
	loc, err := LoadLocation(localStr)
	if err != nil {
		return time.Local
	}
	return loc
}

// getLoc first trying to load location from a cache map. If nothing found in such map, then call
// `time.LocadLocation` to get a timezone location. After trying both way, an error wil be returned
//  if valid Location is not found.
func (lm *locCache) getLoc(name string) (*time.Location, error) {
	if name == "System" {
		return time.Local, nil
	}
	lm.RLock()
	if v, ok := lm.locMap[name]; ok {
		lm.RUnlock()
		return v, nil
	}

	if loc, err := time.LoadLocation(name); err == nil {
		lm.RUnlock()
		lm.Lock()
		lm.locMap[name] = loc
		lm.Unlock()
		return loc, nil
	}

	lm.RUnlock()
	return nil, fmt.Errorf("invalid name for timezone %s", name)
}

// LoadLocation loads time.Location by IANA timezone time.
func LoadLocation(name string) (*time.Location, error) {
	return LocCache.getLoc(name)
}
