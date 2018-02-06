// Copyright 2018 PingCAP, Inc.
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

package main

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
	stats "github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mock"
	log "github.com/sirupsen/logrus"
)

func loadStats(tblInfo *model.TableInfo, path string) (*stats.Table, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jsTable := &stats.JSONTable{}
	err = json.Unmarshal(data, jsTable)
	if err != nil {
		return nil, errors.Trace(err)
	}
	handle := stats.NewHandle(mock.NewContext(), 0)
	return handle.LoadStatsFromJSON(tblInfo, jsTable)
}

type histogram struct {
	stats.Histogram

	index *model.IndexInfo
}

// When the cnt falls in the middle of bucket, we return the idx of lower bound which is an even number.
// When the cnt falls in the end of bucket, we return the upper bound which is odd.
func (h *histogram) getRandomBoundIdx() int {
	cnt := h.Buckets[len(h.Buckets)-1].Count
	randCnt := randInt64(0, cnt)
	for i, bkt := range h.Buckets {
		if bkt.Count >= randCnt {
			if bkt.Count-bkt.Repeat > randCnt {
				return 2 * i
			}
			return 2*i + 1
		}
	}
	return 0
}

func (h *histogram) decodeInt(row *chunk.Row) int64 {
	if h.index == nil {
		return row.GetInt64(0)
	}
	data := row.GetBytes(0)
	_, result, err := codec.DecodeInt(data)
	if err != nil {
		log.Fatal(err)
	}
	return result
}

func (h *histogram) randInt() int64 {
	idx := h.getRandomBoundIdx()
	if idx%2 == 0 {
		lower := h.Bounds.GetRow(idx).GetInt64(0)
		upper := h.Bounds.GetRow(idx + 1).GetInt64(0)
		return randInt64(lower, upper)
	}
	return h.Bounds.GetRow(idx).GetInt64(0)
}

func getValidPrefix(lower, upper string) string {
	prefixLen := 0
	for i := range lower {
		if i >= len(upper) {
			log.Fatal("lower %s is larger than upper %s", lower, upper)
		}
		if lower[i] != upper[i] {
			randCh := uint8(rand.Intn(int(upper[i]-lower[i]))) + lower[i]
			newBytes := make([]byte, i, i+1)
			copy(newBytes, lower[:i])
			newBytes = append(newBytes, byte(randCh))
			return string(newBytes)
		}
		prefixLen++
	}
	return lower
}

func (h *histogram) randString(l int) string {
	idx := h.getRandomBoundIdx()
	if idx%2 == 0 {
		lower := h.Bounds.GetRow(idx).GetString(0)
		upper := h.Bounds.GetRow(idx + 1).GetString(0)
		prefix := getValidPrefix(lower, upper)
		restLen := l - len(prefix)
		if restLen > 0 {
			prefix = prefix + randString(restLen)
		}
		log.Warnf("prefix %s", prefix)
		return prefix
	}
	return h.Bounds.GetRow(idx).GetString(0)
}
