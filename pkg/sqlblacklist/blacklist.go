// Copyright 2025 PingCAP, Inc.
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

package sqlblacklist

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
)

const (
	sqlBlacklistTypeDigest  = "digest"
	sqlBlacklistTypeKeyword = "keyword"
)

// data holds the in-memory blacklist for digest and keyword rules.
// digestTextSet: normalized SQL strings; keywordRules: each rule is a list of keywords (all must match).
type data struct {
	digestTextSet map[string]struct{}
	keywordRules  [][]string
}

var cache atomic.Pointer[data]

func init() {
	cache.Store(&data{})
}

// CheckSQLDenied returns an error if the given SQL text matches the blacklist.
// normalizedSQL is the normalized SQL text; sqlText is the original SQL for keyword matching.
// It is safe to call from the hot path: read-only, no allocations when the blacklist is empty.
func CheckSQLDenied(normalizedSQL, sqlText string) error {
	current := cache.Load()
	if current == nil {
		return nil
	}
	if len(current.digestTextSet) > 0 {
		if normalizedSQL == "" && sqlText != "" {
			normalizedSQL, _ = parser.NormalizeDigest(sqlText)
		}
		if normalizedSQL != "" {
			if _, ok := current.digestTextSet[normalizedSQL]; ok {
				return exeerrors.ErrSQLDeniedByBlacklist.GenWithStackByArgs("digest_text " + normalizedSQL)
			}
		}
	}
	if len(current.keywordRules) > 0 && sqlText != "" {
		sqlLower := strings.ToLower(sqlText)
		for _, rule := range current.keywordRules {
			allMatch := true
			for _, kw := range rule {
				if kw == "" {
					continue
				}
				if !strings.Contains(sqlLower, kw) {
					allMatch = false
					break
				}
			}
			if allMatch {
				return exeerrors.ErrSQLDeniedByBlacklist.GenWithStackByArgs("keywords " + strings.Join(rule, ", "))
			}
		}
	}
	return nil
}

// Load loads the blacklist from mysql.sql_blacklist.
func Load(sctx sessionctx.Context) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	exec := sctx.GetRestrictedSQLExecutor()
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "SELECT HIGH_PRIORITY type, value FROM mysql.sql_blacklist")
	if err != nil {
		return err
	}
	digestTextSet := make(map[string]struct{}, len(rows))
	var keywordRules [][]string
	for _, row := range rows {
		typ := strings.TrimSpace(strings.ToLower(row.GetString(0)))
		val := strings.TrimSpace(row.GetString(1))
		if val == "" {
			continue
		}
		switch typ {
		case sqlBlacklistTypeDigest:
			normalized, _ := parser.NormalizeDigest(val)
			if normalized != "" {
				digestTextSet[normalized] = struct{}{}
			}
		case sqlBlacklistTypeKeyword:
			parts := strings.Split(val, ",")
			kws := make([]string, 0, len(parts))
			for _, p := range parts {
				if s := strings.TrimSpace(p); s != "" {
					kws = append(kws, strings.ToLower(s))
				}
			}
			if len(kws) > 0 {
				keywordRules = append(keywordRules, kws)
			}
		}
	}
	if len(digestTextSet) == 0 {
		digestTextSet = nil
	}
	if len(keywordRules) == 0 {
		keywordRules = nil
	}
	cache.Store(&data{
		digestTextSet: digestTextSet,
		keywordRules:  keywordRules,
	})
	return nil
}
