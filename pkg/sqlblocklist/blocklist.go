// Copyright 2026 PingCAP, Inc.
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

package sqlblocklist

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

const (
	sqlBlocklistTypeDigest  = "digest"
	sqlBlocklistTypeKeyword = "keyword"
	sqlBlocklistUserAll     = "*"
)

// ruleSet holds digest and keyword rules.
// digestTextSet: normalized SQL strings; keywordRules: each rule is a list of keywords (all must match).
type ruleSet struct {
	digestTextSet map[string]struct{}
	keywordRules  [][]string
}

// data holds the in-memory blocklist for digest and keyword rules.
// rulesByUser is keyed by lowercase username; rulesAllUsers applies to all users.
type data struct {
	rulesAllUsers ruleSet
	rulesByUser   map[string]*ruleSet
}

var cache atomic.Pointer[data]

func init() {
	cache.Store(&data{})
}

// CheckSQLDenied returns an error if the given SQL text matches the blocklist.
// user is the current session user name (login user).
// normalizedSQL is the normalized SQL text; sqlText is the original SQL for keyword matching.
// It is safe to call from the hot path: read-only, no allocations when the blocklist is empty.
func CheckSQLDenied(user, normalizedSQL, sqlText string) error {
	current := cache.Load()
	if current == nil || current.isEmpty() {
		return nil
	}

	var userRules *ruleSet
	if user != "" && len(current.rulesByUser) > 0 {
		userRules = current.rulesByUser[strings.ToLower(user)]
	}

	if hasDigestRules(&current.rulesAllUsers) || hasDigestRules(userRules) {
		if normalizedSQL == "" && sqlText != "" {
			normalizedSQL, _ = parser.NormalizeDigest(sqlText)
		}
		if normalizedSQL != "" {
			if matchDigest(&current.rulesAllUsers, normalizedSQL) || matchDigest(userRules, normalizedSQL) {
				return exeerrors.ErrSQLDeniedByBlocklist.GenWithStackByArgs("digest_text " + normalizedSQL)
			}
		}
	}
	if (hasKeywordRules(&current.rulesAllUsers) || hasKeywordRules(userRules)) && sqlText != "" {
		sqlLower := strings.ToLower(sqlText)
		if rule := matchKeywords(&current.rulesAllUsers, sqlLower); rule != nil {
			return exeerrors.ErrSQLDeniedByBlocklist.GenWithStackByArgs("keywords " + strings.Join(rule, ", "))
		}
		if rule := matchKeywords(userRules, sqlLower); rule != nil {
			return exeerrors.ErrSQLDeniedByBlocklist.GenWithStackByArgs("keywords " + strings.Join(rule, ", "))
		}
	}
	return nil
}

// Load loads the blocklist from mysql.sql_blocklist.
func Load(sctx sessionctx.Context) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	exec := sctx.GetRestrictedSQLExecutor()

	builder := newRuleBuilder()
	if err := loadRules(ctx, exec, "mysql.sql_blocklist", "username", builder); err != nil {
		switch {
		case plannererrors.ErrUnknownColumn.Equal(err) || infoschema.ErrColumnNotExists.Equal(err):
			if err := loadRules(ctx, exec, "mysql.sql_blocklist", "user", builder); err != nil {
				if plannererrors.ErrUnknownColumn.Equal(err) || infoschema.ErrColumnNotExists.Equal(err) {
					if err := loadRules(ctx, exec, "mysql.sql_blocklist", "", builder); err != nil && !isIgnorableLoadErr(err) {
						return err
					}
				} else if !isIgnorableLoadErr(err) {
					return err
				}
			}
		default:
			return err
		}
	}

	cache.Store(builder.build())
	return nil
}

func hasDigestRules(rules *ruleSet) bool {
	return rules != nil && len(rules.digestTextSet) > 0
}

func hasKeywordRules(rules *ruleSet) bool {
	return rules != nil && len(rules.keywordRules) > 0
}

func matchDigest(rules *ruleSet, normalizedSQL string) bool {
	if !hasDigestRules(rules) {
		return false
	}
	_, ok := rules.digestTextSet[normalizedSQL]
	return ok
}

func matchKeywords(rules *ruleSet, sqlLower string) []string {
	if !hasKeywordRules(rules) {
		return nil
	}
	for _, rule := range rules.keywordRules {
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
			return rule
		}
	}
	return nil
}

func (d *data) isEmpty() bool {
	return len(d.rulesAllUsers.digestTextSet) == 0 && len(d.rulesAllUsers.keywordRules) == 0 && len(d.rulesByUser) == 0
}

type ruleBuilder struct {
	allUsers ruleSet
	byUser   map[string]*ruleSet
}

func newRuleBuilder() *ruleBuilder {
	return &ruleBuilder{}
}

func (b *ruleBuilder) getRuleSet(user string) *ruleSet {
	if user == "" || user == sqlBlocklistUserAll {
		return &b.allUsers
	}
	user = strings.ToLower(user)
	if b.byUser == nil {
		b.byUser = make(map[string]*ruleSet)
	}
	rs := b.byUser[user]
	if rs == nil {
		rs = &ruleSet{}
		b.byUser[user] = rs
	}
	return rs
}

func (b *ruleBuilder) addDigest(user, val string) {
	normalized, _ := parser.NormalizeDigest(val)
	if normalized == "" {
		return
	}
	rs := b.getRuleSet(user)
	if rs.digestTextSet == nil {
		rs.digestTextSet = make(map[string]struct{})
	}
	rs.digestTextSet[normalized] = struct{}{}
}

func (b *ruleBuilder) addKeywordRule(user string, kws []string) {
	if len(kws) == 0 {
		return
	}
	rs := b.getRuleSet(user)
	rs.keywordRules = append(rs.keywordRules, kws)
}

func (b *ruleBuilder) build() *data {
	if len(b.allUsers.digestTextSet) == 0 {
		b.allUsers.digestTextSet = nil
	}
	if len(b.allUsers.keywordRules) == 0 {
		b.allUsers.keywordRules = nil
	}
	if len(b.byUser) == 0 {
		b.byUser = nil
	}
	return &data{
		rulesAllUsers: b.allUsers,
		rulesByUser:   b.byUser,
	}
}

func loadRules(ctx context.Context, exec sqlexec.RestrictedSQLExecutor, table, userColumn string, builder *ruleBuilder) error {
	query := "SELECT HIGH_PRIORITY type, value FROM " + table
	if userColumn != "" {
		query = "SELECT HIGH_PRIORITY type, value, `" + userColumn + "` FROM " + table
	}
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, query)
	if err != nil {
		return err
	}
	for _, row := range rows {
		typ := strings.TrimSpace(strings.ToLower(row.GetString(0)))
		val := strings.TrimSpace(row.GetString(1))
		if val == "" {
			continue
		}
		user := sqlBlocklistUserAll
		if userColumn != "" {
			user = strings.TrimSpace(row.GetString(2))
			if user == "" {
				user = sqlBlocklistUserAll
			}
		}
		switch typ {
		case sqlBlocklistTypeDigest:
			builder.addDigest(user, val)
		case sqlBlocklistTypeKeyword:
			parts := strings.Split(val, ",")
			kws := make([]string, 0, len(parts))
			for _, p := range parts {
				if s := strings.TrimSpace(p); s != "" {
					kws = append(kws, strings.ToLower(s))
				}
			}
			builder.addKeywordRule(user, kws)
		}
	}
	return nil
}

func isIgnorableLoadErr(err error) bool {
	return infoschema.ErrTableNotExists.Equal(err)
}
