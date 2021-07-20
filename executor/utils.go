// Copyright 2021 PingCAP, Inc.
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

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
)

// SetFromString constructs a slice of strings from a comma separated string.
// It is assumed that there is no duplicated entry. You could use addToSet to maintain this property.
// It is exported for tests. I HOPE YOU KNOW WHAT YOU ARE DOING.
func SetFromString(value string) []string {
	if len(value) == 0 {
		return nil
	}
	return strings.Split(value, ",")
}

func setToString(set []string) string {
	return strings.Join(set, ",")
}

// addToSet add a value to the set, e.g:
// addToSet("Select,Insert,Update", "Update") returns "Select,Insert,Update".
func addToSet(set []string, value string) []string {
	for _, v := range set {
		if v == value {
			return set
		}
	}
	return append(set, value)
}

// deleteFromSet delete the value from the set, e.g:
// deleteFromSet("Select,Insert,Update", "Update") returns "Select,Insert".
func deleteFromSet(set []string, value string) []string {
	for i, v := range set {
		if v == value {
			copy(set[i:], set[i+1:])
			return set[:len(set)-1]
		}
	}
	return set
}

type sqlDigestTextRetriever struct {
	sqlDigestsMap map[string]string
}

func (r *sqlDigestTextRetriever) runQuery(ctx context.Context, exec sqlexec.RestrictedSQLExecutor, queryGlobal bool, inValues []interface{}) (map[string]string, error) {
	stmt := "select digest, digest_text from information_schema.statements_summary union distinct " +
		"select digest, digest_text from information_schema.statements_summary_history"
	if queryGlobal {
		stmt = "select digest, digest_text from information_schema.cluster_statements_summary union distinct " +
			"select digest, digest_text from information_schema.cluster_statements_summary_history"
	}

	if len(inValues) > 0 {
		stmt += " where digest in (" + strings.Repeat("%?,", len(inValues)-1) + "%?)"
	}
	stmtNode, err := exec.ParseWithParams(ctx, stmt, inValues...)
	if err != nil {
		return nil, err
	}
	rows, _, err := exec.ExecRestrictedStmt(ctx, stmtNode)
	if err != nil {
		return nil, err
	}

	res := make(map[string]string, len(rows))
	for _, row := range rows {
		res[row.GetString(0)] = row.GetString(1)
	}
	return res, nil
}

func (r *sqlDigestTextRetriever) updateWithQueryResult(queryResult map[string]string) {
	for digest, text := range r.sqlDigestsMap {
		if len(text) > 0 {
			// The text of this digest is already known
			continue
		}
		sqlText, ok := queryResult[digest]
		if ok {
			r.sqlDigestsMap[digest] = sqlText
		}
	}
}

func (r *sqlDigestTextRetriever) retrieveLocal(ctx context.Context, sctx sessionctx.Context) error {
	const fetchAllLimit = 512

	if len(r.sqlDigestsMap) == 0 {
		return nil
	}

	exec, ok := sctx.(sqlexec.RestrictedSQLExecutor)
	if !ok {
		return errors.New("restricted sql can't be executed in this context")
	}

	var queryResult map[string]string
	if len(r.sqlDigestsMap) <= fetchAllLimit {
		inValues := make([]interface{}, 0, len(r.sqlDigestsMap))
		for key := range r.sqlDigestsMap {
			inValues = append(inValues, key)
		}
		var err error
		queryResult, err = r.runQuery(ctx, exec, false, inValues)
		if err != nil {
			return errors.Trace(err)
		}

		if len(queryResult) == len(r.sqlDigestsMap) {
			r.sqlDigestsMap = queryResult
			return nil
		}
	} else {
		var err error
		queryResult, err = r.runQuery(ctx, exec, false, nil)
		if err != nil {
			return errors.Trace(err)
		}
	}

	r.updateWithQueryResult(queryResult)
	return nil
}

func (r *sqlDigestTextRetriever) retrieveGlobal(ctx context.Context, sctx sessionctx.Context) error {
	err := r.retrieveLocal(ctx, sctx)
	if err != nil {
		return errors.Trace(err)
	}

	var unknownDigests []interface{}
	for k, v := range r.sqlDigestsMap {
		if len(v) == 0 {
			unknownDigests = append(unknownDigests, k)
		}
	}

	if len(unknownDigests) == 0 {
		return nil
	}

	const fetchAllLimit = 512
	exec, ok := sctx.(sqlexec.RestrictedSQLExecutor)
	if !ok {
		return errors.New("restricted sql can't be executed in this context")
	}

	var queryResult map[string]string
	if len(r.sqlDigestsMap) <= fetchAllLimit {
		queryResult, err = r.runQuery(ctx, exec, true, unknownDigests)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		queryResult, err = r.runQuery(ctx, exec, true, nil)
		if err != nil {
			return errors.Trace(err)
		}
	}

	r.updateWithQueryResult(queryResult)
	return nil
}

type batchRetrieverHelper struct {
	retrieved    bool
	retrievedIdx int
	batchSize    int
	totalRows    int
}

func (b *batchRetrieverHelper) nextBatch(retrieveRange func(start, end int) error) error {
	if b.retrieved {
		return nil
	}
	if b.retrievedIdx >= b.totalRows {
		b.retrieved = true
		return nil
	}
	start := b.retrievedIdx
	end := b.retrievedIdx + b.batchSize
	if end > b.totalRows {
		end = b.totalRows
	}

	err := retrieveRange(start, end)
	if err != nil {
		b.retrieved = true
		return err
	}
	b.retrievedIdx = end
	if b.retrievedIdx == b.totalRows {
		b.retrieved = true
	}
	return nil
}
