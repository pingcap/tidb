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
	"github.com/pingcap/failpoint"
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

// SQLDigestTextRetriever is used to find the normalized SQL statement text by SQL digests in statements_summary table.
// It's exported for test purposes.
type SQLDigestTextRetriever struct {
	// SQLDigestsMap is the place to put the digests that's requested for getting SQL text and also the place to put
	// the query result.
	SQLDigestsMap map[string]string

	// Replace querying for test purposes.
	mockLocalData  map[string]string
	mockGlobalData map[string]string
	// There are two ways for querying information: 1) query specified digests by WHERE IN query, or 2) query all
	// information to avoid the too long WHERE IN clause. If there are more than `fetchAllLimit` digests needs to be
	// queried, the second way will be chosen; otherwise, the first way will be chosen.
	fetchAllLimit int
}

// NewSQLDigestTextRetriever creates a new SQLDigestTextRetriever.
func NewSQLDigestTextRetriever() *SQLDigestTextRetriever {
	return &SQLDigestTextRetriever{
		SQLDigestsMap: make(map[string]string),
		fetchAllLimit: 512,
	}
}

func (r *SQLDigestTextRetriever) runMockQuery(data map[string]string, inValues []interface{}) (map[string]string, error) {
	if len(inValues) == 0 {
		return data, nil
	}
	res := make(map[string]string, len(inValues))
	for _, digest := range inValues {
		if text, ok := data[digest.(string)]; ok {
			res[digest.(string)] = text
		}
	}
	return res, nil
}

// runFetchDigestQuery runs query to the system tables to fetch the kv mapping of SQL digests and normalized SQL texts
// of the given SQL digests, if `inValues` is given, or all these mappings otherwise. If `queryGlobal` is false, it
// queries information_schema.statements_summary and information_schema.statements_summary_history; otherwise, it
// queries the cluster version of these two tables.
func (r *SQLDigestTextRetriever) runFetchDigestQuery(ctx context.Context, sctx sessionctx.Context, queryGlobal bool, inValues []interface{}) (map[string]string, error) {
	// If mock data is set, query the mock data instead of the real statements_summary tables.
	if !queryGlobal && r.mockLocalData != nil {
		return r.runMockQuery(r.mockLocalData, inValues)
	} else if queryGlobal && r.mockGlobalData != nil {
		return r.runMockQuery(r.mockGlobalData, inValues)
	}

	exec, ok := sctx.(sqlexec.RestrictedSQLExecutor)
	if !ok {
		return nil, errors.New("restricted sql can't be executed in this context")
	}

	// Information in statements_summary will be periodically moved to statements_summary_history. Union them together
	// to avoid missing information when statements_summary is just cleared.
	stmt := "select digest, digest_text from information_schema.statements_summary union distinct " +
		"select digest, digest_text from information_schema.statements_summary_history"
	if queryGlobal {
		stmt = "select digest, digest_text from information_schema.cluster_statements_summary union distinct " +
			"select digest, digest_text from information_schema.cluster_statements_summary_history"
	}
	// Add the where clause if `inValues` is specified.
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

func (r *SQLDigestTextRetriever) updateDigestInfo(queryResult map[string]string) {
	for digest, text := range r.SQLDigestsMap {
		if len(text) > 0 {
			// The text of this digest is already known
			continue
		}
		sqlText, ok := queryResult[digest]
		if ok {
			r.SQLDigestsMap[digest] = sqlText
		}
	}
}

// RetrieveLocal tries to retrieve the SQL text of the SQL digests from local information.
func (r *SQLDigestTextRetriever) RetrieveLocal(ctx context.Context, sctx sessionctx.Context) error {
	if len(r.SQLDigestsMap) == 0 {
		return nil
	}

	var queryResult map[string]string
	if len(r.SQLDigestsMap) <= r.fetchAllLimit {
		inValues := make([]interface{}, 0, len(r.SQLDigestsMap))
		for key := range r.SQLDigestsMap {
			inValues = append(inValues, key)
		}
		var err error
		queryResult, err = r.runFetchDigestQuery(ctx, sctx, false, inValues)
		if err != nil {
			return errors.Trace(err)
		}

		if len(queryResult) == len(r.SQLDigestsMap) {
			r.SQLDigestsMap = queryResult
			return nil
		}
	} else {
		var err error
		queryResult, err = r.runFetchDigestQuery(ctx, sctx, false, nil)
		if err != nil {
			return errors.Trace(err)
		}
	}

	r.updateDigestInfo(queryResult)
	return nil
}

// RetrieveGlobal tries to retrieve the SQL text of the SQL digests from the information of the whole cluster.
func (r *SQLDigestTextRetriever) RetrieveGlobal(ctx context.Context, sctx sessionctx.Context) error {
	err := r.RetrieveLocal(ctx, sctx)
	if err != nil {
		return errors.Trace(err)
	}

	failpoint.Inject("sqlDigestRetrieverSkipGlobal", func() {
		failpoint.Return(nil)
	})

	var unknownDigests []interface{}
	for k, v := range r.SQLDigestsMap {
		if len(v) == 0 {
			unknownDigests = append(unknownDigests, k)
		}
	}

	if len(unknownDigests) == 0 {
		return nil
	}

	var queryResult map[string]string
	if len(r.SQLDigestsMap) <= r.fetchAllLimit {
		queryResult, err = r.runFetchDigestQuery(ctx, sctx, true, unknownDigests)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		queryResult, err = r.runFetchDigestQuery(ctx, sctx, true, nil)
		if err != nil {
			return errors.Trace(err)
		}
	}

	r.updateDigestInfo(queryResult)
	return nil
}

// batchRetrieverHelper is a helper for batch returning data with known total rows. This helps implementing memtable
// retrievers of some information_schema tables. Initialize `batchSize` and `totalRows` fields to use it.
type batchRetrieverHelper struct {
	// When retrieved is true, it means retrieving is finished.
	retrieved bool
	// The index that the retrieving process has been done up to (exclusive).
	retrievedIdx int
	batchSize    int
	totalRows    int
}

// nextBatch calculates the index range of the next batch. If there is such a non-empty range, the `retrieveRange` func
// will be invoked and the range [start, end) is passed to it. Returns error if `retrieveRange` returns error.
func (b *batchRetrieverHelper) nextBatch(retrieveRange func(start, end int) error) error {
	if b.retrievedIdx >= b.totalRows {
		b.retrieved = true
	}
	if b.retrieved {
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
