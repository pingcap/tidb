// Copyright 2020 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
)

var ErrInvalidCommand = errors.New("Found line beginning with -- that didn't contain a valid mysqltest command, check your syntax or use # if you intended to write comment")

// Different query command type
const (
	Q_CONNECTION = iota + 1
	Q_QUERY
	Q_CONNECT
	Q_SLEEP
	Q_REAL_SLEEP
	Q_INC
	Q_DEC
	Q_SOURCE
	Q_DISCONNECT
	Q_LET
	Q_ECHO
	Q_WHILE
	Q_END_BLOCK
	Q_SYSTEM
	Q_RESULT
	Q_REQUIRE
	Q_SAVE_MASTER_POS
	Q_SYNC_WITH_MASTER
	Q_SYNC_SLAVE_WITH_MASTER
	Q_ERROR
	Q_SEND
	Q_REAP
	Q_DIRTY_CLOSE
	Q_REPLACE
	Q_REPLACE_COLUMN
	Q_PING
	Q_EVAL
	Q_EVAL_RESULT
	Q_ENABLE_QUERY_LOG
	Q_DISABLE_QUERY_LOG
	Q_ENABLE_RESULT_LOG
	Q_DISABLE_RESULT_LOG
	Q_ENABLE_CONNECT_LOG
	Q_DISABLE_CONNECT_LOG
	Q_WAIT_FOR_SLAVE_TO_STOP
	Q_ENABLE_WARNINGS
	Q_DISABLE_WARNINGS
	Q_ENABLE_INFO
	Q_DISABLE_INFO
	Q_ENABLE_SESSION_TRACK_INFO
	Q_DISABLE_SESSION_TRACK_INFO
	Q_ENABLE_METADATA
	Q_DISABLE_METADATA
	Q_EXEC
	Q_EXECW
	Q_DELIMITER
	Q_DISABLE_ABORT_ON_ERROR
	Q_ENABLE_ABORT_ON_ERROR
	Q_DISPLAY_VERTICAL_RESULTS
	Q_DISPLAY_HORIZONTAL_RESULTS
	Q_QUERY_VERTICAL
	Q_QUERY_HORIZONTAL
	Q_SORTED_RESULT
	Q_LOWERCASE
	Q_START_TIMER
	Q_END_TIMER
	Q_CHARACTER_SET
	Q_DISABLE_PS_PROTOCOL
	Q_ENABLE_PS_PROTOCOL
	Q_DISABLE_RECONNECT
	Q_ENABLE_RECONNECT
	Q_IF
	Q_DISABLE_PARSING
	Q_ENABLE_PARSING
	Q_REPLACE_REGEX
	Q_REPLACE_NUMERIC_ROUND
	Q_REMOVE_FILE
	Q_FILE_EXIST
	Q_WRITE_FILE
	Q_COPY_FILE
	Q_PERL
	Q_DIE
	Q_EXIT
	Q_SKIP
	Q_CHMOD_FILE
	Q_APPEND_FILE
	Q_CAT_FILE
	Q_DIFF_FILES
	Q_SEND_QUIT
	Q_CHANGE_USER
	Q_MKDIR
	Q_RMDIR
	Q_LIST_FILES
	Q_LIST_FILES_WRITE_FILE
	Q_LIST_FILES_APPEND_FILE
	Q_SEND_SHUTDOWN
	Q_SHUTDOWN_SERVER
	Q_RESULT_FORMAT_VERSION
	Q_MOVE_FILE
	Q_REMOVE_FILES_WILDCARD
	Q_SEND_EVAL
	Q_OUTPUT /* redirect output to a file */
	Q_RESET_CONNECTION
	Q_SINGLE_QUERY
	Q_BEGIN_CONCURRENT
	Q_END_CONCURRENT
	Q_UNKNOWN /* Unknown command.   */
	Q_COMMENT /* Comments, ignored. */
	Q_COMMENT_WITH_COMMAND
	Q_EMPTY_LINE
)

// ParseQueries parses an array of string into an array of query object.
// Note: a query statement may reside in several lines.
func ParseQueries(qs ...query) ([]query, error) {
	queries := make([]query, 0, len(qs))
	for _, rs := range qs {
		realS := rs.Query
		s := rs.Query
		q := query{}
		q.tp = Q_UNKNOWN
		q.Line = rs.Line
		// a valid query's length should be at least 3.
		if len(s) < 3 {
			continue
		}
		// we will skip #comment and line with zero characters here
		if s[0] == '#' {
			q.tp = Q_COMMENT
		} else if s[0:2] == "--" {
			q.tp = Q_COMMENT_WITH_COMMAND
			if s[2] == ' ' {
				s = s[3:]
			} else {
				s = s[2:]
			}
		} else if s[0] == '\n' {
			q.tp = Q_EMPTY_LINE
		}

		if q.tp != Q_COMMENT {
			// Calculate first word length(the command), terminated
			// by 'space' , '(' or 'delimiter'
			var i int
			for i = 0; i < len(s) && s[i] != '(' && s[i] != ' ' && s[i] != ';' && s[i] != '\n'; i++ {
			}
			if i > 0 {
				q.firstWord = s[:i]
			}
			s = s[i:]

			q.Query = s
			if q.tp == Q_UNKNOWN || q.tp == Q_COMMENT_WITH_COMMAND {
				if err := q.getQueryType(realS); err != nil {
					return nil, err
				}
			}
		}

		queries = append(queries, q)
	}
	return queries, nil
}

// for a single query, it has some prefix. Prefix mapps to a query type.
// e.g query_vertical maps to Q_QUERY_VERTICAL
func (q *query) getQueryType(qu string) error {
	tp := findType(q.firstWord)
	if tp > 0 {
		if tp == Q_ECHO || tp == Q_SORTED_RESULT {
			q.Query = strings.TrimSpace(q.Query)
		}
		q.tp = tp
	} else {
		// No mysqltest command matched
		if q.tp != Q_COMMENT_WITH_COMMAND {
			// A query that will sent to tidb
			q.Query = qu
			q.tp = Q_QUERY
		} else {
			return ErrInvalidCommand
		}
	}
	return nil
}
