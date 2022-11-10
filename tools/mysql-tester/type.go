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
)

var commandMap = map[string]int{
	"connection":                 Q_CONNECTION,
	"query":                      Q_QUERY,
	"connect":                    Q_CONNECT,
	"sleep":                      Q_SLEEP,
	"real_sleep":                 Q_REAL_SLEEP,
	"inc":                        Q_INC,
	"dec":                        Q_DEC,
	"source":                     Q_SOURCE,
	"disconnect":                 Q_DISCONNECT,
	"let":                        Q_LET,
	"echo":                       Q_ECHO,
	"while":                      Q_WHILE,
	"end":                        Q_END_BLOCK,
	"system":                     Q_SYSTEM,
	"result":                     Q_RESULT,
	"require":                    Q_REQUIRE,
	"save_master_pos":            Q_SAVE_MASTER_POS,
	"sync_with_master":           Q_SYNC_WITH_MASTER,
	"sync_slave_with_master":     Q_SYNC_SLAVE_WITH_MASTER,
	"error":                      Q_ERROR,
	"send":                       Q_SEND,
	"reap":                       Q_REAP,
	"dirty_close":                Q_DIRTY_CLOSE,
	"replace_result":             Q_REPLACE,
	"replace_column":             Q_REPLACE_COLUMN,
	"ping":                       Q_PING,
	"eval":                       Q_EVAL,
	"eval_result":                Q_EVAL_RESULT,
	"enable_query_log":           Q_ENABLE_QUERY_LOG,
	"disable_query_log":          Q_DISABLE_QUERY_LOG,
	"enable_result_log":          Q_ENABLE_RESULT_LOG,
	"disable_result_log":         Q_DISABLE_RESULT_LOG,
	"enable_connect_log":         Q_ENABLE_CONNECT_LOG,
	"disable_connect_log":        Q_DISABLE_CONNECT_LOG,
	"wait_for_slave_to_stop":     Q_WAIT_FOR_SLAVE_TO_STOP,
	"enable_warnings":            Q_ENABLE_WARNINGS,
	"disable_warnings":           Q_DISABLE_WARNINGS,
	"enable_info":                Q_ENABLE_INFO,
	"disable_info":               Q_DISABLE_INFO,
	"enable_session_track_info":  Q_ENABLE_SESSION_TRACK_INFO,
	"disable_session_track_info": Q_DISABLE_SESSION_TRACK_INFO,
	"enable_metadata":            Q_ENABLE_METADATA,
	"disable_metadata":           Q_DISABLE_METADATA,
	"exec":                       Q_EXEC,
	"execw":                      Q_EXECW,
	"delimiter":                  Q_DELIMITER,
	"disable_abort_on_error":     Q_DISABLE_ABORT_ON_ERROR,
	"enable_abort_on_error":      Q_ENABLE_ABORT_ON_ERROR,
	"vertical_results":           Q_DISPLAY_VERTICAL_RESULTS,
	"horizontal_results":         Q_DISPLAY_HORIZONTAL_RESULTS,
	"query_vertical":             Q_QUERY_VERTICAL,
	"query_horizontal":           Q_QUERY_HORIZONTAL,
	"sorted_result":              Q_SORTED_RESULT,
	"lowercase_result":           Q_LOWERCASE,
	"start_timer":                Q_START_TIMER,
	"end_timer":                  Q_END_TIMER,
	"character_set":              Q_CHARACTER_SET,
	"disable_ps_protocol":        Q_DISABLE_PS_PROTOCOL,
	"enable_ps_protocol":         Q_ENABLE_PS_PROTOCOL,
	"disable_reconnect":          Q_DISABLE_RECONNECT,
	"enable_reconnect":           Q_ENABLE_RECONNECT,
	"if":                         Q_IF,
	"disable_parsing":            Q_DISABLE_PARSING,
	"enable_parsing":             Q_ENABLE_PARSING,
	"replace_regex":              Q_REPLACE_REGEX,
	"replace_numeric_round":      Q_REPLACE_NUMERIC_ROUND,
	"remove_file":                Q_REMOVE_FILE,
	"file_exists":                Q_FILE_EXIST,
	"write_file":                 Q_WRITE_FILE,
	"copy_file":                  Q_COPY_FILE,
	"perl":                       Q_PERL,
	"die":                        Q_DIE,
	"exit":                       Q_EXIT,
	"skip":                       Q_SKIP,
	"chmod":                      Q_CHMOD_FILE,
	"append_file":                Q_APPEND_FILE,
	"cat_file":                   Q_CAT_FILE,
	"diff_files":                 Q_DIFF_FILES,
	"send_quit":                  Q_SEND_QUIT,
	"change_user":                Q_CHANGE_USER,
	"mkdir":                      Q_MKDIR,
	"rmdir":                      Q_RMDIR,
	"list_files":                 Q_LIST_FILES,
	"list_files_write_file":      Q_LIST_FILES_WRITE_FILE,
	"list_files_append_file":     Q_LIST_FILES_APPEND_FILE,
	"send_shutdown":              Q_SEND_SHUTDOWN,
	"shutdown_server":            Q_SHUTDOWN_SERVER,
	"result_format":              Q_RESULT_FORMAT_VERSION,
	"move_file":                  Q_MOVE_FILE,
	"remove_files_wildcard":      Q_REMOVE_FILES_WILDCARD,
	"send_eval":                  Q_SEND_EVAL,
	"output":                     Q_OUTPUT,
	"reset_connection":           Q_RESET_CONNECTION,
	"single_query":               Q_SINGLE_QUERY,
	"begin_concurrent":           Q_BEGIN_CONCURRENT,
	"end_concurrent":             Q_END_CONCURRENT,
}

func findType(cmdName string) int {
	key := strings.ToLower(cmdName)
	if v, ok := commandMap[key]; ok {
		return v
	}

	return -1
}
