// Go driver for MySQL X Protocol
//
// Copyright 2016 Simon J Mudd.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

/*

states are as shown below:

* if we get an error message (where we expect) we act on it and don't consider it an error
* if we get another type of error we immediately abort after reporting the problem.

queryStateStart
         |
         |        +-------[B/N]------+
         |        |                  |
         V        V                  |
queryStateWaitingForColumnMetaData ------[E/*]-----+
         |                                         |
        [C]       +-------[C/N]------+             |
         |        |                  |             |
         V        V                  |             V
queryStateWaitingRow --------------------[E/*]---->|
         |                                         |
        [F]       +--------[N]-------+             |
         |        |                  |             |
         V        V                  |             V
queryStateWaitingExecuteOk --------------[E/*]---->|
         |                                         |
         V                                         V
queryStateDone                               queryStateError

Events:
[S] Write SQL_STMT_EXECUTE
[B] Receive RESULTSET_COLUMN_META_DATA
[C] Receive RESULTSET_ROW
[E] Receive ERROR
[F] Receive RESULTSET_FETCH_DONE
[N] Receive NOTICE


*/

type queryState int

const (
	queryStateStart                 queryState = iota // not started yet
	queryStateWaitingColumnMetaData                   // query sent waiting for some data
	queryStateWaitingRow                              // query sent waiting for row data
	queryStateWaitingExecuteOk                        // query sent waiting for execute ok
	queryStateDone                                    // query complete (could be error)
	queryStateError                                   // error of some sort
	queryStateUnknown                                 // unknown state
)

var queryStateName map[queryState]string

func init() {
	queryStateName = map[queryState]string{
		queryStateStart:                 "Start",
		queryStateWaitingColumnMetaData: "Waiting for Column Metadata",
		queryStateWaitingRow:            "Waiting for Row",
		queryStateWaitingExecuteOk:      "Waiting for Execute Ok",
		queryStateDone:                  "Completed",
		queryStateError:                 "Error",
		queryStateUnknown:               "Unnown",
	}
}

// return the string version of the state
func (q *queryState) String() string {
	if q == nil {
		return queryStateName[queryStateUnknown]
	}
	return queryStateName[*q]
}

// Finished means we're not waiting for anything whether due to an error or due to completing the state changes
func (q *queryState) Finished() bool {
	return q != nil && (*q == queryStateDone || *q == queryStateError)
}

// CollectingColumnMetaData returns true if we're still collecting column meta data.
func (q *queryState) CollectingColumnMetaData() bool {
	return !q.Finished() && *q != queryStateWaitingRow
}
