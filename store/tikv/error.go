// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"github.com/juju/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
)

var (
	// errInnerRetryable if caller can retry this directly then return this error.
	errInnerRetryable = errors.New("try again innerly")
	// errInvalidResponse represents response message is invalid.
	errInvalidResponse = errors.New("invalid response")
	// errBodyMissing response body is missing error
	errBodyMissing = errors.New("response body is missing")
)

// TiDB decides whether to retry transaction by checking if error message contains
// string "try again later" literally.
// In TiClient we use `errors.Annotate(err, txnRetryableMark)` to direct TiDB to
// restart a transaction.
// Note that it should be only used if i) the error occurs inside a transaction
// and ii) the error is not totally unexpected and hopefully will recover soon.
const txnRetryableMark = "[try again later]"

// errMismatch if response mismatches request return error.
func errMismatch(resp *pb.Response, req *pb.Request) error {
	return errors.Errorf("message type mismatches, response[%s] request[%s]",
		resp.GetType(), req.GetType())
}
