// Copyright 2024 PingCAP, Inc.
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

package preparesnap

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
)

func convertErr(err *errorpb.Error) error {
	if err == nil {
		return nil
	}
	return errors.New(err.Message)
}

func leaseExpired() error {
	return errors.New("the lease has expired")
}

func unsupported() error {
	return errors.New("unsupported operation")
}

func retryLimitExceeded() error {
	return errors.New("the limit of retrying exceeded")
}
