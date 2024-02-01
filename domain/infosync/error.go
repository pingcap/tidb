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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package infosync

import (
<<<<<<< HEAD:domain/infosync/error.go
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/dbterror"
=======
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/util/dbterror"
>>>>>>> 5bc70a9f6c2 (errno: add test to check every errcode has its message (#49774)):pkg/domain/infosync/error.go
)

var (
	// ErrHTTPServiceError means we got a http response with a status code which is not '2xx'
	ErrHTTPServiceError = dbterror.ClassDomain.NewStd(errno.ErrHTTPServiceError)
)
