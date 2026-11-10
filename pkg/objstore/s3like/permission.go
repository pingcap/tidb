// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3like

import (
	"context"
	goerrors "errors"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// CheckPermissions checks whether the client has the given permissions.
func CheckPermissions(ctx context.Context, cli PrefixClient, perms []storeapi.Permission) error {
	for _, perm := range perms {
		switch perm {
		case storeapi.AccessBuckets:
			if err := cli.CheckBucketExistence(ctx); err != nil {
				return errors.Annotatef(err, "permission %s", perm)
			}
		case storeapi.ListObjects:
			if err := cli.CheckListObjects(ctx); err != nil {
				return errors.Annotatef(err, "permission %s", perm)
			}
		case storeapi.GetObject:
			if err := cli.CheckGetObject(ctx); err != nil {
				return errors.Annotatef(err, "permission %s", perm)
			}
		case storeapi.PutAndDeleteObject:
			if err := cli.CheckPutAndDeleteObject(ctx); err != nil {
				return errors.Annotatef(err, "permission %s", perm)
			}
		default:
			return goerrors.New("unknown permission: " + string(perm))
		}
	}
	return nil
}
