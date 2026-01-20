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

package ossstore

import (
	"context"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
)

// API defines the subset of OSS client methods used by ossstore.
type API interface {
	IsBucketExist(ctx context.Context, bucket string, optFns ...func(*oss.Options)) (bool, error)
	HeadObject(ctx context.Context, request *oss.HeadObjectRequest, optFns ...func(*oss.Options)) (*oss.HeadObjectResult, error)
	GetObject(ctx context.Context, request *oss.GetObjectRequest, optFns ...func(*oss.Options)) (*oss.GetObjectResult, error)
	PutObject(ctx context.Context, request *oss.PutObjectRequest, optFns ...func(*oss.Options)) (*oss.PutObjectResult, error)
	CopyObject(ctx context.Context, request *oss.CopyObjectRequest, optFns ...func(*oss.Options)) (*oss.CopyObjectResult, error)
	DeleteObject(ctx context.Context, request *oss.DeleteObjectRequest, optFns ...func(*oss.Options)) (*oss.DeleteObjectResult, error)
	DeleteMultipleObjects(ctx context.Context, request *oss.DeleteMultipleObjectsRequest, optFns ...func(*oss.Options)) (*oss.DeleteMultipleObjectsResult, error)
	ListObjectsV2(ctx context.Context, request *oss.ListObjectsV2Request, optFns ...func(*oss.Options)) (*oss.ListObjectsV2Result, error)
	InitiateMultipartUpload(ctx context.Context, request *oss.InitiateMultipartUploadRequest, optFns ...func(*oss.Options)) (*oss.InitiateMultipartUploadResult, error)
	UploadPart(ctx context.Context, request *oss.UploadPartRequest, optFns ...func(*oss.Options)) (*oss.UploadPartResult, error)
	CompleteMultipartUpload(ctx context.Context, request *oss.CompleteMultipartUploadRequest, optFns ...func(*oss.Options)) (*oss.CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, request *oss.AbortMultipartUploadRequest, optFns ...func(*oss.Options)) (*oss.AbortMultipartUploadResult, error)
	ListParts(ctx context.Context, request *oss.ListPartsRequest, optFns ...func(*oss.Options)) (*oss.ListPartsResult, error)
}
