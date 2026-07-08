// Copyright 2026 PingCAP, Inc.
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

package s3store

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type gcsS3CompatibleSigner struct {
	signer *v4.Signer
}

func newGCSS3CompatibleSigner() *gcsS3CompatibleSigner {
	return &gcsS3CompatibleSigner{signer: v4.NewSigner()}
}

func (s *gcsS3CompatibleSigner) SignHTTP(
	ctx context.Context,
	credentials aws.Credentials,
	r *http.Request,
	payloadHash string,
	service string,
	region string,
	signingTime time.Time,
	optFns ...func(*v4.SignerOptions),
) error {
	// GCS S3 interoperability rejects signatures that include these SDK-added
	// headers. Keep sending the headers, but exclude them from the canonical
	// request to match AWS SDK v1 behavior.
	savedHeaders := http.Header{}
	for _, key := range []string{"Accept-Encoding", "Amz-Sdk-Invocation-Id", "Amz-Sdk-Request"} {
		if values, ok := r.Header[key]; ok {
			savedHeaders[key] = append([]string(nil), values...)
			r.Header.Del(key)
		}
	}
	err := s.signer.SignHTTP(ctx, credentials, r, payloadHash, service, region, signingTime, optFns...)
	for key, values := range savedHeaders {
		r.Header[key] = values
	}
	return err
}
