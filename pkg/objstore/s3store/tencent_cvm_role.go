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

package s3store

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/log"
	common "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"go.uber.org/zap"
)

const (
	tencentCVMRoleCredentialSource = "TencentCVMRole"
)

type tencentCVMRoleCredentialsProvider struct {
	credential common.CredentialIface
}

func createTencentCVMRoleCred() (aws.CredentialsProvider, error) {
	credential, err := common.DefaultCvmRoleProvider().GetCredential()
	if err != nil {
		// Keep the AWS default credential chain as a fallback, matching the
		// existing Alibaba RAM role behavior in createOssRAMCred.
		log.Warn("failed to get Tencent CVM role credential", zap.Error(err))
		return nil, nil
	}
	return &tencentCVMRoleCredentialsProvider{credential: credential}, nil
}

func (p *tencentCVMRoleCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	if err := ctx.Err(); err != nil {
		return aws.Credentials{}, err
	}

	accessKeyID, secretAccessKey, sessionToken := p.credential.GetCredential()
	if accessKeyID == "" || secretAccessKey == "" || sessionToken == "" {
		return aws.Credentials{}, errors.New("tencent CVM role returned incomplete credentials")
	}

	// CvmRoleCredential owns the expiration state and refreshes itself five
	// minutes before ExpiredTime. CredentialIface does not expose that time, so
	// make the converted value immediately expire in any outer AWS credential
	// cache. The next AWS retrieval will call GetCredential again, allowing the
	// Tencent SDK to refresh when necessary without re-fetching metadata early.
	return aws.Credentials{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		SessionToken:    sessionToken,
		Source:          tencentCVMRoleCredentialSource,
		CanExpire:       true,
		Expires:         time.Now(),
	}, nil
}
