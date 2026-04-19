// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package kms

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/smithy-go"
	pErrors "github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

const (
	// need to keep it exact same as in TiKV ENCRYPTION_VENDOR_NAME_AWS_KMS
	EncryptionVendorNameAwsKms = "AWS"
)

type AwsKms struct {
	client       *kms.Client
	currentKeyID string
	region       string
	endpoint     string
}

func NewAwsKms(masterKeyConfig *encryptionpb.MasterKeyKms) (*AwsKms, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(masterKeyConfig.Region),
	)
	if err != nil {
		return nil, pErrors.Annotate(err, "failed to load AWS config")
	}

	// set custom endpoint if provided
	if masterKeyConfig.Endpoint != "" {
		cfg.BaseEndpoint = aws.String(masterKeyConfig.Endpoint)
	}

	// only use static credentials if both access key and secret key are provided
	if masterKeyConfig.AwsKms != nil &&
		masterKeyConfig.AwsKms.AccessKey != "" &&
		masterKeyConfig.AwsKms.SecretAccessKey != "" {
		cfg.Credentials = credentials.NewStaticCredentialsProvider(
			masterKeyConfig.AwsKms.AccessKey,
			masterKeyConfig.AwsKms.SecretAccessKey,
			"",
		)
	}

	return &AwsKms{
		client:       kms.NewFromConfig(cfg),
		currentKeyID: masterKeyConfig.KeyId,
		region:       masterKeyConfig.Region,
		endpoint:     masterKeyConfig.Endpoint,
	}, nil
}

func (a *AwsKms) Name() string {
	return EncryptionVendorNameAwsKms
}

func (a *AwsKms) DecryptDataKey(ctx context.Context, dataKey []byte) ([]byte, error) {
	input := &kms.DecryptInput{
		CiphertextBlob: dataKey,
		KeyId:          aws.String(a.currentKeyID),
	}

	result, err := a.client.Decrypt(ctx, input)
	if err != nil {
		return nil, classifyDecryptError(err)
	}

	return result.Plaintext, nil
}

func (a *AwsKms) Close() {
	// don't need to do manual close
}

// classifyDecryptError uses v2 SDK error types
func classifyDecryptError(err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFoundException":
			return pErrors.Annotate(err, "wrong master key")
		case "InvalidKeyUsageException":
			return pErrors.Annotate(err, "wrong master key")
		case "DependencyTimeoutException":
			return pErrors.Annotate(err, "API timeout")
		case "KMSInternalException":
			return pErrors.Annotate(err, "API internal error")
		}
	}

	// also check for specific v2 error types
	var notFoundErr *types.NotFoundException
	var invalidKeyErr *types.InvalidKeyUsageException
	var timeoutErr *types.DependencyTimeoutException
	var internalErr *types.KMSInternalException

	switch {
	case errors.As(err, &notFoundErr), errors.As(err, &invalidKeyErr):
		return pErrors.Annotate(err, "wrong master key")
	case errors.As(err, &timeoutErr):
		return pErrors.Annotate(err, "API timeout")
	case errors.As(err, &internalErr):
		return pErrors.Annotate(err, "API internal error")
	default:
		return pErrors.Annotate(err, "KMS error")
	}
}
