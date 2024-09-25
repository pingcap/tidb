// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package kms

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kms"
	pErrors "github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

const (
	// need to keep it exact same as in TiKV ENCRYPTION_VENDOR_NAME_AWS_KMS
	EncryptionVendorNameAwsKms = "AWS"
)

type AwsKms struct {
	client       *kms.KMS
	currentKeyID string
	region       string
	endpoint     string
}

func NewAwsKms(masterKeyConfig *encryptionpb.MasterKeyKms) (*AwsKms, error) {
	config := &aws.Config{
		Region:   aws.String(masterKeyConfig.Region),
		Endpoint: aws.String(masterKeyConfig.Endpoint),
	}

	// Only use static credentials if both access key and secret key are provided
	if masterKeyConfig.AwsKms != nil &&
		masterKeyConfig.AwsKms.AccessKey != "" &&
		masterKeyConfig.AwsKms.SecretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(
			masterKeyConfig.AwsKms.AccessKey,
			masterKeyConfig.AwsKms.SecretAccessKey,
			"",
		)
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, pErrors.Annotate(err, "failed to create AWS session")
	}

	return &AwsKms{
		client:       kms.New(sess),
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

	result, err := a.client.DecryptWithContext(ctx, input)
	if err != nil {
		return nil, classifyDecryptError(err)
	}

	return result.Plaintext, nil
}

func (a *AwsKms) Close() {
	// don't need to do manual close
}

// Update classifyDecryptError to use v1 SDK error types
func classifyDecryptError(err error) error {
	switch err := err.(type) {
	case *kms.NotFoundException, *kms.InvalidKeyUsageException:
		return pErrors.Annotate(err, "wrong master key")
	case *kms.DependencyTimeoutException:
		return pErrors.Annotate(err, "API timeout")
	case *kms.InternalException:
		return pErrors.Annotate(err, "API internal error")
	default:
		return pErrors.Annotate(err, "KMS error")
	}
}
