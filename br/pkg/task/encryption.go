// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"fmt"
	"net/url"
	"path"
	"regexp"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
)

const (
	SchemeLocal = "local"
	SchemeAWS   = "aws-kms"
	SchemeAzure = "azure-kms"
	SchemeGCP   = "gcp-kms"

	AWSVendor      = "aws"
	AWSRegion      = "REGION"
	AWSEndpoint    = "ENDPOINT"
	AWSAccessKeyId = "AWS_ACCESS_KEY_ID"
	AWSSecretKey   = "AWS_SECRET_ACCESS_KEY"

	AzureVendor       = "azure"
	AzureTenantID     = "AZURE_TENANT_ID"
	AzureClientID     = "AZURE_CLIENT_ID"
	AzureClientSecret = "AZURE_CLIENT_SECRET"
	AzureVaultName    = "AZURE_VAULT_NAME"

	GCPVendor      = "gcp"
	GCPCredentials = "CREDENTIALS"
)

var (
	awsRegex   = regexp.MustCompile(`^/([^/]+)$`)
	azureRegex = regexp.MustCompile(`^/(.+)$`)
	gcpRegex   = regexp.MustCompile(`^/projects/([^/]+)/locations/([^/]+)/keyRings/([^/]+)/cryptoKeys/([^/]+)/?$`)
)

func validateAndParseMasterKeyString(keyString string) (encryptionpb.MasterKey, error) {
	u, err := url.Parse(keyString)
	if err != nil {
		return encryptionpb.MasterKey{}, errors.Trace(err)
	}

	switch u.Scheme {
	case SchemeLocal:
		return parseLocalDiskConfig(u)
	case SchemeAWS:
		return parseAwsKmsConfig(u)
	case SchemeAzure:
		return parseAzureKmsConfig(u)
	case SchemeGCP:
		return parseGcpKmsConfig(u)
	default:
		return encryptionpb.MasterKey{}, errors.Errorf("unsupported master key type: %s", u.Scheme)
	}
}

func parseLocalDiskConfig(u *url.URL) (encryptionpb.MasterKey, error) {
	if !path.IsAbs(u.Path) {
		return encryptionpb.MasterKey{}, errors.New("local master key path must be absolute")
	}
	return encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_File{
			File: &encryptionpb.MasterKeyFile{
				Path: u.Path,
			},
		},
	}, nil
}

func parseAwsKmsConfig(u *url.URL) (encryptionpb.MasterKey, error) {
	matches := awsRegex.FindStringSubmatch(u.Path)
	if matches == nil {
		return encryptionpb.MasterKey{}, errors.New("invalid AWS KMS key ID format")
	}
	keyID := matches[1]

	q := u.Query()
	region := q.Get(AWSRegion)
	accessKey := q.Get(AWSAccessKeyId)
	secretAccessKey := q.Get(AWSSecretKey)

	if region == "" {
		return encryptionpb.MasterKey{}, errors.New("missing AWS KMS region info")
	}

	var awsKms *encryptionpb.AwsKms
	if accessKey != "" && secretAccessKey != "" {
		awsKms = &encryptionpb.AwsKms{
			AccessKey:       accessKey,
			SecretAccessKey: secretAccessKey,
		}
	}

	return encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_Kms{
			Kms: &encryptionpb.MasterKeyKms{
				Vendor:   AWSVendor,
				KeyId:    keyID,
				Region:   region,
				Endpoint: q.Get(AWSEndpoint), // Optional
				AwsKms:   awsKms,             // Optional, can read from env
			},
		},
	}, nil
}

func parseAzureKmsConfig(u *url.URL) (encryptionpb.MasterKey, error) {
	matches := azureRegex.FindStringSubmatch(u.Path)
	if matches == nil {
		return encryptionpb.MasterKey{}, errors.New("invalid Azure KMS path format")
	}

	keyID := matches[1] // This now captures the entire path as the key ID
	q := u.Query()

	azureKms := &encryptionpb.AzureKms{
		TenantId:     q.Get(AzureTenantID),
		ClientId:     q.Get(AzureClientID),
		ClientSecret: q.Get(AzureClientSecret),
		KeyVaultUrl:  q.Get(AzureVaultName),
	}

	if azureKms.TenantId == "" || azureKms.ClientId == "" || azureKms.ClientSecret == "" || azureKms.KeyVaultUrl == "" {
		return encryptionpb.MasterKey{}, errors.New("missing required Azure KMS parameters")
	}

	return encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_Kms{
			Kms: &encryptionpb.MasterKeyKms{
				Vendor:   AzureVendor,
				KeyId:    keyID,
				AzureKms: azureKms,
			},
		},
	}, nil
}

func parseGcpKmsConfig(u *url.URL) (encryptionpb.MasterKey, error) {
	matches := gcpRegex.FindStringSubmatch(u.Path)
	if matches == nil {
		return encryptionpb.MasterKey{}, errors.New("invalid GCP KMS path format")
	}

	projectID, location, keyRing, keyName := matches[1], matches[2], matches[3], matches[4]
	q := u.Query()

	gcpKms := &encryptionpb.GcpKms{
		Credential: q.Get(GCPCredentials),
	}

	return encryptionpb.MasterKey{
		Backend: &encryptionpb.MasterKey_Kms{
			Kms: &encryptionpb.MasterKeyKms{
				Vendor: GCPVendor,
				KeyId:  fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", projectID, location, keyRing, keyName),
				GcpKms: gcpKms,
			},
		},
	}, nil
}
