// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"net/url"
	"testing"

	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/stretchr/testify/assert"
)

func TestParseLocalDiskConfig(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    encryptionpb.MasterKey
		expectError bool
	}{
		{
			name:        "Valid local path",
			input:       "local:///path/to/key",
			expected:    encryptionpb.MasterKey{Backend: &encryptionpb.MasterKey_File{File: &encryptionpb.MasterKeyFile{Path: "/path/to/key"}}},
			expectError: false,
		},
		{
			name:        "Invalid local path",
			input:       "local://relative/path",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := url.Parse(tt.input)
			result, err := parseLocalDiskConfig(u)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParseAwsKmsConfig(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    encryptionpb.MasterKey
		expectError bool
	}{
		{
			name:  "Valid AWS config",
			input: "aws-kms:///key-id?AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE&AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY&REGION=us-west-2",
			expected: encryptionpb.MasterKey{
				Backend: &encryptionpb.MasterKey_Kms{
					Kms: &encryptionpb.MasterKeyKms{
						Vendor: "aws",
						KeyId:  "key-id",
						Region: "us-west-2",
					},
				},
			},
			expectError: false,
		},
		{
			name:        "Missing key ID",
			input:       "aws-kms:///?AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE&AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY&REGION=us-west-2",
			expectError: true,
		},
		{
			name:        "Missing required parameter",
			input:       "aws-kms:///key-id?AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE&REGION=us-west-2",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := url.Parse(tt.input)
			result, err := parseAwsKmsConfig(u)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParseAzureKmsConfig(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    encryptionpb.MasterKey
		expectError bool
	}{
		{
			name:  "Valid Azure config",
			input: "azure-kms:///key-name/key-version?AZURE_TENANT_ID=tenant-id&AZURE_CLIENT_ID=client-id&AZURE_CLIENT_SECRET=client-secret&AZURE_VAULT_NAME=vault-name",
			expected: encryptionpb.MasterKey{
				Backend: &encryptionpb.MasterKey_Kms{
					Kms: &encryptionpb.MasterKeyKms{
						Vendor: "azure",
						KeyId:  "key-name/key-version",
						AzureKms: &encryptionpb.AzureKms{
							TenantId:     "tenant-id",
							ClientId:     "client-id",
							ClientSecret: "client-secret",
							KeyVaultUrl:  "vault-name",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:        "Missing required parameter",
			input:       "azure-kms:///key-name/key-version?AZURE_TENANT_ID=tenant-id&AZURE_CLIENT_ID=client-id&AZURE_VAULT_NAME=vault-name",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := url.Parse(tt.input)
			result, err := parseAzureKmsConfig(u)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestParseGcpKmsConfig(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    encryptionpb.MasterKey
		expectError bool
	}{
		{
			name:  "Valid GCP config",
			input: "gcp-kms:///projects/project-id/locations/global/keyRings/ring-name/cryptoKeys/key-name?CREDENTIALS=credentials",
			expected: encryptionpb.MasterKey{
				Backend: &encryptionpb.MasterKey_Kms{
					Kms: &encryptionpb.MasterKeyKms{
						Vendor: "gcp",
						KeyId:  "projects/project-id/locations/global/keyRings/ring-name/cryptoKeys/key-name",
						GcpKms: &encryptionpb.GcpKms{
							Credential: "credentials",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:        "Invalid path format",
			input:       "gcp-kms:///invalid/path?CREDENTIALS=credentials",
			expectError: true,
		},
		{
			name:        "Missing credentials",
			input:       "gcp-kms:///projects/project-id/locations/global/keyRings/ring-name/cryptoKeys/key-name",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _ := url.Parse(tt.input)
			result, err := parseGcpKmsConfig(u)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
