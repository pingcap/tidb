// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestS3ProfileFlag(t *testing.T) {
	// Test defining S3 flags includes profile flag
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	defineS3Flags(flags)

	// Test that the profile flag was defined
	profileFlag := flags.Lookup("s3.profile")
	require.NotNil(t, profileFlag, "s3.profile flag should be defined")
	require.Equal(t, "", profileFlag.DefValue, "s3.profile flag should have empty default value")
	require.Contains(t, profileFlag.Usage, "AWS profile", "s3.profile flag should mention AWS profile in usage")

	// Test setting the profile flag
	err := flags.Set("s3.profile", "my-test-profile")
	require.NoError(t, err, "Should be able to set s3.profile flag")

	// Test getting the profile flag value
	profileValue, err := flags.GetString("s3.profile")
	require.NoError(t, err, "Should be able to get s3.profile flag value")
	require.Equal(t, "my-test-profile", profileValue, "Profile flag value should match what was set")
}

func TestS3BackendOptionsParseFromFlags(t *testing.T) {
	// Create flag set with S3 flags
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	defineS3Flags(flags)

	// Set various S3 flags including profile
	testCases := []struct {
		flag  string
		value string
	}{
		{"s3.region", "us-west-2"},
		{"s3.endpoint", "https://s3.example.com"},
		{"s3.profile", "production"},
		{"s3.storage-class", "GLACIER"},
		{"s3.provider", "aws"},
		{"s3.role-arn", "arn:aws:iam::123456789012:role/MyRole"},
		{"s3.external-id", "my-external-id"},
	}

	for _, tc := range testCases {
		err := flags.Set(tc.flag, tc.value)
		require.NoError(t, err, "Should be able to set flag %s", tc.flag)
	}

	// Parse flags into S3BackendOptions
	options := &S3BackendOptions{}
	err := options.parseFromFlags(flags)
	require.NoError(t, err, "parseFromFlags should succeed")

	// Verify all values were parsed correctly
	require.Equal(t, "us-west-2", options.Region)
	require.Equal(t, "https://s3.example.com", options.Endpoint)
	require.Equal(t, "production", options.Profile)
	require.Equal(t, "GLACIER", options.StorageClass)
	require.Equal(t, "aws", options.Provider)
	require.Equal(t, "arn:aws:iam::123456789012:role/MyRole", options.RoleARN)
	require.Equal(t, "my-external-id", options.ExternalID)
}

func TestS3BackendOptionsParseFromFlagsProfileEmpty(t *testing.T) {
	// Test with empty profile
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	defineS3Flags(flags)

	// Only set non-profile flags
	err := flags.Set("s3.region", "us-east-1")
	require.NoError(t, err)

	options := &S3BackendOptions{}
	err = options.parseFromFlags(flags)
	require.NoError(t, err)

	require.Equal(t, "us-east-1", options.Region)
	require.Equal(t, "", options.Profile, "Profile should be empty when not set")
}

func TestS3BackendOptionsParseFromFlagsProfileSpecialChars(t *testing.T) {
	// Test profile with special characters
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	defineS3Flags(flags)

	specialProfile := "dev-profile_123"
	err := flags.Set("s3.profile", specialProfile)
	require.NoError(t, err)

	options := &S3BackendOptions{}
	err = options.parseFromFlags(flags)
	require.NoError(t, err)

	require.Equal(t, specialProfile, options.Profile)
}

func TestS3BackendOptionsAWSCLIPrecedence(t *testing.T) {
	// Test AWS CLI precedence behavior: command line flags override profile settings
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	defineS3Flags(flags)

	// Set profile and some explicit overrides
	err := flags.Set("s3.profile", "production")
	require.NoError(t, err)
	err = flags.Set("s3.region", "us-west-2") // Explicit override
	require.NoError(t, err)
	err = flags.Set("s3.endpoint", "https://custom.s3.com") // Explicit override
	require.NoError(t, err)
	// Don't set s3.storage-class - should come from profile (if profile were real)

	options := &S3BackendOptions{}
	err = options.parseFromFlags(flags)
	require.NoError(t, err)

	// Test Apply method respects precedence
	s3Backend := &backuppb.S3{}
	err = options.Apply(s3Backend)
	require.NoError(t, err)

	// Profile should always be set
	require.Equal(t, "production", s3Backend.Profile)

	// Explicitly set flags should override profile
	require.Equal(t, "us-west-2", s3Backend.Region)
	require.Equal(t, "https://custom.s3.com", s3Backend.Endpoint)

	// Non-explicitly set flags should be empty (would come from profile in real AWS session)
	require.Equal(t, "", s3Backend.StorageClass)
	require.Equal(t, "", s3Backend.Provider)
}

func TestS3BackendOptionsNoProfile(t *testing.T) {
	// Test that without profile, all flags work normally (existing behavior)
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	defineS3Flags(flags)

	err := flags.Set("s3.region", "us-east-1")
	require.NoError(t, err)
	err = flags.Set("s3.endpoint", "https://s3.amazonaws.com")
	require.NoError(t, err)
	err = flags.Set("s3.storage-class", "GLACIER")
	require.NoError(t, err)

	options := &S3BackendOptions{}
	err = options.parseFromFlags(flags)
	require.NoError(t, err)

	// Test Apply method without profile
	s3Backend := &backuppb.S3{}
	err = options.Apply(s3Backend)
	require.NoError(t, err)

	// All flags should be applied normally when no profile is used
	require.Equal(t, "", s3Backend.Profile)
	require.Equal(t, "us-east-1", s3Backend.Region)
	require.Equal(t, "https://s3.amazonaws.com", s3Backend.Endpoint)
	require.Equal(t, "GLACIER", s3Backend.StorageClass)
}

func TestS3BackendOptionsProfileOnly(t *testing.T) {
	// Test profile-only mode (no explicit overrides)
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	defineS3Flags(flags)

	err := flags.Set("s3.profile", "development")
	require.NoError(t, err)

	options := &S3BackendOptions{}
	err = options.parseFromFlags(flags)
	require.NoError(t, err)

	// Test Apply method
	s3Backend := &backuppb.S3{}
	err = options.Apply(s3Backend)
	require.NoError(t, err)

	// Only profile should be set, everything else empty (comes from profile)
	require.Equal(t, "development", s3Backend.Profile)
	require.Equal(t, "", s3Backend.Region)   // Would come from profile
	require.Equal(t, "", s3Backend.Endpoint) // Would come from profile
	require.Equal(t, "", s3Backend.StorageClass)
}

func TestS3BackendOptionsPartialOverride(t *testing.T) {
	// Test partial override: some flags override profile, others don't
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	defineS3Flags(flags)

	err := flags.Set("s3.profile", "staging")
	require.NoError(t, err)
	err = flags.Set("s3.region", "eu-west-1") // Override profile region
	require.NoError(t, err)
	// Don't set endpoint - should come from profile
	err = flags.Set("s3.storage-class", "STANDARD_IA") // Override profile storage class
	require.NoError(t, err)

	options := &S3BackendOptions{}
	err = options.parseFromFlags(flags)
	require.NoError(t, err)

	// Test Apply method
	s3Backend := &backuppb.S3{}
	err = options.Apply(s3Backend)
	require.NoError(t, err)

	require.Equal(t, "staging", s3Backend.Profile)
	require.Equal(t, "eu-west-1", s3Backend.Region)         // Explicit override
	require.Equal(t, "", s3Backend.Endpoint)                // From profile (empty in test)
	require.Equal(t, "STANDARD_IA", s3Backend.StorageClass) // Explicit override
}

func TestS3BackendOptionsProfileCredentials(t *testing.T) {
	// Test that when using profile, access key and secret key are optional
	options := &S3BackendOptions{
		Profile: "production",
		Region:  "us-west-2",
	}

	s3Backend := &backuppb.S3{}
	err := options.Apply(s3Backend)
	require.NoError(t, err, "Should not require credentials when using profile")

	require.Equal(t, "production", s3Backend.Profile)
	require.Equal(t, "us-west-2", s3Backend.Region)
	require.Equal(t, "", s3Backend.AccessKey)       // No explicit credentials
	require.Equal(t, "", s3Backend.SecretAccessKey) // No explicit credentials
}

func TestS3BackendOptionsProfileWithExplicitCredentials(t *testing.T) {
	// Test that explicit credentials can override profile credentials
	options := &S3BackendOptions{
		Profile:         "development",
		AccessKey:       "explicit-access-key",
		SecretAccessKey: "explicit-secret-key",
	}

	s3Backend := &backuppb.S3{}
	err := options.Apply(s3Backend)
	require.NoError(t, err)

	require.Equal(t, "development", s3Backend.Profile)
	require.Equal(t, "explicit-access-key", s3Backend.AccessKey)
	require.Equal(t, "explicit-secret-key", s3Backend.SecretAccessKey)
}

func TestS3BackendOptionsNoProfileCredentialValidation(t *testing.T) {
	// Test that without profile, credential validation still works as before

	// Case 1: Both keys provided - should be valid
	options1 := &S3BackendOptions{
		AccessKey:       "test-access-key",
		SecretAccessKey: "test-secret-key",
		Region:          "us-east-1",
	}
	s3Backend1 := &backuppb.S3{}
	err := options1.Apply(s3Backend1)
	require.NoError(t, err, "Should accept both keys when no profile")

	// Case 2: Only access key - should fail
	options2 := &S3BackendOptions{
		AccessKey: "test-access-key",
		// Missing SecretAccessKey
		Region: "us-east-1",
	}
	s3Backend2 := &backuppb.S3{}
	err = options2.Apply(s3Backend2)
	require.Error(t, err, "Should require secret key when access key is provided")
	require.Contains(t, err.Error(), "secret_access_key not found")

	// Case 3: Only secret key - should fail
	options3 := &S3BackendOptions{
		SecretAccessKey: "test-secret-key",
		// Missing AccessKey
		Region: "us-east-1",
	}
	s3Backend3 := &backuppb.S3{}
	err = options3.Apply(s3Backend3)
	require.Error(t, err, "Should require access key when secret key is provided")
	require.Contains(t, err.Error(), "access_key not found")

	// Case 4: No credentials - should be valid (could use IAM role, etc.)
	options4 := &S3BackendOptions{
		Region: "us-east-1",
	}
	s3Backend4 := &backuppb.S3{}
	err = options4.Apply(s3Backend4)
	require.NoError(t, err, "Should accept no credentials when no profile (IAM role, etc.)")
}

func TestS3BackendOptionsProfilePartialCredentials(t *testing.T) {
	// Test that with profile, partial credentials don't cause validation errors

	// Case 1: Profile with only access key (no secret) - should be allowed
	options1 := &S3BackendOptions{
		Profile:   "test-profile",
		AccessKey: "override-access-key",
		// No SecretAccessKey - should be OK with profile
	}
	s3Backend1 := &backuppb.S3{}
	err := options1.Apply(s3Backend1)
	require.NoError(t, err, "Should allow partial credentials with profile")

	// Case 2: Profile with only secret key (no access) - should be allowed
	options2 := &S3BackendOptions{
		Profile:         "test-profile",
		SecretAccessKey: "override-secret-key",
		// No AccessKey - should be OK with profile
	}
	s3Backend2 := &backuppb.S3{}
	err = options2.Apply(s3Backend2)
	require.NoError(t, err, "Should allow partial credentials with profile")
}

func TestS3BackendOptionsFlagParsingWithProfile(t *testing.T) {
	// Test that parseFromFlags works correctly with profile and no explicit credentials
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	defineS3Flags(flags)

	// Set only profile, no credentials
	err := flags.Set("s3.profile", "production")
	require.NoError(t, err)
	err = flags.Set("s3.region", "eu-central-1")
	require.NoError(t, err)

	options := &S3BackendOptions{}
	err = options.parseFromFlags(flags)
	require.NoError(t, err)

	// Verify credentials are not required when profile is set
	s3Backend := &backuppb.S3{}
	err = options.Apply(s3Backend)
	require.NoError(t, err, "Should not require explicit credentials when using profile")

	require.Equal(t, "production", s3Backend.Profile)
	require.Equal(t, "eu-central-1", s3Backend.Region)
	require.Equal(t, "", s3Backend.AccessKey)
	require.Equal(t, "", s3Backend.SecretAccessKey)
}

func TestS3ProfileAvoidAutoNewCred(t *testing.T) {
	// Test that when using profile, we don't interfere with AWS SDK credential chain
	// This is verified by ensuring that explicit credentials are not set when using profile

	options := &S3BackendOptions{
		Profile: "test-profile",
		Region:  "us-west-2",
		// Explicitly not setting AccessKey/SecretAccessKey

	}

	s3Backend := &backuppb.S3{}
	err := options.Apply(s3Backend)
	require.NoError(t, err, "Should work with profile and no explicit credentials")

	// Verify profile is set but no explicit credentials
	require.Equal(t, "test-profile", s3Backend.Profile)
	require.Equal(t, "us-west-2", s3Backend.Region)
	require.Equal(t, "", s3Backend.AccessKey, "Should not have explicit access key when using profile")
	require.Equal(t, "", s3Backend.SecretAccessKey, "Should not have explicit secret key when using profile")
}
