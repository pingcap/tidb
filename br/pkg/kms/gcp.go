// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package kms

import (
	"context"
	"hash/crc32"
	"strings"

	"cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	// need to keep it exactly same as TiKV STORAGE_VENDOR_NAME_GCP in TiKV
	StorageVendorNameGcp = "gcp"
)

type GcpKms struct {
	config *encryptionpb.MasterKeyKms
	// the location prefix of key id,
	// format: projects/{project_name}/locations/{location}
	location string
	client   *kms.KeyManagementClient
}

func NewGcpKms(config *encryptionpb.MasterKeyKms) (*GcpKms, error) {
	if config.GcpKms == nil {
		return nil, errors.New("GCP config is missing")
	}

	// config string pattern verified at parsing flag phase, we should have valid string at this stage.
	config.KeyId = strings.TrimSuffix(config.KeyId, "/")

	// join the first 4 parts of the key id to get the location
	location := strings.Join(strings.Split(config.KeyId, "/")[:4], "/")

	ctx := context.Background()

	var clientOpt option.ClientOption
	if config.GcpKms.Credential != "" {
		clientOpt = option.WithCredentialsFile(config.GcpKms.Credential)
	}

	client, err := kms.NewKeyManagementClient(ctx, clientOpt)
	if err != nil {
		return nil, errors.Errorf("failed to create GCP KMS client: %v", err)
	}

	return &GcpKms{
		config:   config,
		location: location,
		client:   client,
	}, nil
}

func (g *GcpKms) Name() string {
	return StorageVendorNameGcp
}

func (g *GcpKms) DecryptDataKey(ctx context.Context, dataKey []byte) ([]byte, error) {
	req := &kmspb.DecryptRequest{
		Name:             g.config.KeyId,
		Ciphertext:       dataKey,
		CiphertextCrc32C: wrapperspb.Int64(int64(g.calculateCRC32C(dataKey))),
	}

	resp, err := g.client.Decrypt(ctx, req)
	if err != nil {
		return nil, errors.Annotate(err, "gcp kms decrypt request failed")
	}

	if int64(g.calculateCRC32C(resp.Plaintext)) != resp.PlaintextCrc32C.Value {
		return nil, errors.New("response corrupted in-transit")
	}

	return resp.Plaintext, nil
}

func (g *GcpKms) checkCRC32(data []byte, expected int64) error {
	crc := int64(g.calculateCRC32C(data))
	if crc != expected {
		return errors.Errorf("crc32c mismatch, expected: %d, got: %d", expected, crc)
	}
	return nil
}

func (g *GcpKms) calculateCRC32C(data []byte) uint32 {
	t := crc32.MakeTable(crc32.Castagnoli)
	return crc32.Checksum(data, t)
}

func (g *GcpKms) Close() {
	err := g.client.Close()
	if err != nil {
		log.Error("failed to close gcp kms client", zap.Error(err))
	}
}
