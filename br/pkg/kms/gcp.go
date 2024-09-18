// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package kms

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"golang.org/x/oauth2/google"
)

const (
	GcpKmsEndpoint       = "https://cloudkms.googleapis.com/v1/"
	MethodDecrypt        = "decrypt"
	KeyIdPattern         = `^projects/([^/]+)/locations/([^/]+)/keyRings/([^/]+)/cryptoKeys/([^/]+)/?$`
	StorageVendorNameGcp = "gcp"
)

var KeyIdRegex = regexp.MustCompile(KeyIdPattern)

type GcpKms struct {
	config   *encryptionpb.MasterKeyKms
	location string
	client   *http.Client
}

func NewGcpKms(config *encryptionpb.MasterKeyKms) (*GcpKms, error) {
	if config.GcpKms == nil {
		return nil, errors.New("GCP config is missing")
	}
	if !KeyIdRegex.MatchString(config.KeyId) {
		return nil, errors.Errorf("invalid key: '%s'", config.KeyId)
	}
	config.KeyId = strings.TrimSuffix(config.KeyId, "/")
	location := strings.Join(strings.Split(config.KeyId, "/")[:4], "/")

	client, err := google.DefaultClient(context.Background(), "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, errors.Errorf("failed to create GCP client: %v", err)
	}

	return &GcpKms{
		config:   config,
		location: location,
		client:   client,
	}, nil
}

func (g *GcpKms) doJSONRequest(ctx context.Context, keyName, method string, data any) ([]byte, error) {
	url := g.formatCallURL(keyName, method)
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Annotate(err, "failed to marshal request")
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(jsonData)))
	if err != nil {
		return nil, errors.Annotate(err, "failed to create request")
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := g.client.Do(req)
	if err != nil {
		return nil, errors.Annotate(err, "request failed")
	}
	defer resp.Body.Close()

	// TODO: Implement metrics
	_ = time.Since(start)

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("request failed with status: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Annotate(err, "failed to read response body")
	}

	return body, nil
}

func (g *GcpKms) formatCallURL(key, method string) string {
	return fmt.Sprintf("%s%s/:%s?alt=json", GcpKmsEndpoint, key, method)
}

func (g *GcpKms) Name() string {
	return StorageVendorNameGcp
}

func (g *GcpKms) DecryptDataKey(ctx context.Context, dataKey []byte) ([]byte, error) {
	decryptReq := DecryptRequest{
		Ciphertext:       base64.StdEncoding.EncodeToString(dataKey),
		CiphertextCrc32c: int64(crc32.Checksum(dataKey, crc32.MakeTable(crc32.Castagnoli))),
	}

	respBody, err := g.doJSONRequest(ctx, g.config.KeyId, MethodDecrypt, decryptReq)
	if err != nil {
		return nil, errors.Annotate(err, "decrypt request failed")
	}

	var resp DecryptResp
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, errors.Annotate(err, "failed to unmarshal decrypt response")
	}

	plaintext, err := base64.StdEncoding.DecodeString(resp.Plaintext)
	if err != nil {
		return nil, errors.Annotate(err, "failed to decode plaintext")
	}

	if err := checkCRC32(plaintext, resp.PlaintextCrc32c); err != nil {
		return nil, err
	}

	return plaintext, nil
}

type EncryptRequest struct {
	Plaintext       string `json:"plaintext"`
	PlaintextCrc32c int64  `json:"plaintextCrc32c,string"`
}

type EncryptResp struct {
	Ciphertext       string `json:"ciphertext"`
	CiphertextCrc32c int64  `json:"ciphertextCrc32c,string"`
}

type DecryptRequest struct {
	Ciphertext       string `json:"ciphertext"`
	CiphertextCrc32c int64  `json:"ciphertextCrc32c,string"`
}

type DecryptResp struct {
	Plaintext       string `json:"plaintext"`
	PlaintextCrc32c int64  `json:"plaintextCrc32c,string"`
}

type GenRandomBytesReq struct {
	LengthBytes     int    `json:"lengthBytes"`
	ProtectionLevel string `json:"protectionLevel"`
}

type GenRandomBytesResp struct {
	Data       string `json:"data"`
	DataCrc32c int64  `json:"dataCrc32c,string"`
}

func checkCRC32(data []byte, expected int64) error {
	crc := int64(crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)))
	if crc != expected {
		return errors.Errorf("crc32c mismatch, expected: %d, got: %d", expected, crc)
	}
	return nil
}
