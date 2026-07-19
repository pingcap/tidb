// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ProtonMail/go-crypto/openpgp/armor"
	"github.com/ProtonMail/go-crypto/openpgp/packet"
	openpgp "github.com/ProtonMail/go-crypto/openpgp/v2"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/stretchr/testify/require"
)

func TestCreateExternalStorage(t *testing.T) {
	mockConfig := defaultConfigForTest(t)
	loc, err := mockConfig.createExternalStorage(tcontext.Background())
	require.NoError(t, err)
	require.Regexp(t, "^file:", loc.URI())
	loc.Close()

	missingKeyConfig := defaultConfigForTest(t)
	missingKeyConfig.GPGKeyFile = filepath.Join(t.TempDir(), "missing-public-key.asc")
	_, err = missingKeyConfig.createExternalStorage(tcontext.Background())
	require.ErrorContains(t, err, "read GPG public key file")

	entity, publicKey := newDumplingGPGTestKey(t)
	keyPath := filepath.Join(t.TempDir(), "public-key.asc")
	require.NoError(t, os.WriteFile(keyPath, publicKey, 0o600))
	inner := objstore.NewMemStorage()
	encryptedConfig := defaultConfigForTest(t)
	encryptedConfig.ExtStorage = inner
	encryptedConfig.GPGKeyFile = keyPath
	encryptedStorage, err := encryptedConfig.createExternalStorage(tcontext.Background())
	require.NoError(t, err)

	plaintext := []byte("dumpling output encrypted through its external storage\n")
	writer, tearDown, err := buildFileWriter(tcontext.Background(), encryptedStorage, "table.csv", compressedio.Gzip)
	require.NoError(t, err)
	_, err = writer.Write(tcontext.Background(), plaintext)
	require.NoError(t, err)
	require.NoError(t, tearDown(tcontext.Background()))
	ciphertext, err := inner.ReadFile(tcontext.Background(), "table.csv.gz.gpg")
	require.NoError(t, err)
	require.False(t, bytes.Contains(ciphertext, plaintext))

	message, err := openpgp.ReadMessage(bytes.NewReader(ciphertext), openpgp.EntityList{entity}, nil, nil)
	require.NoError(t, err)
	compressed, err := io.ReadAll(message.UnverifiedBody)
	require.NoError(t, err)
	reader, err := compressedio.NewReader(compressedio.Gzip, compressedio.DecompressConfig{}, bytes.NewReader(compressed))
	require.NoError(t, err)
	decrypted, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, plaintext, decrypted)
}

func newDumplingGPGTestKey(t *testing.T) (*openpgp.Entity, []byte) {
	t.Helper()
	entity, err := openpgp.NewEntity("Dumpling Test", "", "dumpling@example.com", &packet.Config{
		Algorithm:     packet.PubKeyAlgoEdDSA,
		Curve:         packet.Curve25519,
		DefaultCipher: packet.CipherAES256,
	})
	require.NoError(t, err)
	var publicKey bytes.Buffer
	armorWriter, err := armor.Encode(&publicKey, openpgp.PublicKeyType, nil)
	require.NoError(t, err)
	require.NoError(t, entity.Serialize(armorWriter))
	require.NoError(t, armorWriter.Close())
	return entity, publicKey.Bytes()
}

func TestMatchMysqlBugVersion(t *testing.T) {
	cases := []struct {
		serverInfo version.ServerInfo
		expected   bool
	}{
		{version.ParseServerInfo("5.7.25-TiDB-3.0.6"), false},
		{version.ParseServerInfo("8.0.2"), false},
		{version.ParseServerInfo("8.0.3"), true},
		{version.ParseServerInfo("8.0.22"), true},
		{version.ParseServerInfo("8.0.23"), false},
	}
	for _, x := range cases {
		require.Equalf(t, x.expected, matchMysqlBugversion(x.serverInfo), "server info: %s", x.serverInfo)
	}
}

func TestGetConfTables(t *testing.T) {
	tablesList := []string{"db1t1", "db2.t1"}
	_, err := GetConfTables(tablesList)
	require.EqualError(t, err, fmt.Sprintf("--tables-list only accepts qualified table names, but `%s` lacks a dot", tablesList[0]))

	tablesList = []string{"db1.t1", "db2t1"}
	_, err = GetConfTables(tablesList)
	require.EqualError(t, err, fmt.Sprintf("--tables-list only accepts qualified table names, but `%s` lacks a dot", tablesList[1]))

	tablesList = []string{"db1.t1", "db2.t1"}
	expectedDBTables := NewDatabaseTables().
		AppendTables("db1", []string{"t1"}, []uint64{0}).
		AppendTables("db2", []string{"t1"}, []uint64{0})
	actualDBTables, err := GetConfTables(tablesList)
	require.NoError(t, err)
	require.Equal(t, expectedDBTables, actualDBTables)
}
