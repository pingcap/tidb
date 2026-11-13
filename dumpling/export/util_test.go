// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type clusterCheckKVServer struct {
	etcdserverpb.UnimplementedKVServer
}

func (*clusterCheckKVServer) Range(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	return &etcdserverpb.RangeResponse{Kvs: []*mvccpb.KeyValue{
		{Key: []byte(tidbServerInformationPath + "/server1")},
	}}, nil
}

func TestCheckSameClusterTLS(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:        true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certPath, certPEM, 0600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0600))
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(certPEM))

	for _, tc := range []struct {
		name     string
		tls      bool
		override bool
		ddlID    string
	}{
		{name: "plaintext", ddlID: "server1"},
		{name: "SQL TLS fallback", tls: true, ddlID: "server1"},
		{name: "cluster TLS overrides SQL TLS", tls: true, override: true, ddlID: "server1"},
		{name: "TLS different cluster", tls: true, ddlID: "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := DefaultConfig()
			var opts []grpc.ServerOption
			if tc.tls {
				opts = append(opts, grpc.Creds(credentials.NewTLS(&tls.Config{
					MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair},
					ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool,
				})))
				conf.Security.CAPath, conf.Security.CertPath, conf.Security.KeyPath = certPath, certPath, keyPath
				if tc.override {
					conf.ClusterSSLCA, conf.ClusterSSLCert, conf.ClusterSSLKey = certPath, certPath, keyPath
					conf.Security.CAPath, conf.Security.CertPath, conf.Security.KeyPath = "missing-ca", "missing-cert", "missing-key"
				}
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer(opts...)
			etcdserverpb.RegisterKVServer(server, &clusterCheckKVServer{})
			serveDone := make(chan struct{})
			go func() {
				defer close(serveDone)
				_ = server.Serve(listener)
			}()
			t.Cleanup(func() {
				server.Stop()
				<-serveDone
			})
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM information_schema.tidb_servers_info;")).
				WillReturnRows(sqlmock.NewRows([]string{"DDL_ID"}).AddRow(tc.ddlID))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			same, err := checkSameCluster(tcontext.Background().WithContext(ctx), db,
				[]string{listener.Addr().String()}, pdSecurityOptionForGC(conf))
			require.NoError(t, err)
			require.Equal(t, tc.ddlID == "server1", same)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRepeatableRead(t *testing.T) {
	data := [][]any{
		{version.ServerTypeUnknown, ConsistencyTypeNone, true},
		{version.ServerTypeMySQL, ConsistencyTypeFlush, true},
		{version.ServerTypeMariaDB, ConsistencyTypeLock, true},
		{version.ServerTypeTiDB, ConsistencyTypeNone, true},
		{version.ServerTypeTiDB, ConsistencyTypeSnapshot, false},
		{version.ServerTypeTiDB, ConsistencyTypeLock, true},
	}
	dec := func(d []any) (version.ServerType, string, bool) {
		return d[0].(version.ServerType), d[1].(string), d[2].(bool)
	}
	for tag, datum := range data {
		serverTp, consistency, expectRepeatableRead := dec(datum)
		comment := fmt.Sprintf("test case number: %d", tag)
		rr := needRepeatableRead(serverTp, consistency)
		require.True(t, rr == expectRepeatableRead, comment)
	}
}

func TestInfiniteChan(t *testing.T) {
	in, out := infiniteChan[int]()
	go func() {
		for i := range 10000 {
			in <- i
		}
	}()
	for i := range 10000 {
		j := <-out
		require.Equal(t, i, j)
	}
	close(in)
}
