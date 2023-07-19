package server

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/core"
	"github.com/stretchr/testify/require"
)

func TestCheckCN(t *testing.T) {
	s := &Server{cfg: &config.Config{Security: config.Security{ClusterVerifyCN: []string{"a ", "b", "c"}}}}
	tlsConfig := &tls.Config{}
	s.setCNChecker(tlsConfig)
	require.NotNil(t, tlsConfig.VerifyPeerCertificate)
	err := tlsConfig.VerifyPeerCertificate(nil, [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "a"}}}})
	require.NoError(t, err)
	err = tlsConfig.VerifyPeerCertificate(nil, [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "b"}}}})
	require.NoError(t, err)
	err = tlsConfig.VerifyPeerCertificate(nil, [][]*x509.Certificate{{{Subject: pkix.Name{CommonName: "d"}}}})
	require.Error(t, err)
}

func TestWriteDBTablesData(t *testing.T) {
	// No table in a schema.
	info := infoschema.MockInfoSchema([]*model.TableInfo{})
	rc := httptest.NewRecorder()
	tbs := info.SchemaTables(model.NewCIStr("test"))
	require.Equal(t, 0, len(tbs))
	writeDBTablesData(rc, tbs)
	var ti []*model.TableInfo
	decoder := json.NewDecoder(rc.Body)
	err := decoder.Decode(&ti)
	require.NoError(t, err)
	require.Equal(t, 0, len(ti))

	// One table in a schema.
	info = infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable()})
	rc = httptest.NewRecorder()
	tbs = info.SchemaTables(model.NewCIStr("test"))
	require.Equal(t, 1, len(tbs))
	writeDBTablesData(rc, tbs)
	decoder = json.NewDecoder(rc.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.Equal(t, 1, len(ti))
	require.Equal(t, ti[0].ID, tbs[0].Meta().ID)
	require.Equal(t, ti[0].Name.String(), tbs[0].Meta().Name.String())

	// Two tables in a schema.
	info = infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	rc = httptest.NewRecorder()
	tbs = info.SchemaTables(model.NewCIStr("test"))
	require.Equal(t, 2, len(tbs))
	writeDBTablesData(rc, tbs)
	decoder = json.NewDecoder(rc.Body)
	err = decoder.Decode(&ti)
	require.NoError(t, err)
	require.Equal(t, 2, len(ti))
	require.Equal(t, ti[0].ID, tbs[0].Meta().ID)
	require.Equal(t, ti[1].ID, tbs[1].Meta().ID)
	require.Equal(t, ti[0].Name.String(), tbs[0].Meta().Name.String())
	require.Equal(t, ti[1].Name.String(), tbs[1].Meta().Name.String())
}
