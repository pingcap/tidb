package clustertablestest

import (
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestConfigViewPrivilege(t *testing.T) {
	s := new(clusterTablesSuite)
	s.store, s.dom = testkit.CreateMockStoreAndDomain(t)
	s.rpcserver, s.listenAddr = s.setUpRPCService(t, "127.0.0.1:0", nil)
	s.httpServer, s.mockAddr = s.setUpMockPDHTTPServer()
	s.startTime = time.Now()
	defer s.httpServer.Close()
	defer s.rpcserver.Stop()

	tk := s.newTestKitWithRoot(t)
	dropUserTk := s.newTestKitWithRoot(t)
	tk.MustExec("create user 'config_view_user'@'localhost'")
	defer dropUserTk.MustExec("drop user 'config_view_user'@'localhost'")
	tk.MustExec("grant config_view on *.* to 'config_view_user'@'localhost'")

	fpExpr := `return("pd,` + s.listenAddr + `,` + s.mockAddr + `;tikv,` + s.listenAddr + `,` + s.mockAddr + `")`
	fpName := "github.com/pingcap/tidb/pkg/executor/mockClusterConfigServerInfo"
	require.NoError(t, failpoint.Enable(fpName, fpExpr))
	defer func() { require.NoError(t, failpoint.Disable(fpName)) }()

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{
		Username: "config_view_user",
		Hostname: "localhost",
	}, nil, nil, nil))
	tk.MustExec("use information_schema")
	tk.MustQuery("show config")
	tk.MustQuery("select * from information_schema.cluster_config")
	if runtime.GOOS != "windows" && !(runtime.GOOS == "darwin" && runtime.GOARCH == "arm64") {
		tk.MustQuery("select count(*) > 0 from information_schema.cluster_hardware").Check(testkit.Rows("1"))
	}

	err := tk.ExecToErr(`set config tikv testkey="testval"`)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CONFIG privilege(s) for this operation")
}
