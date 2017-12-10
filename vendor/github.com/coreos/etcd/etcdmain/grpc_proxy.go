// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdmain

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/coreos/etcd/etcdserver/api/v3election/v3electionpb"
	"github.com/coreos/etcd/etcdserver/api/v3lock/v3lockpb"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/pkg/debugutil"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/proxy/grpcproxy"

	"github.com/cockroachdb/cmux"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	grpcProxyListenAddr        string
	grpcProxyEndpoints         []string
	grpcProxyDNSCluster        string
	grpcProxyInsecureDiscovery bool
	grpcProxyCert              string
	grpcProxyKey               string
	grpcProxyCA                string

	grpcProxyAdvertiseClientURL string
	grpcProxyResolverPrefix     string
	grpcProxyResolverTTL        int

	grpcProxyNamespace string

	grpcProxyEnablePprof bool
)

func init() {
	rootCmd.AddCommand(newGRPCProxyCommand())
}

// newGRPCProxyCommand returns the cobra command for "grpc-proxy".
func newGRPCProxyCommand() *cobra.Command {
	lpc := &cobra.Command{
		Use:   "grpc-proxy <subcommand>",
		Short: "grpc-proxy related command",
	}
	lpc.AddCommand(newGRPCProxyStartCommand())

	return lpc
}

func newGRPCProxyStartCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "start",
		Short: "start the grpc proxy",
		Run:   startGRPCProxy,
	}

	cmd.Flags().StringVar(&grpcProxyListenAddr, "listen-addr", "127.0.0.1:23790", "listen address")
	cmd.Flags().StringVar(&grpcProxyDNSCluster, "discovery-srv", "", "DNS domain used to bootstrap initial cluster")
	cmd.Flags().BoolVar(&grpcProxyInsecureDiscovery, "insecure-discovery", false, "accept insecure SRV records")
	cmd.Flags().StringSliceVar(&grpcProxyEndpoints, "endpoints", []string{"127.0.0.1:2379"}, "comma separated etcd cluster endpoints")
	cmd.Flags().StringVar(&grpcProxyCert, "cert", "", "identify secure connections with etcd servers using this TLS certificate file")
	cmd.Flags().StringVar(&grpcProxyKey, "key", "", "identify secure connections with etcd servers using this TLS key file")
	cmd.Flags().StringVar(&grpcProxyCA, "cacert", "", "verify certificates of TLS-enabled secure etcd servers using this CA bundle")
	cmd.Flags().StringVar(&grpcProxyAdvertiseClientURL, "advertise-client-url", "127.0.0.1:23790", "advertise address to register (must be reachable by client)")
	cmd.Flags().StringVar(&grpcProxyResolverPrefix, "resolver-prefix", "", "prefix to use for registering proxy (must be shared with other grpc-proxy members)")
	cmd.Flags().IntVar(&grpcProxyResolverTTL, "resolver-ttl", 0, "specify TTL, in seconds, when registering proxy endpoints")
	cmd.Flags().StringVar(&grpcProxyNamespace, "namespace", "", "string to prefix to all keys for namespacing requests")
	cmd.Flags().BoolVar(&grpcProxyEnablePprof, "enable-pprof", false, `Enable runtime profiling data via HTTP server. Address is at client URL + "/debug/pprof/"`)

	return &cmd
}

func startGRPCProxy(cmd *cobra.Command, args []string) {
	if grpcProxyResolverPrefix != "" && grpcProxyResolverTTL < 1 {
		fmt.Fprintln(os.Stderr, fmt.Errorf("invalid resolver-ttl %d", grpcProxyResolverTTL))
		os.Exit(1)
	}
	if grpcProxyResolverPrefix == "" && grpcProxyResolverTTL > 0 {
		fmt.Fprintln(os.Stderr, fmt.Errorf("invalid resolver-prefix %q", grpcProxyResolverPrefix))
		os.Exit(1)
	}
	if grpcProxyResolverPrefix != "" && grpcProxyResolverTTL > 0 && grpcProxyAdvertiseClientURL == "" {
		fmt.Fprintln(os.Stderr, fmt.Errorf("invalid advertise-client-url %q", grpcProxyAdvertiseClientURL))
		os.Exit(1)
	}

	srvs := discoverEndpoints(grpcProxyDNSCluster, grpcProxyCA, grpcProxyInsecureDiscovery)
	if len(srvs.Endpoints) != 0 {
		grpcProxyEndpoints = srvs.Endpoints
	}

	l, err := net.Listen("tcp", grpcProxyListenAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if l, err = transport.NewKeepAliveListener(l, "tcp", nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	plog.Infof("listening for grpc-proxy client requests on %s", grpcProxyListenAddr)
	defer func() {
		l.Close()
		plog.Infof("stopping listening for grpc-proxy client requests on %s", grpcProxyListenAddr)
	}()
	m := cmux.New(l)

	cfg, err := newClientCfg()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	client, err := clientv3.New(*cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if len(grpcProxyNamespace) > 0 {
		client.KV = namespace.NewKV(client.KV, grpcProxyNamespace)
		client.Watcher = namespace.NewWatcher(client.Watcher, grpcProxyNamespace)
		client.Lease = namespace.NewLease(client.Lease, grpcProxyNamespace)
	}

	kvp, _ := grpcproxy.NewKvProxy(client)
	watchp, _ := grpcproxy.NewWatchProxy(client)
	if grpcProxyResolverPrefix != "" {
		grpcproxy.Register(client, grpcProxyResolverPrefix, grpcProxyAdvertiseClientURL, grpcProxyResolverTTL)
	}
	clusterp, _ := grpcproxy.NewClusterProxy(client, grpcProxyAdvertiseClientURL, grpcProxyResolverPrefix)
	leasep, _ := grpcproxy.NewLeaseProxy(client)
	mainp := grpcproxy.NewMaintenanceProxy(client)
	authp := grpcproxy.NewAuthProxy(client)
	electionp := grpcproxy.NewElectionProxy(client)
	lockp := grpcproxy.NewLockProxy(client)

	server := grpc.NewServer(
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
	pb.RegisterKVServer(server, kvp)
	pb.RegisterWatchServer(server, watchp)
	pb.RegisterClusterServer(server, clusterp)
	pb.RegisterLeaseServer(server, leasep)
	pb.RegisterMaintenanceServer(server, mainp)
	pb.RegisterAuthServer(server, authp)
	v3electionpb.RegisterElectionServer(server, electionp)
	v3lockpb.RegisterLockServer(server, lockp)

	errc := make(chan error)

	grpcl := m.Match(cmux.HTTP2())
	go func() { errc <- server.Serve(grpcl) }()

	httpmux := http.NewServeMux()
	httpmux.HandleFunc("/", http.NotFound)
	httpmux.Handle("/metrics", prometheus.Handler())
	if grpcProxyEnablePprof {
		for p, h := range debugutil.PProfHandlers() {
			httpmux.Handle(p, h)
		}
		plog.Infof("pprof is enabled under %s", debugutil.HTTPPrefixPProf)
	}

	srvhttp := &http.Server{
		Handler: httpmux,
	}

	var httpl net.Listener
	if cfg.TLS != nil {
		srvhttp.TLSConfig = cfg.TLS
		httpl = tls.NewListener(m.Match(cmux.Any()), cfg.TLS)
	} else {
		httpl = m.Match(cmux.HTTP1())
	}
	go func() { errc <- srvhttp.Serve(httpl) }()

	go func() { errc <- m.Serve() }()

	// grpc-proxy is initialized, ready to serve
	notifySystemd()

	fmt.Fprintln(os.Stderr, <-errc)
	os.Exit(1)
}

func newClientCfg() (*clientv3.Config, error) {
	// set tls if any one tls option set
	var cfgtls *transport.TLSInfo
	tlsinfo := transport.TLSInfo{}
	if grpcProxyCert != "" {
		tlsinfo.CertFile = grpcProxyCert
		cfgtls = &tlsinfo
	}

	if grpcProxyKey != "" {
		tlsinfo.KeyFile = grpcProxyKey
		cfgtls = &tlsinfo
	}

	if grpcProxyCA != "" {
		tlsinfo.CAFile = grpcProxyCA
		cfgtls = &tlsinfo
	}

	cfg := clientv3.Config{
		Endpoints:   grpcProxyEndpoints,
		DialTimeout: 5 * time.Second,
	}
	if cfgtls != nil {
		clientTLS, err := cfgtls.ClientConfig()
		if err != nil {
			return nil, err
		}
		cfg.TLS = clientTLS
	}

	// TODO: support insecure tls

	return &cfg, nil
}
