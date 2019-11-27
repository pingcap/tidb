package metrictable

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/api/prometheus/v1"
	pmodel "github.com/prometheus/common/model"
)

const promReadTimeout = time.Second * 10

func queryMetric(addr string, def metricTableDef, queryRange v1.Range) (pmodel.Value, error) {
	queryClient, err := newQueryClient(addr)
	if err != nil {
		return nil, err
	}

	promQLAPI := v1.NewAPI(queryClient)
	ctx, cancel := context.WithTimeout(context.Background(), promReadTimeout)
	defer cancel()

	promQL := def.genPromQL(nil)
	return queryRangePromQL(ctx, promQLAPI, promQL, queryRange)
}

func queryRangePromQL(ctx context.Context, api v1.API, promQL string, queryRange v1.Range) (pmodel.Value, error) {
	result, _, err := api.QueryRange(ctx, promQL, queryRange)
	return result, err
}

func getMetricAddr(ctx sessionctx.Context) (string, error) {
	// Get PD servers info.
	store := ctx.GetStore()
	etcd, ok := store.(tikv.EtcdBackend)
	if !ok {
		return "", errors.Errorf("%T not an etcd backend", store)
	}
	for _, addr := range etcd.EtcdAddrs() {
		addr = strings.TrimSpace(addr)
		return addr, nil
	}
	return "", errors.Errorf("pd address was not found")
}

type queryClient struct {
	api.Client
}

func newQueryClient(addr string) (api.Client, error) {
	promClient, err := api.NewClient(api.Config{
		Address: fmt.Sprintf("http://%s", addr),
	})
	if err != nil {
		return nil, err
	}
	return &queryClient{
		promClient,
	}, nil
}

// URL implement the api.Client interface.
// This is use to convert prometheus api path to PD API path.
func (c *queryClient) URL(ep string, args map[string]string) *url.URL {
	ep = strings.Replace(ep, "api/v1", "pd/api/v1/metric", 1)
	return c.Client.URL(ep, args)
}
