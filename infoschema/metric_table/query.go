package metric_table

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/api/prometheus/v1"
	pmodel "github.com/prometheus/common/model"
)

const promReadTimeout = time.Second * 10

func queryMetric(promAddr string, def metricTableDef, queryRange v1.Range) (pmodel.Value, error) {
	promClient, err := api.NewClient(api.Config{
		Address: fmt.Sprintf("http://%s", promAddr),
	})
	if err != nil {
		return nil, err
	}

	promQLAPI := v1.NewAPI(promClient)
	ctx, cancel := context.WithTimeout(context.Background(), promReadTimeout)
	defer cancel()

	promQL := def.genPromQL(nil)
	return queryRangePromQL(ctx, promQLAPI, promQL, queryRange)
}

func queryRangePromQL(ctx context.Context, api v1.API, promQL string, queryRange v1.Range) (pmodel.Value, error) {
	result, warning, err := api.QueryRange(ctx, promQL, queryRange)
	if err != nil {
		return nil, err
	}
	fmt.Printf("warning: %v\n", warning)
	if result.Type() == pmodel.ValMatrix {
		matrix, _ := result.(pmodel.Matrix)
		for _, ss := range matrix {
			vals := make([]string, len(ss.Values))
			for i, v := range ss.Values {
				vals[i] = fmt.Sprintf("%s @[%s]", v.Value, time.Unix(int64(v.Timestamp/1000), 0))
			}
			fmt.Println(fmt.Sprintf("%s =>\n%s\n", ss.Metric, strings.Join(vals, "\n")))
		}
	}
	return result, nil
}

func getMetricAddr() string {
	return "127.0.0.1:9090"
}
