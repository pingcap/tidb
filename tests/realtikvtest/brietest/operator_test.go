package brietest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/task/operator"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

var (
	serviceGCSafepointPrefix = "pd/api/v1/gc/safepoint"
)

func getJson(url string, response any) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(response)
}

func pdAPI(cfg operator.PauseGcConfig, path string) string {
	return fmt.Sprintf("http://%s/%s", cfg.Config.PD[0], path)
}

func verifyGCStopped(t *require.Assertions, cfg operator.PauseGcConfig) {
	var result struct {
		SPs []struct {
			ServiceID string `json:"service_id"`
			ExpiredAt int64  `json:"expired_at"`
			SafePoint int64  `json:"safe_point"`
		} `json:"service_gc_safe_points"`
	}
	t.NoError(getJson(pdAPI(cfg, serviceGCSafepointPrefix), &result))
	for _, sp := range result.SPs {
		if sp.ServiceID != "gc-worker" {

		}
	}
}

func TestOperator(t *testing.T) {
	req := require.New(t)
	rd := make(chan struct{})
	cfg := operator.PauseGcConfig{
		Config: task.Config{
			PD: []string{"127.0.0.1:2379"},
		},
		TTL:       5 * time.Minute,
		SafePoint: oracle.GoTimeToTS(time.Now()),
		OnAllReady: func() {
			close(rd)
		},
	}

	ctx := context.Background()
	go func() {
		req.NoError(operator.AdaptEnvForSnapshotBackup(ctx, &cfg))
	}()
	req.Eventually(func() bool {
		select {
		case <-rd:
			return true
		default:
			return false
		}
	}, 10*time.Second, time.Second)

	verifyGCStopped(req, cfg)
}
