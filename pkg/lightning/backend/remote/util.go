package remote

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
)

const (
	maxDuplicateBatchSize = 4 << 20
	taskExitsMsg          = "task exists"

	sleepDuration = 3 * time.Second
	retryCount    = 6000 // 3s * 6000 = 5h

	updateFlushedChunkDuration = 10 * time.Second
)

func genLoadDataTaskID(keyspaceID uint32, taskID int64, cfg *backend.EngineConfig) string {
	return fmt.Sprintf("%d-%d-%d-%d", keyspaceID, taskID, cfg.TableInfo.ID, cfg.Remote.EngineID)
}

func retryableHTTPStatusCode(statusCode int) bool {
	return statusCode == http.StatusServiceUnavailable || // 503
		statusCode == http.StatusInternalServerError || // 500
		statusCode == http.StatusRequestTimeout // 408
}

func sendRequestWithRetry(ctx context.Context, httpClient *http.Client, method, url string, data []byte) (*http.Response, error) {
	var (
		req  *http.Request
		resp *http.Response
		err  error
	)
	for retry := 0; retry < retryCount; retry++ {
		body := bytes.NewReader(data)
		req, err = http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			return nil, err
		}
		resp, err = httpClient.Do(req)
		if err != nil || retryableHTTPStatusCode(resp.StatusCode) {
			statusCode := 0
			if resp != nil {
				statusCode = resp.StatusCode
			}
			log.FromContext(ctx).Warn("failed to send http request", zap.Error(err), zap.Int("status code", statusCode))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(sleepDuration):
				continue
			}
		}
		break
	}
	return resp, err
}

func sendRequest(ctx context.Context, httpClient *http.Client, method, url string, data []byte) ([]byte, error) {
	resp, err := sendRequestWithRetry(ctx, httpClient, method, url, data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		return nil, common.ErrRequestRemoteWorker.FastGenByArgs(resp.StatusCode, string(msg))
	}
	return io.ReadAll(resp.Body)
}

func parseLDWUrl(resp *http.Response, enableTLS bool) string {
	base := strings.TrimSuffix(resp.Header.Get("Location"), "/load_data")
	if !enableTLS && strings.HasPrefix(base, "https") {
		return "http" + base[len("https"):]
	}
	return base
}
