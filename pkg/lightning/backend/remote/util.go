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
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"go.uber.org/zap"
)

const (
	maxDuplicateBatchSize = 4 << 20
	taskExitsMsg          = "task exists"

	// format of load data task id: ${keyspaceID}-${taskID}-${tableID}-${engineID}
	loadDatatTaskIDFormat = "%d-%d-%d-%d"

	sleepDuration = 3 * time.Second
	retryCount    = 6000 // 3s * 6000 = 5h

	updateFlushedChunkDuration = 10 * time.Second
)

func genLoadDataTaskID(keyspaceID uint32, taskID int64, cfg *backend.EngineConfig) string {
	return fmt.Sprintf(loadDatatTaskIDFormat, keyspaceID, taskID, cfg.TableInfo.ID, cfg.Remote.EngineID)
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

func parseRemoteWorkerURL(resp *http.Response, enableTLS bool) string {
	base := strings.TrimSuffix(resp.Header.Get("Location"), "/load_data")
	if !enableTLS && strings.HasPrefix(base, "https") {
		return "http" + base[len("https"):]
	}
	return base
}

// EstimateEngineDataSize estimates the data size of the table in the engine.
func EstimateEngineDataSize(tblMeta *mydump.MDTableMeta, tblInfo *checkpoints.TidbTableInfo, isIndexEngine bool, logger log.Logger) int64 {
	if tblMeta == nil || tblInfo == nil {
		// if we can't get table meta or table info, we can't estimate data size.
		return 0
	}
	if isIndexEngine {
		if len(tblInfo.Core.Indices) == 0 || (tblInfo.Core.IsCommonHandle && len(tblInfo.Core.Indices) == 1) {
			return 0
		}
	}

	totalSize := int64(0)
	for _, dataFile := range tblMeta.DataFiles {
		totalSize += dataFile.FileMeta.RealSize
	}
	if tblMeta.IndexRatio > 1 {
		totalSize = int64(float64(totalSize) * tblMeta.IndexRatio)
	}
	logger.Info("estimate data size",
		zap.Int64("estimatedDataSize", totalSize),
		zap.String("db", tblInfo.DB),
		zap.String("table", tblInfo.Name),
		zap.Bool("isIndexEngine", isIndexEngine),
	)
	return totalSize
}

// IsRecoverFromEngineCp checks whether the engine recover from the engine checkpoint.
func IsRecoverFromEngineCp(cp *checkpoints.EngineCheckpoint) bool {
	if cp.Status <= checkpoints.CheckpointStatusMaxInvalid ||
		cp.Status >= checkpoints.CheckpointStatusImported {
		return false
	}

	for _, chunk := range cp.Chunks {
		if chunk.FinishedSize() > 0 {
			return true
		}
	}
	return false
}
