// Copyright 2026 PingCAP, Inc.
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

package remote

import (
	"context"
	"io"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSPFreshRecoveryPollsWithoutUploadingChunks(t *testing.T) {
	for _, tc := range []struct{ name, state string }{
		{"retrying", `"status":"retrying","next-retry-at-ms":0`},
		{"paused", `"status":"paused","next-retry-at-ms":0`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				puts, gets := 0, 0
				var c *chunkSender
				client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					require.Equal(t, "worker.test", req.URL.Host)
					require.Equal(t, "/spfresh/build", req.URL.Path)
					require.Equal(t, "7", req.URL.Query().Get("cluster_id"))
					require.Equal(t, "task-1", req.URL.Query().Get("task_id"))
					require.Zero(t, c.loadState().HandledChunkID)
					if req.Method == http.MethodGet {
						gets++
						require.Zero(t, req.ContentLength)
						require.Empty(t, req.URL.Query().Get("writer_id"))
						// These aggregate watermarks can belong to another writer. They must
						// neither acknowledge this chunk nor delete it from the replay cache.
						if gets <= 20 {
							require.Equal(t, 1, puts)
							return jsonResponse(req, `{"task-id":"task-1",`+tc.state+`,"received-chunk-watermark":99,"ingested-chunk-watermark":99}`), nil
						}
						return jsonResponse(req, `{"task-id":"task-1","status":"running","next-retry-at-ms":null,"received-chunk-watermark":99,"ingested-chunk-watermark":99}`), nil
					}
					require.Equal(t, http.MethodPut, req.Method)
					require.Equal(t, "11", req.URL.Query().Get("writer_id"))
					require.Equal(t, "1", req.URL.Query().Get("chunk_id"))
					body, err := io.ReadAll(req.Body)
					require.NoError(t, err)
					require.Equal(t, "original chunk", string(body))
					puts++
					if puts <= 2 { // Recovery can race with a GET that reported running.
						state := tc.state
						if puts == 2 {
							state = `"status":"running","next-retry-at-ms":null`
						}
						resp := jsonResponse(req, `{`+state+`}`)
						resp.StatusCode = http.StatusAccepted
						return resp, nil
					}
					return jsonResponse(req, `{"handled-chunk-id":1,"flushed-chunk-id":1}`), nil
				})}
				cache, err := newChunkCache("task-1", 11, "", true)
				require.NoError(t, err)
				c = newTestChunkSender(client, cache, taskTypeSPFreshBuild)
				c.e.addr = "http://worker.test"
				require.NoError(t, c.putChunkToRemote(context.Background(), &chunk{id: 1, data: []byte("original chunk")}))
				require.Equal(t, 3, puts)
				require.Equal(t, 22, gets)
				require.Equal(t, uint64(1), c.loadState().HandledChunkID)
				require.Zero(t, c.getFlushedChunkID())
				data, err := cache.get(1)
				require.NoError(t, err)
				require.Equal(t, "original chunk", string(data))
			})
		})
	}
}

func TestSPFreshRecoveryRejectsInvalidTaskState(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		code             int
	}{
		{"canceled", `{"task-id":"task-1","status":"canceled"}`, ErrTaskCanceled.Error(), 200},
		{"error", `{"task-id":"task-1","status":"error","error":"broken"}`, "broken", 200},
		{"sealing", `{"task-id":"task-1","status":"sealing"}`, "sealing", 200},
		{"sealed", `{"task-id":"task-1","status":"sealed"}`, "sealed", 200},
		{"unknown", `{"task-id":"task-1","status":"unknown"}`, "unknown", 200},
		{"missing_status", `{"task-id":"task-1"}`, "cannot resume", 200},
		{"wrong_task", `{"task-id":"task-2","status":"running"}`, "identity mismatch", 200},
		{"missing_task", `{"status":"running"}`, "identity mismatch", 200},
		{"malformed", `{`, "invalid SPFresh", 200},
		{"missing", ``, "Not Found", 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				puts := 0
				client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if req.Method == http.MethodPut {
						puts++
						resp := jsonResponse(req, `{"status":"retrying","next-retry-at-ms":0}`)
						resp.StatusCode = http.StatusAccepted
						return resp, nil
					}
					require.Equal(t, http.MethodGet, req.Method)
					resp := jsonResponse(req, tc.body)
					resp.StatusCode = tc.code
					resp.Status = http.StatusText(tc.code)
					return resp, nil
				})}
				cache, err := newChunkCache("task-1", 11, "", true)
				require.NoError(t, err)
				c := newTestChunkSender(client, cache, taskTypeSPFreshBuild)
				require.ErrorContains(t, c.putChunkToRemote(context.Background(), &chunk{id: 1, data: []byte("chunk")}), tc.want)
				require.Equal(t, 1, puts)
				require.Zero(t, c.loadState().HandledChunkID)
				_, err = cache.get(1)
				require.NoError(t, err)
			})
		})
	}
}

func TestSPFreshRecoveryBackoffAndCancellation(t *testing.T) {
	now := time.Now()
	expired := now.Add(-time.Hour).UnixMilli()
	future := now.Add(time.Minute).UnixMilli()
	for attempt := 0; attempt < 20; attempt++ {
		for _, hint := range []*int64{nil, &expired} {
			delay := spfreshBuildPollDelay(attempt, hint, now)
			require.GreaterOrEqual(t, delay, spfreshBuildRetryDelay(attempt)/2)
			require.LessOrEqual(t, delay, spfreshBuildRetryMaxDelay)
		}
		require.Equal(t, time.UnixMilli(future).Sub(now), spfreshBuildPollDelay(attempt, &future, now))
	}
	synctest.Test(t, func(t *testing.T) {
		client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			t.Fatal("canceled wait must not send a request")
			return nil, nil
		})}
		c := newTestChunkSender(client, nil, taskTypeSPFreshBuild)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, c.waitForSPFreshBuildRunning(ctx, spfreshRecoveryState{Status: "retrying", NextRetryAtMs: &future}), context.Canceled)
	})
}

func TestSPFreshRecoveryRejectsMissingRetryMetadataBeforeRunning(t *testing.T) {
	for _, source := range []string{"PUT", "GET"} {
		for _, tc := range []struct{ name, status, metadata string }{
			{"retrying_without_hint", "retrying", ""},
			{"retrying_null_hint", "retrying", `,"next-retry-at-ms":null`},
			{"paused_without_hint", "paused", ""},
			{"paused_null_hint", "paused", `,"next-retry-at-ms":null`},
		} {
			t.Run(source+"/"+tc.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					puts, gets := 0, 0
					client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
						if req.Method == http.MethodPut {
							puts++
							body := `{"status":"retrying","next-retry-at-ms":0}`
							if source == "PUT" {
								body = `{"status":"` + tc.status + `"` + tc.metadata + `}`
							}
							resp := jsonResponse(req, body)
							resp.StatusCode = http.StatusAccepted
							return resp, nil
						}
						gets++
						if source == "GET" && gets == 1 {
							return jsonResponse(req, `{"task-id":"task-1","status":"`+tc.status+`"`+tc.metadata+`}`), nil
						}
						// A later running response must not mask the protocol error.
						return jsonResponse(req, `{"task-id":"task-1","status":"running","next-retry-at-ms":null}`), nil
					})}
					cache, err := newChunkCache("task-1", 11, "", true)
					require.NoError(t, err)
					c := newTestChunkSender(client, cache, taskTypeSPFreshBuild)
					require.ErrorContains(t, c.putChunkToRemote(context.Background(), &chunk{id: 1, data: []byte("chunk")}), tc.status+" requires next-retry-at-ms")
					require.Equal(t, 1, puts)
					if source == "PUT" {
						require.Zero(t, gets)
					} else {
						require.Equal(t, 1, gets)
					}
					require.Zero(t, c.loadState().HandledChunkID)
					data, err := cache.get(1)
					require.NoError(t, err)
					require.Equal(t, "chunk", string(data))
				})
			})
		}
	}
}

func TestSPFreshPausedRecoveryHonorsCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gets := 0
		client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			gets++
			if gets == 20 {
				cancel()
			}
			return jsonResponse(req, `{"task-id":"task-1","status":"paused","next-retry-at-ms":0}`), nil
		})}
		c := newTestChunkSender(client, nil, taskTypeSPFreshBuild)
		expired := int64(0)
		require.ErrorIs(t, c.waitForSPFreshBuildRunning(ctx, spfreshRecoveryState{Status: "paused", NextRetryAtMs: &expired}), context.Canceled)
		require.Equal(t, 20, gets)
	})
}

func TestSPFreshRecoveryDuringGapReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		requests := 0
		client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests++
			switch requests {
			case 1:
				require.Equal(t, http.MethodPut, req.Method)
				require.Equal(t, "2", req.URL.Query().Get("chunk_id"))
				return jsonResponse(req, `{"handled-chunk-id":0}`), nil
			case 2:
				require.Equal(t, http.MethodPut, req.Method)
				require.Equal(t, "1", req.URL.Query().Get("chunk_id"))
				resp := jsonResponse(req, `{"status":"retrying","next-retry-at-ms":0}`)
				resp.StatusCode = http.StatusAccepted
				return resp, nil
			case 3:
				require.Equal(t, http.MethodGet, req.Method)
				return jsonResponse(req, `{"task-id":"task-1","status":"paused","next-retry-at-ms":0}`), nil
			case 4:
				require.Equal(t, http.MethodGet, req.Method)
				return jsonResponse(req, `{"task-id":"task-1","status":"running"}`), nil
			case 5, 6:
				id, data := "1", "first"
				if requests == 6 {
					id, data = "2", "second"
				}
				require.Equal(t, http.MethodPut, req.Method)
				require.Equal(t, id, req.URL.Query().Get("chunk_id"))
				body, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.Equal(t, data, string(body))
				return jsonResponse(req, `{"handled-chunk-id":`+id+`}`), nil
			default:
				t.Fatalf("unexpected request %d", requests)
				return nil, nil
			}
		})}
		cache, err := newChunkCache("task-1", 11, "", true)
		require.NoError(t, err)
		require.NoError(t, cache.put(1, []byte("first")))
		c := newTestChunkSender(client, cache, taskTypeSPFreshBuild)
		require.NoError(t, c.putChunkToRemote(context.Background(), &chunk{id: 2, data: []byte("second")}))
		require.Equal(t, 6, requests)
		require.Equal(t, uint64(2), c.loadState().HandledChunkID)
		require.Zero(t, c.getFlushedChunkID())
		for _, id := range []uint64{1, 2} {
			_, err := cache.get(id)
			require.NoError(t, err)
		}
	})
}
