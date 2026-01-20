// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package httputil

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/pingcap/errors"
)

// NewClient returns an HTTP(s) client.
func NewClient(tlsConf *tls.Config) *http.Client {
	// defaultTimeout for non-context requests.
	const defaultTimeout = 30 * time.Second
	cli := &http.Client{Timeout: defaultTimeout}
	if tlsConf != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConf
		transport.IdleConnTimeout = 30 * time.Second
		cli.Transport = transport
	}
	return cli
}

// GetJSON fetches a page and parses it as JSON. The parsed result will be
// stored into the `v`. The variable `v` must be a pointer to a type that can be
// unmarshalled from JSON.
//
// Example:
//
//	client := &http.Client{}
//	var resp struct { IP string }
//	if err := util.GetJSON(client, "http://api.ipify.org/?format=json", &resp); err != nil {
//		return errors.Trace(err)
//	}
//	fmt.Println(resp.IP)
func GetJSON(ctx context.Context, client *http.Client, url string, v any) error {
	body, err := doGet(ctx, client, url)
	if err != nil {
		return errors.Trace(err)
	}
	defer body.Close()
	return errors.Trace(json.NewDecoder(body).Decode(v))
}

// GetText gets a page and returns its content as a string.
func GetText(client *http.Client, url string) (string, error) {
	body, err := doGet(context.Background(), client, url)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(data), nil
}

func doGet(ctx context.Context, client *http.Client, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return nil, errors.Errorf("get %s http status code != 200, message %s", url, string(body))
	}

	return resp.Body, nil
}
