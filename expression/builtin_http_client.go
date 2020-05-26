package expression

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/config"
)

type tidbServerClient struct {
	port       uint
	statusPort uint
}

func newTiDBServerClient() *tidbServerClient {
	globalConfig := config.GetGlobalConfig()
	return &tidbServerClient{
		port:       globalConfig.Port,
		statusPort: globalConfig.Status.StatusPort,
	}
}

// statusURL return the full URL of a status path
func (client *tidbServerClient) statusURL(path string) string {
	return fmt.Sprintf("http://localhost:%d%s", client.statusPort, path)
}

// fetchStatus exec http.Get to server status port
func (client *tidbServerClient) fetchStatus(path string) (*http.Response, error) {
	return http.Get(client.statusURL(path))
}

// postStatus exec http.Port to server status port
func (client *tidbServerClient) postStatus(path, contentType string, body io.Reader) (*http.Response, error) {
	return http.Post(client.statusURL(path), contentType, body)
}

// formStatus post a form request to server status address
func (client *tidbServerClient) formStatus(path string, data url.Values) (*http.Response, error) {
	return http.PostForm(client.statusURL(path), data)
}

// getDSN generates a DSN string for MySQL connection.
func (client *tidbServerClient) getDSN() string {
	cfg := mysql.NewConfig()
	cfg.User = "root"
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("127.0.0.1:%d", client.port)
	cfg.DBName = "test"
	return cfg.FormatDSN()
}
