package datad

import (
	"net/url"
	"strings"
)

// A Client routes requests for data.
type Client struct {
	backend Backend

	reg *registry

	serversPrefix string
}

func NewClient(b Backend, keyPrefix string) *Client {
	keyPrefix = strings.TrimSuffix(keyPrefix, "/")
	return &Client{
		backend:       b,
		reg:           newRegistry(b, keyPrefix+"/registry"),
		serversPrefix: keyPrefix + "/servers",
	}
}

// ListServers returns a list of server URLs in the cluster.
func (c *Client) ListServers() ([]string, error) {
	servers, err := c.backend.List(c.serversPrefix + "/")
	if err != nil {
		return nil, err
	}

	for i, s := range servers {
		servers[i], err = decodeServerURL(s)
		if err != nil {
			return nil, err
		}
	}

	return servers, nil
}

// AddServer adds an server URL to the cluster, making it available to be
// assigned data.
func (c *Client) AddServer(urlStr string) error {
	return c.backend.Set(c.serversPrefix+"/"+encodeServerURL(urlStr), "")
}

func encodeServerURL(urlStr string) string {
	return url.QueryEscape(urlStr)
}

func decodeServerURL(encURLStr string) (string, error) {
	return url.QueryUnescape(encURLStr)
}
