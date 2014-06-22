package datad

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

// A Client routes requests for data.
type Client struct {
	backend Backend

	registry *Registry
}

func NewClient(b Backend) *Client {
	return &Client{
		backend:  b,
		registry: NewRegistry(b),
	}
}

var ErrNoNodesForKey = errors.New("key has no nodes")

// NodesInCluster returns a list of all nodes in the cluster.
func (c *Client) NodesInCluster() ([]string, error) {
	return c.backend.List(nodesPrefix, false)
}

// NodesForKey returns a list of nodes that, according to the registry, hold the
// data specified by key.
func (c *Client) NodesForKey(key string) ([]string, error) {
	return c.registry.NodesForKey(key)
}

// TransportForKey returns a HTTP transport (http.RoundTripper) optimized for
// accessing the data specified by key.
//
// If key is not registered to any nodes, ErrNoNodesForKey is returned.
func (c *Client) TransportForKey(key string, underlying http.RoundTripper) (http.RoundTripper, error) {
	nodes, err := c.NodesForKey(key)
	if err != nil {
		return nil, err
	}

	if len(nodes) == 0 {
		return nil, ErrNoNodesForKey
	}

	if underlying == nil {
		underlying = http.DefaultTransport
	}
	return &keyTransport{key, nodes, c, underlying}, nil
}

type keyTransport struct {
	key       string
	nodes     []string
	c         *Client
	transport http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t *keyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request so we can modify the URL.
	req2 := *req

	// Copy over everything important but the URL host (because we'll try different hosts).
	req2.URL = &url.URL{
		Scheme:   req.URL.Scheme,
		Path:     req.URL.Path,
		RawQuery: req.URL.RawQuery,
		Fragment: req.URL.Fragment,
	}
	if req2.URL.Scheme == "" {
		req2.URL.Scheme = "http"
	}

	for i, node := range t.nodes {
		// TODO(sqs): this code assumes the node is a "host:port".
		req2.URL.Host = node

		resp, err := t.transport.RoundTrip(&req2)
		if err == nil && (resp.StatusCode >= 200 && resp.StatusCode <= 399) {
			return resp, err
		}

		if err == nil {
			defer resp.Body.Close()
			var body []byte
			body, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				return resp, err
			}
			err = &HTTPError{resp.StatusCode, string(bytes.TrimSpace(body))}
		}

		Log.Printf("Request for %q failed in keyTransport (%s); deregistering node %q from key.", req.URL, err, node)
		if err := t.c.registry.Remove(t.key, node); err != nil {
			return nil, err
		}
		// TODO(sqs): remove node from t.nodes

		if i == len(t.nodes)-1 {
			// no more attempts remaining
			return resp, err
		}
	}
	panic("unreachable")
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("http %d: %s", e.StatusCode, e.Body) }
