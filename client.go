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

// Update updates key from the data source on the nodes that are registered to
// it. If key is not registered to any nodes, a node is registered for it and
// the key is created on that node.
func (c *Client) Update(key string) (nodes []string, err error) {
	return c.update(key, nil)
}

// update is like Update, but takes a clusterNodes param to avoid requerying
// ClusterNodes each time (for callers who call update many times in a short
// period of time).
func (c *Client) update(key string, clusterNodes []string) (nodes []string, err error) {
	nodes, err = c.NodesForKey(key)
	if err != nil {
		return nil, err
	}

	if len(nodes) == 0 {
		// Register a node for the key.
		if clusterNodes == nil {
			clusterNodes, err = c.NodesInCluster()
			if err != nil {
				return nil, err
			}
		}

		// Try to choose the same node as other clients that might be calling Update on the same key concurrently.
		regNode := clusterNodes[keyBucket(key, len(clusterNodes))]

		Log.Printf("Key to update does not exist yet: %q; registering key to node %s (will trigger update).", key, regNode)

		// TODO(sqs): optimize this by only adding if not exists, and then
		// seeing if it exists (to avoid potentially duplicating work).
		err = c.registry.Add(key, regNode)
		if err != nil {
			return nil, err
		}

		// The call to Add will trigger the update on the node, so we're done.
		return []string{regNode}, nil
	}

	for i, node := range nodes {
		Log.Printf("Triggering update of key %q on node %s (%d/%d)...", key, node, i+1, len(nodes))
		// Each node watches its list of registered keys, so just re-adding it
		// to the registry will trigger an update.
		err = c.registry.Add(key, node)
		if err != nil {
			return nil, err
		}
	}
	Log.Printf("Finished triggering updates of key %q on %d nodes (%v).", key, len(nodes), nodes)

	return nodes, nil
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

	return c.transportForKey(key, underlying, nodes)
}

// transportForkey is like TransportForKey but is optimized for callers who
// already know the nodes that are registered to key.
func (c *Client) transportForKey(key string, underlying http.RoundTripper, nodes []string) (http.RoundTripper, error) {
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
			return resp, nil
		}

		if err == nil {
			defer resp.Body.Close()
			var body []byte
			body, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				return nil, err
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
			return nil, err
		}
	}
	panic("unreachable")
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("http %d: %s", e.StatusCode, e.Body) }
