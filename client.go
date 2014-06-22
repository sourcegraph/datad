package datad

import (
	"errors"
	"net/url"
	"strings"
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

var ErrNoProviderForKey = errors.New("key has no provider")

// Providers returns a list of all provider URLs in the cluster.
func (c *Client) ListProviders() ([]string, error) {
	providers, err := c.backend.List(providersPrefix + "/")
	if err != nil {
		return nil, err
	}

	for i, s := range providers {
		providers[i], err = decodeURLForKey(s)
		if err != nil {
			return nil, err
		}
	}

	return providers, nil
}

// KeyProviders returns a list of provider URLs that are registered for key.
func (c *Client) ProvidersForKey(key string) ([]string, error) {
	return c.registry.Providers(key)
}

// // DataURL returns a URL to a piece of data specified by key, on a data server
// // that has previously been added to the cluster.
// //
// // TODO(sqs): add consistent param (if true, forces an update on all providers,
// // and perhaps just takes the one that returns first)
// //
// // TODO(sqs): add create param (to create nonexistent keys)
// func (c *Client) DataURL(key string) (*url.URL, error) {
// 	key = slash(key)

// 	dvs, err := c.DataURLVersions(key)
// 	if err != nil {
// 		return nil, err
// 	}

// 	if len(dvs) == 0 {
// 		return nil, ErrNoProviderForKey
// 	}

// 	for dataURL, _ := range dvs {
// 		return url.Parse(dataURL)
// 	}
// 	panic("unreachable")
// }

// // DataTransport returns an http.RoundTripper that routes HTTP requests to the
// // data server that holds key.
// //
// // TODO(sqs): try all 3 requests and return the first that succeeds?
// //
// // TODO(sqs): add consistent param that ensures all servers are mutually up to
// // date?
// func (c *Client) DataTransport(key string, underlying http.RoundTripper) (http.RoundTripper, error) {
// 	key = slash(key)

// 	dvs, err := c.DataURLVersions(key)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if underlying == nil {
// 		underlying = http.DefaultTransport
// 	}
// 	return &dataTransport{dvs, underlying, key, c}, nil
// }

// type dataTransport struct {
// 	dataURLVersions map[string]string
// 	transport       http.RoundTripper
// 	key             string
// 	c               *Client
// }

// // RoundTrip implements http.RoundTripper.
// func (t *dataTransport) RoundTrip(req *http.Request) (*http.Response, error) {
// 	// Clone the request so we can modify the URL.
// 	req2 := *req

// 	// Only copy over the URL path (because we use different hosts).
// 	req2.URL = &url.URL{
// 		Path:     strings.TrimPrefix(req.URL.Path, "/"), // so it's relative to the data base URL
// 		RawQuery: req.URL.RawQuery,
// 		Fragment: req.URL.Fragment,
// 	}

// 	i := 0
// 	for dataURLStr, _ := range t.dataURLVersions {
// 		dataURL, err := url.Parse(dataURLStr)
// 		if err != nil {
// 			return nil, err
// 		}

// 		req.URL = dataURL.ResolveReference(req.URL)

// 		resp, err := t.transport.RoundTrip(req)
// 		if err == nil && (resp.StatusCode >= 200 && resp.StatusCode <= 399) {
// 			return resp, err
// 		}

// 		if err == nil {
// 			defer resp.Body.Close()
// 			var body []byte
// 			body, err = ioutil.ReadAll(resp.Body)
// 			if err != nil {
// 				return resp, err
// 			}
// 			err = &HTTPError{resp.StatusCode, string(body)}
// 		}

// 		Log.Printf("request for %q failed in DataTransport (%s); deregistering data server %q from key", req.URL, err, dataURLStr)
// 		// TODO(sqs): reregister data server

// 		if i == len(t.dataURLVersions)-1 { // last one
// 			return resp, err
// 		}

// 		i++
// 	}
// 	panic("unreachable")
// }

// ProviderClient returns a client to access metadata about the provider at
// providerURL.
func (c *Client) ProviderClient(providerURL string) (*ProviderClient, error) {
	url, err := url.Parse(providerURL)
	if err != nil {
		return nil, err
	}

	return NewProviderClient(url, nil), nil
}

// encodeURLForKey encodes a URL for use as a single HTTP path component.
func encodeURLForKey(urlStr string) string {
	return url.QueryEscape(strings.Replace(urlStr, "/", "%2F", -1))
}

// decodeURLForKey decodes a URL that was encoded with encodeURLForKey.
func decodeURLForKey(encURLStr string) (string, error) {
	urlStr, err := url.QueryUnescape(encURLStr)
	if err != nil {
		return "", err
	}
	return strings.Replace(urlStr, "%2F", "/", -1), nil
}
