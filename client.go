package datad

import (
	"net/url"
	"strings"
)

// A Client routes requests for data.
type Client struct {
	backend Backend

	reg *Registry

	serversPrefix string
}

func NewClient(b Backend, keyPrefix string) *Client {
	keyPrefix = strings.TrimSuffix(keyPrefix, "/")
	return &Client{
		backend:       b,
		reg:           NewRegistry(b, keyPrefix+"/registry"),
		serversPrefix: keyPrefix + "/servers",
	}
}

// ListProviders returns a list of provider server URLs in the cluster.
func (c *Client) ListProviders() ([]string, error) {
	servers, err := c.backend.List(c.serversPrefix + "/")
	if err != nil {
		return nil, err
	}

	for i, s := range servers {
		servers[i], err = decodeURLForKey(s)
		if err != nil {
			return nil, err
		}
	}

	return servers, nil
}

// AddProvider adds an server to the cluster, making it available to be assigned
// data.
func (c *Client) AddProvider(providerURL, dataURL string) error {
	return c.backend.Set(c.serversPrefix+"/"+encodeURLForKey(providerURL), dataURL)
}

// ProviderDataURL gets the data URL corresponding to the provider (added with
// AddProvider).
func (c *Client) ProviderDataURL(providerURL string) (string, error) {
	return c.backend.Get(c.serversPrefix + "/" + encodeURLForKey(providerURL))
}

// RegisterKeysOnServer examines the keys provided by a server and adds them to
// the central registry.
func (c *Client) RegisterKeysOnServer(providerURL string) error {
	pc, err := c.Provider(providerURL)
	if err != nil {
		return err
	}

	kvs, err := pc.KeyVersions("/")
	if err != nil {
		return err
	}

	for k, ver := range kvs {
		if !strings.HasPrefix(k, "/") {
			k = "/" + k
		}
		err := c.reg.AddProvider(k, providerURL, ver)
		if err != nil {
			return err
		}

		// TODO(sqs): check if this provider's version differs from that of the
		// other providers
	}

	return nil
}

// DataURL returns a URL to a piece of data specified by key, on a data server
// that has previously been added to the cluster.
//
// TODO(sqs): add consistent param (if true, forces an update on all providers,
// and perhaps just takes the one that returns first)
//
// TODO(sqs): add create param (to create nonexistent keys)
func (c *Client) DataURL(key string) (*url.URL, error) {
	pvs, err := c.reg.ProviderVersions(key)
	if err != nil {
		return nil, err
	}

	if len(pvs) == 0 {
		return nil, ErrNoProviderForKey
	}

	for p, _ := range pvs {
		dataURL, err := c.ProviderDataURL(p)
		if err != nil {
			return nil, err
		}

		u, err := url.Parse(dataURL)
		if err != nil {
			return nil, err
		}
		return u, nil
	}
	panic("unreachable")
}

func (c *Client) Provider(providerURL string) (Provider, error) {
	url, err := url.Parse(providerURL)
	if err != nil {
		return nil, err
	}

	return NewProviderClient(url, nil), nil
}

// encodeURLForKey encodes a URL for use as a single HTTP path component.
func encodeURLForKey(urlStr string) string {
	return url.QueryEscape(urlStr)
}

// decodeURLForKey decodes a URL that was encoded with encodeURLForKey.
func decodeURLForKey(encURLStr string) (string, error) {
	return url.QueryUnescape(encURLStr)
}
