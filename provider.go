package datad

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

// A Provider makes data accessible to the datad cluster.
type Provider interface {
	// HasKey returns whether this provider has the underlying data for key. If
	// not, it returns the error ErrKeyNotExist.
	HasKey(key string) (bool, error)

	// Keys returns a list of keys under keyPrefix.
	Keys(keyPrefix string) ([]string, error)

	// Update performs a synchronous update of this key's data from the
	// underlying data source. If key does not exist in this provider, it will
	// be created.
	Update(key string) error
}

type providerHandler struct {
	Provider
	keyFunc KeyFunc
	inner   http.Handler
}

func (h providerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key, err := h.keyFunc(r.URL.Path)
	if err != nil {
		http.Error(w, "could not determine key from request path", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET", "HEAD":
		if present, err := h.HasKey(key); err != nil && err != ErrKeyNotExist {
			log.Printf("serveInner [%s]: %s", r.URL.RequestURI(), err)
			http.Error(w, "failed to determine if this provider has key", http.StatusInternalServerError)
			return
		} else if err == ErrKeyNotExist || !present {
			http.Error(w, "no such key found on this provider", http.StatusNotFound)
			return
		}

	case "POST", "PUT":
		if r.URL.Path == key {
			// Only update if the request is for the key itself, not some
			// sub-resource of the key.
			err := h.Update(key)
			if err != nil {
				http.Error(w, "failed to update key on provider", http.StatusInternalServerError)
				return
			}
		}
	}

	h.inner.ServeHTTP(w, r)
}

func providerErrorHTTPStatus(err error) int {
	if err == ErrKeyNotExist {
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}

// ProviderClient communicates with a provider's HTTP API.
type ProviderClient struct {
	baseURL    *url.URL
	httpClient *http.Client
}

// NewProviderClient returns a Provider that performs actions against a
// provider's HTTP API.
func NewProviderClient(providerURL *url.URL, httpClient *http.Client) *ProviderClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &ProviderClient{providerURL, httpClient}
}

// HasKey returns whether the provider has underlying data for key.
func (c *ProviderClient) HasKey(key string) (bool, error) {
	key = slash(key)

	url := c.baseURL.ResolveReference(&url.URL{Path: key})
	resp, err := c.do("GET", url.String())
	if err != nil {
		if err, ok := err.(*HTTPError); ok && err.StatusCode == http.StatusNotFound {
			return false, ErrKeyNotExist
		}
		return false, err
	}
	defer resp.Body.Close()

	return true, nil
}

// Update updates key to the latest version from the original data source.
func (c *ProviderClient) Update(key string) error {
	key = slash(key)

	url := c.baseURL.ResolveReference(&url.URL{Path: key})
	resp, err := c.do("PUT", url.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *ProviderClient) do(method, url string) (*http.Response, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return resp, err
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, &HTTPError{resp.StatusCode, string(body)}
	}

	return resp, nil
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("http %d: %s", e.StatusCode, e.Body) }
