package datad

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
)

// A Provider makes data accessible to the datad cluster.
type Provider interface {
	// KeyVersion returns key's version. It returns ErrKeyNotExist if no such
	// key exists.
	KeyVersion(key string) (string, error)

	// KeyVersions returns a map of keys under keyPrefix to their version.
	KeyVersions(keyPrefix string) (map[string]string, error)

	// Update performs a synchronous update of key's value to version from the
	// underlying data source. If key does not exist in this provider, it will
	// be created.
	Update(key, version string) error
}

var ErrNoProviderForKey = errors.New("key has no provider")

type providerHandler struct{ Provider }

func NewProviderHandler(p Provider) http.Handler {
	h := providerHandler{p}
	m := NewProviderRouter()
	m.Get(keyVersionRoute).HandlerFunc(h.serveKeyVersion)
	m.Get(keyVersionsRoute).HandlerFunc(h.serveKeyVersions)
	m.Get(updateRoute).HandlerFunc(h.serveUpdate)
	m.Path("/").Methods("GET").HandlerFunc(h.serveRoot)
	return m
}

func (h providerHandler) serveKeyVersion(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["Key"]
	version, err := h.KeyVersion(key)
	if err != nil {
		http.Error(w, err.Error(), providerErrorHTTPStatus(err))
	}

	w.Header().Set("content-type", "text/plain")
	w.Write([]byte(version))
}

func (h providerHandler) serveKeyVersions(w http.ResponseWriter, r *http.Request) {
	keyPrefix := mux.Vars(r)["KeyPrefix"]
	kvs, err := h.KeyVersions(keyPrefix)
	if err != nil {
		http.Error(w, err.Error(), providerErrorHTTPStatus(err))
	}

	w.Header().Set("content-type", "application/json")
	err = json.NewEncoder(w).Encode(kvs)
	if err != nil {
		log.Printf("datad: error writing HTTP response for key versions under prefix %q: %s", keyPrefix, err)
	}
}

func (h providerHandler) serveUpdate(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["Key"]
	version := r.URL.Query().Get("version")
	err := h.Update(key, version)
	if err != nil {
		http.Error(w, err.Error(), providerErrorHTTPStatus(err))
	}
}

func (h providerHandler) serveRoot(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "datad provider %s\n", version)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "get key version:      GET /keys/KEY\n")
	fmt.Fprintln(w, "list subkey versions: GET /keys/KEY-PREFIX/  (trailing slash required)\n")
	fmt.Fprintln(w, "update key version:   PUT /keys/KEY?version=VERSION\n")
}

func providerErrorHTTPStatus(err error) int {
	if err == ErrKeyNotExist {
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}

const (
	keyVersionsRoute = "KeyVersions"
	keyVersionRoute  = "KeyVersion"
	updateRoute      = "Update"
)

// NewProviderRouter creates a HTTP router for the provider HTTP API. It doesn't
// attach any handlers to the routes.
func NewProviderRouter() *mux.Router {
	m := mux.NewRouter()
	m.StrictSlash(true)
	m.Path("/keys{KeyPrefix:.*}/").Methods("GET").Name(keyVersionsRoute)
	m.Path("/keys{Key:.+}").Methods("GET").Name(keyVersionRoute)
	m.Path("/keys{Key:.+}").Methods("PUT").Queries("version", "").Name(updateRoute)
	return m
}

// providerClient communicates with a provider's HTTP API.
type providerClient struct {
	baseURL    *url.URL
	httpClient *http.Client
}

var providerClientRouter = NewProviderRouter()

// NewProviderClient returns a Provider that performs actions against a
// provider's HTTP API.
func NewProviderClient(providerURL *url.URL, httpClient *http.Client) Provider {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &providerClient{providerURL, httpClient}
}

func (c *providerClient) KeyVersion(key string) (string, error) {
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}

	url, err := c.url(keyVersionRoute, map[string]string{"Key": key}, nil)
	if err != nil {
		return "", err
	}

	resp, err := c.do("GET", url.String())
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	ver, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(ver), nil
}

func (c *providerClient) KeyVersions(keyPrefix string) (map[string]string, error) {
	if !strings.HasPrefix(keyPrefix, "/") {
		keyPrefix = "/" + keyPrefix
	}
	if !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix = keyPrefix + "/"
	}

	url, err := c.url(keyVersionsRoute, map[string]string{"KeyPrefix": keyPrefix}, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.do("GET", url.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var kvs map[string]string
	err = json.NewDecoder(resp.Body).Decode(&kvs)
	return kvs, err
}

func (c *providerClient) Update(key, version string) error {
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}

	url, err := c.url(updateRoute, map[string]string{"Key": key}, url.Values{"version": []string{version}})
	if err != nil {
		return err
	}

	resp, err := c.do("PUT", url.String())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *providerClient) do(method, url string) (*http.Response, error) {
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

func (c *providerClient) url(routeName string, routeVars map[string]string, query url.Values) (*url.URL, error) {
	route := providerClientRouter.Get(routeName)
	if route == nil {
		panic("url: no route named '" + routeName + "'")
	}

	routeVarsList := make([]string, 2*len(routeVars))
	i := 0
	for name, val := range routeVars {
		routeVarsList[i*2] = name
		routeVarsList[i*2+1] = val
		i++
	}
	url, err := route.URL(routeVarsList...)
	if err != nil {
		return nil, err
	}

	url = c.baseURL.ResolveReference(url)

	// make the route URL path relative to BaseURL by trimming the leading "/"
	url.Path = strings.TrimPrefix(url.Path, "/")

	url.RawQuery = query.Encode()

	return url, nil
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string { return e.Body }
