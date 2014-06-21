package datad

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

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
	key := r.URL.Path
	version, err := h.KeyVersion(key)
	if err != nil {
		http.Error(w, err.Error(), providerErrorHTTPStatus(err))
	}

	w.Header().Set("content-type", "text/plain")
	w.Write([]byte(version))
}

func (h providerHandler) serveKeyVersions(w http.ResponseWriter, r *http.Request) {
	keyPrefix := r.URL.Path
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
	key := r.URL.Path
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
	m.Path("/keys/{Key:.+}/").Methods("GET").Name(keyVersionsRoute)
	m.Path("/keys/{Key:.+}").Methods("GET").Name(keyVersionRoute)
	m.Path("/keys/{Key:.+}").Methods("PUT").Queries("version", "").Name(updateRoute)
	return m
}
