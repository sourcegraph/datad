package datad

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// InMemoryProvider is maps keys to their version.
type InMemoryKeyVersioner map[string]string

func NewInMemoryKeyVersioner(keyVersions map[string]string) InMemoryKeyVersioner {
	if keyVersions == nil {
		keyVersions = make(map[string]string)
	} else {
		// Ensure all paths begin with '/'.
		for k, v := range keyVersions {
			if !strings.HasPrefix(k, "/") {
				delete(keyVersions, k)
				keyVersions["/"+k] = v
			}
		}
	}
	return InMemoryKeyVersioner(keyVersions)
}

func (m InMemoryKeyVersioner) KeyVersion(key string) (string, error) {
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}
	v, present := m[key]
	if !present {
		return "", ErrKeyNotExist
	}
	return v, nil
}

func (m InMemoryKeyVersioner) KeyVersions(keyPrefix string) (map[string]string, error) {
	if !strings.HasPrefix(keyPrefix, "/") {
		keyPrefix = "/" + keyPrefix
	}
	if !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}
	subkvs := make(map[string]string)
	for k, v := range m {
		if strings.HasPrefix(k, keyPrefix) {
			subkvs[strings.TrimPrefix(k, keyPrefix)] = v
		}
	}
	return subkvs, nil
}

type FakeProvider struct{ InMemoryKeyVersioner }

func (p FakeProvider) Update(key, version string) error {
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}

	// simulate some computation or remote data fetch
	p.InMemoryKeyVersioner[key] = version
	return nil
}

func testProvider(t *testing.T, p Provider) {
	kvs, err := p.KeyVersions("/")
	if err != nil {
		t.Fatal(err)
	}
	if wantKVs := map[string]string{"k0": "0", "k1": "10"}; !reflect.DeepEqual(kvs, wantKVs) {
		t.Errorf("got KeyVersions == %v, want %v", kvs, wantKVs)
	}

	err = p.Update("k0", "2")
	if err != nil {
		t.Error(err)
	}

	kv, err := p.KeyVersion("k0")
	if err != nil {
		t.Fatal(err)
	}
	if want := "2"; kv != want {
		t.Errorf("got KeyVersion == %q, want %q", kv, want)
	}
}

func TestFakeProvider(t *testing.T) {
	keyVersions := map[string]string{
		"/k0": "0",
		"/k1": "10",
	}
	testProvider(t, FakeProvider{NewInMemoryKeyVersioner(keyVersions)})
}

// newProviderClientTest returns a client configured to use a test server. Use
// the ServeMux to create mock handlers. Call Close() on the *httptest.Server
// when done.
func newProviderClientTest() (*providerClient, *http.ServeMux, *httptest.Server) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	url, _ := url.Parse(server.URL)
	c := NewProviderClient(url, nil)
	return c.(*providerClient), mux, server
}

func TestProviderClient_KeyVersion(t *testing.T) {
	c, mux, s := newProviderClientTest()
	defer s.Close()

	mux.HandleFunc("/keys/k", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		fmt.Fprint(w, "v")
	})

	v, err := c.KeyVersion("k")
	if err != nil {
		t.Error(err)
	}
	if want := "v"; v != want {
		t.Errorf("got KeyVersion == %q, want %q", v, want)
	}
}

func TestProviderClient_KeyVersions(t *testing.T) {
	c, mux, s := newProviderClientTest()
	defer s.Close()

	mux.HandleFunc("/keys/", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		fmt.Fprint(w, `{"k0":"v0","k1":"v1"}`)
	})

	kvs, err := c.KeyVersions("")
	if err != nil {
		t.Error(err)
	}
	if want := map[string]string{"k0": "v0", "k1": "v1"}; !reflect.DeepEqual(kvs, want) {
		t.Errorf("got KeyVersions == %q, want %q", kvs, want)
	}
}

func TestProviderClient_Update(t *testing.T) {
	c, mux, s := newProviderClientTest()
	defer s.Close()

	wantVersion := "v2"

	mux.HandleFunc("/keys/k", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		if v := r.URL.Query().Get("version"); v != wantVersion {
			t.Errorf("got version == %q, want %q", v, wantVersion)
		}
	})

	err := c.Update("k", wantVersion)
	if err != nil {
		t.Error(err)
	}
}

func testMethod(t *testing.T, r *http.Request, want string) {
	if want != r.Method {
		t.Errorf("Request method = %v, want %v", r.Method, want)
	}
}
