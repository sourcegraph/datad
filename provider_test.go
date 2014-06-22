package datad

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

type NoopProvider struct{}

func (_ NoopProvider) KeyForPath(path string) (string, error)  { return path }
func (_ NoopProvider) HasKey(key string) (bool, error)         { return false, nil }
func (_ NoopProvider) Keys(keyPrefix string) ([]string, error) { return nil, nil }
func (_ NoopProvider) Update(key string) error                 { return nil }

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

func (m InMemoryKeyVersioner) HasKey(key string) (bool, error) {
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}
	v, present := m[key]
	if !present {
		return false, ErrKeyNotExist
	}
	return true, nil
}

func (m InMemoryKeyVersioner) Keys(keyPrefix string) ([]string, error) {
	if !strings.HasPrefix(keyPrefix, "/") {
		keyPrefix = "/" + keyPrefix
	}
	if !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}
	var subkeys []string
	for k, v := range m {
		if strings.HasPrefix(k, keyPrefix) {
			subkeys = append(subkeys, strings.TrimPrefix(k, keyPrefix))
		}
	}
	return subkeys, nil
}

type FakeProvider struct{ InMemoryKeyVersioner }

func (p FakeProvider) KeyForPath(path string) (string, error) {
	return path, nil
}

func (p FakeProvider) Update(key string) error {
	_, err := p.HasKey(key)
	return err
}

func testProvider(t *testing.T, p Provider) {
	keys, err := p.Keys("/")
	if err != nil {
		t.Fatal(err)
	}
	if wantKeys := []string{"k0", "k1"}; !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("got KeyVersions == %v, want %v", keys, wantKeys)
	}

	err = p.Update("k0")
	if err != nil {
		t.Error(err)
	}

	present, err := p.HasKey("k0")
	if err != nil {
		t.Fatal(err)
	}
	if !present {
		t.Errorf("!HasKey")
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
func newProviderClientTest() (*ProviderClient, *http.ServeMux, *httptest.Server) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	url, _ := url.Parse(server.URL)
	c := NewProviderClient(url, nil)
	return c, mux, server
}

func TestProviderClient_HasKey(t *testing.T) {
	c, mux, s := newProviderClientTest()
	defer s.Close()

	mux.HandleFunc("/my/key", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
	})

	present, err := c.HasKey("my/key")
	if err != nil {
		t.Error(err)
	}
	if !present {
		t.Error("!present")
	}
}

func TestProviderClient_Update(t *testing.T) {
	c, mux, s := newProviderClientTest()
	defer s.Close()

	mux.HandleFunc("/my/key", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
	})

	err := c.Update("my/key")
	if err != nil {
		t.Error(err)
	}
}

func testMethod(t *testing.T, r *http.Request, want string) {
	if want != r.Method {
		t.Errorf("Request method = %v, want %v", r.Method, want)
	}
}
