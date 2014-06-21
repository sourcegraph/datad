package datad

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

type datum struct{ value, version string }

// InMemoryProvider is maps keys to their version.
type InMemoryData map[string]datum

func NewInMemoryData(data map[string]datum) InMemoryData {
	if data == nil {
		data = make(map[string]datum)
	} else {
		// Ensure all paths begin with '/'.
		for k, d := range data {
			if !strings.HasPrefix(k, "/") {
				delete(data, k)
				data["/"+k] = d
			}
		}
	}
	return InMemoryData(data)
}

func (m InMemoryData) KeyVersion(key string) (string, error) {
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}
	d, present := m[key]
	if !present {
		return "", ErrKeyNotExist
	}
	return d.version, nil
}

func (m InMemoryData) KeyVersions(keyPrefix string) (map[string]string, error) {
	if !strings.HasPrefix(keyPrefix, "/") {
		keyPrefix = "/" + keyPrefix
	}
	if !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}
	subkvs := make(map[string]string)
	for k, d := range m {
		if strings.HasPrefix(k, keyPrefix) {
			subkvs[strings.TrimPrefix(k, keyPrefix)] = d.version
		}
	}
	return subkvs, nil
}

type FakeServer struct {
	InMemoryData
}

func NewFakeServer(data map[string]datum) FakeServer {
	return FakeServer{NewInMemoryData(data)}
}

func (s FakeServer) Update(key, version string) error {
	// simulate some computation or remote data fetch
	ver, _ := strconv.Atoi(version)

	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}
	s.InMemoryData[key] = datum{"val" + string('A'+ver), version}

	if *debug {
		log.Printf("data[%q] = %+v", key, s.InMemoryData[key])
	}

	return nil
}

func (s FakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d, present := s.InMemoryData[r.URL.Path]
	if !present {
		http.Error(w, ErrKeyNotExist.Error(), http.StatusNotFound)
	}
	w.Write([]byte(d.value))
}

func TestIntegration(t *testing.T) {
	data := map[string]datum{
		"/alice": {"valA", "0"},
		"/bob":   {"valB", "1"},
	}
	fakeServer := NewFakeServer(data)

	dataServer := httptest.NewServer(fakeServer)
	defer dataServer.Close()

	providerServer := httptest.NewServer(NewProviderHandler(fakeServer))
	defer providerServer.Close()

	c := NewClient(NewInMemoryBackend(nil), "/")

	// Add the server.
	err := c.AddProvider(providerServer.URL, dataServer.URL)
	if err != nil {
		t.Fatal(err)
	}

	// Check that it was added.
	providers, err := c.ListProviders()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{providerServer.URL}; !reflect.DeepEqual(providers, want) {
		t.Errorf("got providers == %v, want %v", providers, want)
	}

	// Check that the key is unroutable because although it exists on the
	// provider, the provider has not yet synced to the registry (we call
	// RegisterKeysOnServer below).
	_, err = c.DataURL("/alice")
	if err != ErrNoProviderForKey {
		t.Error(err)
	}

	// Register the server's existing data.
	err = c.RegisterKeysOnServer(providerServer.URL)
	if err != nil {
		t.Fatal(err)
	}

	// After calling RegisterKeysOnServer, the key should be routable.
	dataURL, err := c.DataURL("/alice")
	if err != nil {
		t.Fatal(err)
	}
	if want := dataServer.URL; dataURL.String() != want {
		t.Errorf("got DataURL == %q, want %q", dataURL, want)
	}

	dataTransport, err := c.DataTransport("/alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	val := httpGet("DataTransport", t, dataTransport, "/alice")
	if want := "valA"; val != want {
		t.Errorf("got /alice == %q, want %q", val, want)
	}
}

func httpGet(label string, t *testing.T, transport http.RoundTripper, url string) string {
	c := &http.Client{Transport: transport}
	resp, err := c.Get(url)
	if err != nil {
		t.Fatalf("%s (%s): %s", label, url, err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s (%s): %s", label, url, err)
	}
	return string(body)
}
