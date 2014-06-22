package datad

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type datum struct{ value string }

type data map[string]datum

func newData(m map[string]datum) data {
	if m == nil {
		m = make(map[string]datum)
	} else {
		// Ensure all paths begin with '/'.
		for k, d := range m {
			if !strings.HasPrefix(k, "/") {
				delete(m, k)
				m["/"+k] = d
			}
		}
	}
	return data(m)
}

func (m data) HasKey(key string) (bool, error) {
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}
	d, present := m[key]
	if !present {
		return false, ErrKeyNotExist
	}
	return true, nil
}

func (m data) Keys(keyPrefix string) ([]string, error) {
	if !strings.HasPrefix(keyPrefix, "/") {
		keyPrefix = "/" + keyPrefix
	}
	if !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}
	var subkeys []string
	for k, d := range m {
		if strings.HasPrefix(k, keyPrefix) {
			subkeys = append(subkeys, strings.TrimPrefix(k, keyPrefix))
		}
	}
	return subkeys, nil
}

type noopUpdateProvider struct{ data }

func (p noopUpdateProvider) Update(key string) error {
	if present, err := p.HasKey(key); err != nil {
		return err
	}
	return nil
}

type dataHandler map[string]datum

func (h dataHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d, present := h[slash(r.URL.Path)]
	if !present {
		http.Error(w, ErrKeyNotExist.Error(), http.StatusNotFound)
		return
	}
	w.Write([]byte(d.value))
}

func TestIntegration_Simple(t *testing.T) {
	data := data{"/key": {"val"}}
	b := NewInMemoryBackend(nil)

	ds := httptest.NewServer(dataHandler(data))
	defer ds.Close()

	pub := NewPublisher(ds.URL, b, noopUpdateProvider{data})
	pub.Start()
	defer pub.Stop()

	c := NewClient(b)

	// Check that the provider has been published.
	providers, err := c.ListProviders()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{ds.URL}; !reflect.DeepEqual(providers, want) {
		t.Errorf("got providers == %v, want %v", providers, want)
	}

	// Check that the key is unroutable because although it exists on the
	// provider, the provider has not yet synced to the registry (we call
	// RegisterKeysOnProvider below).
	_, err = c.DataURL("/alice")
	if err != ErrNoProviderForKey {
		t.Error(err)
	}

	// Register the server's existing data.
	err = c.RegisterKeysOnProvider(providerServer.URL)
	if err != nil {
		t.Fatal(err)
	}

	// After calling RegisterKeysOnProvider, the key should be routable.
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

// Test that a key is deregistered from a data server if the data server
// is failing (HTTP errors are being returned).
func TestIntegration_FailingDataServer(t *testing.T) {
	t.Skip("not yet implemented")

	// The "/alice" key will be registered to a provider that's no longer up.
	fakeServer := NewFakeServer(map[string]datum{"/alice": {"valA", "0"}})

	dataServer := httptest.NewServer(fakeServer)
	defer dataServer.Close()
	providerServer := httptest.NewServer(NewProviderHandler(fakeServer))
	defer providerServer.Close()

	fakeServer2 := NewFakeServer(nil)
	providerServer2 := httptest.NewServer(NewProviderHandler(fakeServer2))
	defer providerServer2.Close()
	failingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "dummy error", http.StatusInternalServerError)
	}))
	defer failingServer.Close()

	c := NewClient(NewInMemoryBackend(nil))

	// Add the failing provider.
	err := c.AddProvider(providerServer2.URL, failingServer.URL)
	if err != nil {
		t.Fatal(err)
	}

	// Register the "/alice" key to the failing provider (always returns HTTP 500).
	err = c.reg.AddProvider("/alice", providerServer2.URL, "0")
	if err != nil {
		t.Fatal(err)
	}

	// Add the provider that is up. (But don't register "/alice" to it.)
	err = c.AddProvider(providerServer.URL, dataServer.URL)
	if err != nil {
		t.Fatal(err)
	}

	// Test that DataURL returns the URL to the failing data server. (It does not
	// check for liveness of the data server.)
	dataURL, err := c.DataURL("/alice")
	if err != nil {
		t.Error(err)
	}
	if dataURL.String() != failingServer.URL {
		t.Errorf("got DataURL == %q, want %q", dataURL, failingServer.URL)
	}

	// Test that the DataTransport will reassign "/alice" to another provider
	// when it notices that the request to the failing server fails.
	dataTransport, err := c.DataTransport("/alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = (&http.Client{Transport: dataTransport}).Get("/alice")
	if err == nil || !strings.Contains(err.Error(), "dummy error") {
		t.Errorf("got DataTransport get error %v, want \"dummy error\"", err)
	}

	// Test that the "/alice" key is unregistered from the failing server and
	// registered to the live server.
	pvs, err := c.reg.ProviderVersions("/alice")
	if err != nil {
		t.Error(err)
	}
	if want := map[string]string{dataServer.URL: "0"}; !reflect.DeepEqual(pvs, want) {
		t.Errorf("got ProviderVersions == %v, want %v", pvs, want)
	}
}

func TestIntegration_TwoProviders(t *testing.T) {
	data1 := map[string]datum{"/alice": {"valA", "0"}}
	fakeServer1 := NewFakeServer(data1)
	dataServer1 := httptest.NewServer(fakeServer1)
	defer dataServer1.Close()
	providerServer1 := httptest.NewServer(NewProviderHandler(fakeServer1))
	defer providerServer1.Close()

	data2 := map[string]datum{"/bob": {"valB", "1"}}
	fakeServer2 := NewFakeServer(data2)
	dataServer2 := httptest.NewServer(fakeServer2)
	defer dataServer2.Close()
	providerServer2 := httptest.NewServer(NewProviderHandler(fakeServer2))
	defer providerServer2.Close()

	c := NewClient(NewInMemoryBackend(nil))

	// Add the servers.
	err := c.AddProvider(providerServer1.URL, dataServer1.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = c.AddProvider(providerServer2.URL, dataServer2.URL)
	if err != nil {
		t.Fatal(err)
	}

	// Check that they were added.
	providers, err := c.ListProviders()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(providers)
	wantProviders := []string{providerServer1.URL, providerServer2.URL}
	sort.Strings(wantProviders)
	if !reflect.DeepEqual(providers, wantProviders) {
		t.Errorf("got providers == %v, want %v", providers, wantProviders)
	}

	// Register the servers' existing data.
	err = c.RegisterKeysOnProvider(providerServer1.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = c.RegisterKeysOnProvider(providerServer2.URL)
	if err != nil {
		t.Fatal(err)
	}

	// After calling RegisterKeysOnProvider, the keys should be routable.

	// "/alice" is on server 1.
	dataURL, err := c.DataURL("/alice")
	if err != nil {
		t.Fatal(err)
	}
	if want := dataServer1.URL; dataURL.String() != want {
		t.Errorf("got DataURL == %q, want %q", dataURL, want)
	}

	// "/bob" is on server 2.
	dataURL, err = c.DataURL("/bob")
	if err != nil {
		t.Fatal(err)
	}
	if want := dataServer2.URL; dataURL.String() != want {
		t.Errorf("got DataURL == %q, want %q", dataURL, want)
	}
}

func TestIntegration_TwoProviders_DifferentVersions(t *testing.T) {
	t.Skip("not yet implemented")

	data1 := map[string]datum{"/alice": {"valA", "0"}}
	fakeServer1 := NewFakeServer(data1)
	dataServer1 := httptest.NewServer(fakeServer1)
	defer dataServer1.Close()
	providerServer1 := httptest.NewServer(NewProviderHandler(fakeServer1))
	defer providerServer1.Close()

	data2 := map[string]datum{"/alice": {"valB", "1"}}
	fakeServer2 := NewFakeServer(data2)
	dataServer2 := httptest.NewServer(fakeServer2)
	defer dataServer2.Close()
	providerServer2 := httptest.NewServer(NewProviderHandler(fakeServer2))
	defer providerServer2.Close()

	c := NewClient(NewInMemoryBackend(nil))

	// Add the servers.
	err := c.AddProvider(providerServer1.URL, dataServer1.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = c.AddProvider(providerServer2.URL, dataServer2.URL)
	if err != nil {
		t.Fatal(err)
	}

	// Register the servers' existing data.
	err = c.RegisterKeysOnProvider(providerServer1.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = c.RegisterKeysOnProvider(providerServer2.URL)
	if err != nil {
		t.Fatal(err)
	}

	// Server 1 has version 0 of "/alice" and server 2 has version 1 of
	// "/alice". TODO(sqs): The second call to RegisterKeysOnProvider recognize
	// this and trigger an update on server.

	// After the updates, they should both be at version 1 (TODO(sqs): or maybe they both
	// update from the source, since it's hard to know which is the newer one).
	dvs, err := c.DataURLVersions("/alice")
	if err != nil {
		t.Fatal(err)
	}
	for dataURL, ver := range dvs {
		if want := "1"; ver != want {
			t.Errorf("got dataURL %q version == %q, want %q", dataURL, ver, want)
		}
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
