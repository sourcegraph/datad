package datad

import (
	"reflect"
	"strings"
	"testing"
)

type datum struct {
	value   string
	version string
}

type InMemoryProvider map[string]datum

func NewInMemoryProvider(m map[string]datum) InMemoryProvider {
	if m == nil {
		m = make(map[string]datum)
	}
	return InMemoryProvider(m)
}

func (p InMemoryProvider) KeyVersions(keyPrefix string) (map[string]string, error) {
	if !strings.HasPrefix(keyPrefix, "/") {
		keyPrefix = "/" + keyPrefix
	}
	if !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}
	subkvs := make(map[string]string)
	for k, v := range p {
		if strings.HasPrefix(k, keyPrefix) {
			subkvs[strings.TrimPrefix(k, keyPrefix)] = v.version
		}
	}
	return subkvs, nil
}

func (p InMemoryProvider) KeyVersion(key string) (string, error) {
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}
	d, present := p[key]
	if !present {
		return "", ErrKeyNotExist
	}
	return d.version, nil
}

type FakeProvider struct{ InMemoryProvider }

func (p FakeProvider) Update(key, version string) error {
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}

	// simulate some computation or remote data fetch
	p.InMemoryProvider[key] = datum{value: strings.ToUpper(version), version: version}
	return nil
}

func testProvider(t *testing.T, p Provider) {
	kvs, err := p.KeyVersions("/")
	if err != nil {
		t.Fatal(err)
	}
	if wantKVs := map[string]string{"k0": "a", "k1": "b", "k2": "c"}; !reflect.DeepEqual(kvs, wantKVs) {
		t.Errorf("got KeyVersions == %v, want %v", kvs, wantKVs)
	}

	err = p.Update("k0", "x")
	if err != nil {
		t.Error(err)
	}

	kv, err := p.KeyVersion("k0")
	if err != nil {
		t.Fatal(err)
	}
	if want := "x"; kv != want {
		t.Errorf("got KeyVersion == %q, want %q", kv, want)
	}
}

func TestFakeProvider(t *testing.T) {
	data := map[string]datum{
		"/k0": {"A", "a"},
		"/k1": {"B", "b"},
		"/k2": {"C", "c"},
	}

	testProvider(t, FakeProvider{NewInMemoryProvider(data)})
}
