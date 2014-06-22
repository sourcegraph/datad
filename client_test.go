package datad

import (
	"reflect"
	"testing"
)

func TestClient_Providers(t *testing.T) {
	b := NewInMemoryBackend(nil)
	c := NewClient(b)
	pub := NewPublisher("http://example.com", b, NoopProvider{})

	providers, err := c.ListProviders()
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 0 {
		t.Errorf("got %d initial providers, want 0", len(providers))
	}

	err = pub.addProviderToCluster()
	if err != nil {
		t.Fatal(err)
	}

	providers, err = c.ListProviders()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"http://example.com"}; !reflect.DeepEqual(providers, want) {
		t.Errorf("got providers == %v, want %v", providers, want)
	}
}
