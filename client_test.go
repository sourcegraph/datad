package datad

import (
	"reflect"
	"testing"
)

func TestClient_Providers(t *testing.T) {
	c := NewClient(NewInMemoryBackend(nil))

	providers, err := c.ListProviders()
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 0 {
		t.Errorf("got %d initial providers, want 0", len(providers))
	}

	err = c.AddProvider("http://provider.example.com", "http://data.example.com")
	if err != nil {
		t.Fatal(err)
	}

	providers, err = c.ListProviders()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"http://provider.example.com"}; !reflect.DeepEqual(providers, want) {
		t.Errorf("got providers == %v, want %v", providers, want)
	}

	dataURL, err := c.ProviderDataURL("http://provider.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if want := "http://data.example.com"; dataURL != want {
		t.Errorf("got ProviderDataURL == %q, want %q", dataURL, want)
	}
}

// func TestClient_ExistingItem(t *testing.T) {
// 	c := NewClient(NewInMemoryBackend(nil), "/")

// 	err := c.reg.setServerForKey("k", "http://example.com")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	server, err := c.GetServer("k")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	want := "http://example.com"
// 	if server != want {
// 		t.Errorf("got server == %q, want %q", server, want)
// 	}
// }
