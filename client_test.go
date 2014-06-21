package datad

import (
	"reflect"
	"testing"
)

func TestClient_ListAndAddEndpoints(t *testing.T) {
	c := NewClient(NewInMemoryBackend(nil), "/")

	servers, err := c.ListServers()
	if err != nil {
		t.Fatal(err)
	}
	if len(servers) != 0 {
		t.Errorf("got %d initial servers, want 0", len(servers))
	}

	err = c.AddServer("http://example.com")
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"http://example.com"}
	servers, err = c.ListServers()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(servers, want) {
		t.Errorf("got servers == %v, want %v", servers, want)
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
