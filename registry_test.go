package datad

import "testing"

func TestRegistry(t *testing.T) {
	r := newRegistry(NewInMemoryBackend(nil), "/")

	_, err := r.serverForKey("k")
	if err != errNoServerForKey {
		t.Fatal(err)
	}

	err = r.setServerForKey("k", "e")
	if err != nil {
		t.Fatal(err)
	}

	server, err := r.serverForKey("k")
	if err != nil {
		t.Fatal(err)
	}
	if want := "e"; server != want {
		t.Errorf("before SetServerForKey, got ServerForKey == %q, want %q", server, want)
	}
}
