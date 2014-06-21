package datad

import (
	"reflect"
	"testing"
)

func TestRegistry(t *testing.T) {
	r := NewRegistry(NewInMemoryBackend(nil), "/")

	pvs, err := r.ProviderVersions("k")
	if err != nil {
		t.Fatal(err)
	}
	if len(pvs) != 0 {
		t.Errorf("got pvs == %v, want empty", pvs)
	}

	err = r.AddProvider("k", "p", "v")
	if err != nil {
		t.Fatal(err)
	}

	pvs, err = r.ProviderVersions("k")
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]string{"p": "v"}; !reflect.DeepEqual(pvs, want) {
		t.Errorf("got ProviderVersions == %v, want %v", pvs, want)
	}

	pv, err := r.ProviderVersion("k", "p")
	if err != nil {
		t.Fatal(err)
	}
	if want := "v"; pv != want {
		t.Errorf("got ProviderVersion == %q, want %q", pv, want)
	}
}
