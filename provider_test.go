package datad

import (
	"reflect"
	"testing"
)

func testProvider(t *testing.T, p Provider) {
	keys, err := p.Keys("/")
	if err != nil {
		t.Fatal(err)
	}
	if wantKeys := []string{"k0", "k1"}; !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("got Keys == %v, want %v", keys, wantKeys)
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
	m := map[string]datum{
		"/k0": {"a"},
		"/k1": {"b"},
	}
	testProvider(t, noopUpdateProvider{newData(m)})
}
