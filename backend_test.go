package datad

import (
	"reflect"
	"testing"
)

func testBackend(t *testing.T, b Backend) {
	v, err := b.Get("dir/key")
	if err != ErrKeyNotExist {
		t.Fatal(err)
	}
	if v != "" {
		t.Errorf("got v == %q, want empty", v)
	}

	keys, err := b.List("dir")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Errorf("got keys == %v, want empty", keys)
	}

	wantV := "v"
	err = b.Set("dir/key", wantV)
	if err != nil {
		t.Error(err)
	}

	v, err = b.Get("dir/key")
	if err != nil {
		t.Fatal(err)
	}
	if v != wantV {
		t.Errorf("got v == %q, want %q", v, wantV)
	}

	keys, err = b.List("dir")
	if err != nil {
		t.Fatal(err)
	}
	if wantKeys := []string{"key"}; !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("got keys == %v, want %v", keys, wantKeys)
	}
}

func TestInMemoryBackend(t *testing.T) {
	b := NewInMemoryBackend(nil)
	testBackend(t, b)
}
