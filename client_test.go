package datad

import (
	"reflect"
	"testing"

	etcd_client "github.com/coreos/go-etcd/etcd"
)

func TestClient_NodesInCluster(t *testing.T) {
	withEtcd(t, func(ec *etcd_client.Client) {
		b := NewEtcdBackend("/", ec)
		c := NewClient(b)
		node := NewNode("example.com", b, NoopProvider{})

		nodes, err := c.NodesInCluster()
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) != 0 {
			t.Errorf("got %d initial nodes, want 0", len(nodes))
		}

		err = node.addToCluster()
		if err != nil {
			t.Fatal(err)
		}

		nodes, err = c.NodesInCluster()
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"example.com"}; !reflect.DeepEqual(nodes, want) {
			t.Errorf("got nodes == %v, want %v", nodes, want)
		}
	})
}
