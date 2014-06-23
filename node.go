package datad

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

var NodeMembershipTTL = 5 * time.Second

// A Node ensures that the provider's keys are registered and coordinates
// distribution of data among the other nodes in the cluster.
type Node struct {
	Name     string
	Provider Provider

	backend  Backend
	registry *Registry

	Log *log.Logger

	stopChan chan struct{}
}

// NewNode creates a new node to publish data from a provider to the cluster.
// The name ("host:port") is advertised to the cluster and therefore must be
// accessible by the other clients and nodes in the cluster. The name should be
// the host and port where the data on this machine is accessible.
//
// Call Start on this node to begin publishing its keys to the cluster.
func NewNode(name string, b Backend, p Provider) *Node {
	name = cleanNodeName(name)
	return &Node{
		Name:     name,
		Provider: p,
		backend:  b,
		registry: NewRegistry(b),
		Log:      log.New(os.Stderr, "", log.Ltime|log.Lshortfile),
		stopChan: make(chan struct{}),
	}
}

func cleanNodeName(name string) string {
	name = strings.TrimPrefix(name, "http://")
	parseName := name
	if !strings.Contains(parseName, ":") {
		parseName += ":80"
	}
	_, _, err := net.SplitHostPort(parseName)
	if err != nil {
		panic("NewNode: bad name '" + name + "': " + err.Error() + " (name should be 'host:port')")
	}
	return name
}

// Start begins advertising this node's provider's keys to the
// cluster.
func (n *Node) Start() error {
	n.logf("Starting node %s.", n.Name)

	err := n.joinCluster()
	if err != nil {
		n.logf("Failed to join cluster: %s", err)
		return err
	}

	err = n.registerExistingKeys()
	if err != nil {
		n.logf("Failed to register existing keys: %s", err)
		return err
	}

	go n.watchRegisteredKeys()
	go n.balancePeriodically()

	return nil
}

// Stop deregisters this node's keys and stops background processes
// for this node.
func (n *Node) Stop() error {
	close(n.stopChan)
	return nil
}

// joinCluster adds this node's provider to the cluster, making it available to
// receive requests for and be assigned keys. It then periodically re-adds this
// node to the cluster before the TTL on the etcd cluster membership key
// elapses.
func (n *Node) joinCluster() error {
	err := n.refreshClusterMembership()
	if err != nil {
		return err
	}

	if NodeMembershipTTL < time.Second {
		panic("NodeMembershipTTL must be at least 1 second")
	}

	go func() {
		t := time.NewTicker(NodeMembershipTTL)
		for {
			select {
			case <-t.C:
				err := n.refreshClusterMembership()
				if err != nil {
					n.logf("Error refreshing node %s cluster membership: %s.", n.Name, err)
				}
			case <-n.stopChan:
				t.Stop()
				return
			}
		}
	}()

	return nil
}

func (n *Node) refreshClusterMembership() error {
	err := n.backend.SetDir(keyPathJoin(nodesPrefix, n.Name), uint64(NodeMembershipTTL/time.Second))
	if isEtcdErrorCode(err, 102) {
		err = n.backend.UpdateDir(keyPathJoin(nodesPrefix, n.Name), uint64(NodeMembershipTTL/time.Second))
	}
	return err
}

// watchRegisteredKeys watches the registry for changes to the list of keys that
// this node is registered for, or for modifications of existing registrations
// (e.g., updates requested).
func (n *Node) watchRegisteredKeys() error {
	watchKey := keysForNodeDir(n.Name)

	recv := make(chan *etcd.Response)
	stopWatch := make(chan bool)

	// Receive watched changes.
	go func() {
		for {
			select {
			case resp := <-recv:
				key := strings.TrimPrefix(resp.Node.Key, watchKey+"/")
				n.logf("Registry changed: %s on key %q.", resp.Action, key)
				if !strings.Contains(strings.ToLower(resp.Action), "delete") {
					n.logf("Updating key %q in data source (in response to registry %s).", key, resp.Action)
					err := n.Provider.Update(key)
					if err != nil {
						n.logf("Error updating key %q in data source: %s.", key, err)
					}
				}
			case <-n.stopChan:
				n.logf("Stopping registry watcher.")
				stopWatch <- true
				return
			}
		}
	}()

	_, err := n.backend.(*EtcdBackend).etcd.Watch(watchKey, 0, true, recv, stopWatch)
	if err != etcd.ErrWatchStoppedByUser {
		return err
	}
	return nil
}

// registerExistingKeys examines this node's provider's local storage for data
// and registers each data key it finds. This means that when the node starts
// up, it's immediately able to receive requests for the data it already has on
// disk. Without this, the cluster would not know that this node's provider has
// these keys.
func (n *Node) registerExistingKeys() error {
	keys, err := n.Provider.Keys("")
	if err != nil {
		return err
	}

	if len(keys) == 0 {
		return nil
	}

	n.logf("Found %d existing keys in provider: %v. Registering existing keys to this node...", len(keys), keys)
	for _, key := range keys {
		err := n.registry.Add(key, n.Name)
		if err != nil {
			return err
		}
	}
	n.logf("Finished registering existing %d keys to this node.", len(keys))

	return nil
}

// startBalancer starts a periodic process that balances the distribution of
// keys to nodes.
func (n *Node) balancePeriodically() {
	// TODO(sqs): allow tweaking the balance interval
	t := time.NewTicker(time.Minute)
	for {
		select {
		case <-t.C:
			err := n.balance()
			if err != nil {
				n.logf("Error balancing: %s. Will retry next balance interval.", err)
			}
		case <-n.stopChan:
			t.Stop()
			return
		}
	}
}

// balance examines all keys and ensures each key has a registered node. If not,
// it registers a node for the key. This lets the cluster heal itself after a
// node goes down (which causes keys to be orphaned).
func (n *Node) balance() error {
	keyMap, err := n.registry.KeyMap()
	if err != nil {
		return err
	}

	if len(keyMap) == 0 {
		return nil
	}

	c := NewClient(n.backend)
	clusterNodes, err := c.NodesInCluster()
	if err != nil {
		return err
	}

	// TODO(sqs): allow tweaking this parameter
	x := rand.Intn(10)
	start := time.Now()

	n.logf("Balancer: starting on %d keys, with known cluster nodes %v.", len(keyMap), clusterNodes)
	actions := 0
	for key, nodes := range keyMap {
		if len(nodes) == 0 {
			regNode := clusterNodes[keyBucket(key, len(clusterNodes))]

			n.logf("Balancer: found unregistered key %q; registering it to node %s.", key, regNode)

			// TODO(sqs): optimize this by only adding if not exists, and then
			// seeing if it exists (to avoid potentially duplicating work).
			err := c.registry.Add(key, regNode)
			if err != nil {
				return err
			}

			actions++
			continue
		}

		// Check liveness of key on each node.
		for _, node := range nodes {
			t, err := c.transportForKey(key, nil, []string{node})
			if err != nil {
				return err
			}
			resp, err := (&http.Client{Transport: t}).Get(slash(key))
			if err != nil {
				actions++
				n.logf("Balancer: liveness check failed for key %q on node %s: %s. Client deregistered key from node.", key, node, err)
			}
			if resp != nil {
				resp.Body.Close()
			}
		}

		// Update keys on this node (but not each time, to avoid overloading the
		// origin servers).
		if x == 0 {
			for _, node := range nodes {
				if node == n.Name {
					n.logf("Balancer: updating key %q in data source on current node.", key)
					err := n.Provider.Update(key)
					if err != nil {
						n.logf("Balancer: error updating key %q in data source on current node: %s. (Continuing to balance other keys.)", key, err)
					}
					actions++
				}
			}
		}
	}
	n.logf("Balancer: completed in %s for %d keys (%d non-read actions performed).", time.Since(start), len(keyMap), actions)

	return nil
}

func (n *Node) logf(format string, a ...interface{}) {
	if n.Log != nil {
		n.Log.Printf(fmt.Sprintf("Node %s: ", n.Name)+format, a...)
	}
}
