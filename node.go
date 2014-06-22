package datad

import (
	"log"
	"net"
	"strings"
)

// A Node ensures that the provider's keys are registered and coordinates
// distribution of data among the other nodes in the cluster.
type Node struct {
	Name     string
	Provider Provider

	backend  Backend
	registry *Registry

	Log *log.Logger
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

	err := n.addToCluster()
	if err != nil {
		return err
	}

	err = n.registerExistingKeys()
	if err != nil {
		return err
	}

	return nil
}

// Stop deregisters this node's keys and stops background processes
// for this node.
func (n *Node) Stop() error {
	// TODO(sqs): implement this
	return nil
}

// addToCluster adds this node's provider to the cluster, making it
// available to receive requests for and be assigned keys.
func (n *Node) addToCluster() error {
	return n.backend.SetDir(keyPathJoin(nodesPrefix, n.Name))
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

	n.logf("Found %d existing keys in %s: %v. Registering existing keys to this node (%s)...", len(keys), keys, n.Name)
	for _, key := range keys {
		err := n.registry.Add(key, n.Name)
		if err != nil {
			return err
		}
	}
	n.logf("Finished registering existing %d keys to this node (%s).", len(keys), n.Name)

	return nil
}

func (n *Node) logf(format string, a ...interface{}) {
	if n.Log != nil {
		n.Log.Printf(format, a...)
	}
}
