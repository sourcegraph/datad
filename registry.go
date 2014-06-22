package datad

import "time"

var RegistrationTTL = 60 * time.Second

// A Registry contains a bidirectional mapping between data keys and nodes: (1)
// for a given data key, a list of cluster nodes that have the underlying data
// on disk; and (2) for a given node, a list of data keys that it should
// fetch/compute and store on disk.
type Registry struct {
	backend Backend
}

func NewRegistry(b Backend) *Registry {
	return &Registry{b}
}

func (r *Registry) KeysForNode(node string) ([]string, error) {
	return r.backend.ListKeys(keysForNodeDir(node), true)
}

func (r *Registry) NodesForKey(key string) ([]string, error) {
	return r.backend.ListKeys(nodesForKeyDir(key), true)
}

func (r *Registry) Add(key, node string) error {
	err := r.backend.Set(nodesForKeyDir(key)+"/"+node, "")
	if err != nil {
		return err
	}

	err = r.backend.Set(keysForNodeDir(node)+"/"+key, "")
	if err != nil {
		return err
	}

	return nil
}

func (r *Registry) Remove(key, node string) error {
	err := r.backend.Delete(nodesForKeyDir(key) + "/" + node)
	if err != nil {
		return err
	}

	err = r.backend.Delete(keysForNodeDir(node) + "/" + key)
	if err != nil {
		return err
	}

	return nil
}

func nodesForKeyDir(key string) string {
	return keyPathJoin(dataPrefix, key, "__nodes")
}

func keysForNodeDir(node string) string {
	return keyPathJoin(nodesPrefix, node, "__keys")
}
