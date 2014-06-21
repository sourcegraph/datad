package datad

import (
	"errors"
	"strings"
)

// The registry is the mapping of keys to the servers that hold them.
type registry struct {
	backend Backend

	// keyPrefix always ends with a "/".
	keyPrefix string
}

func newRegistry(b Backend, keyPrefix string) *registry {
	keyPrefix = strings.TrimSuffix(keyPrefix, "/")
	return &registry{b, keyPrefix}
}

var errNoServerForKey = errors.New("key has no server")

func (r *registry) serverForKey(key string) (string, error) {
	server, err := r.backend.Get(r.keyPrefix + "/" + key)
	if err == ErrKeyNotExist {
		return "", errNoServerForKey
	} else if err != nil {
		return "", err
	}
	return server, nil
}

func (r *registry) setServerForKey(key string, url string) error {
	return r.backend.Set(r.keyPrefix+"/"+key, url)
}
