package datad

import (
	"errors"
	"strings"

	"github.com/coreos/go-etcd/etcd"
)

type Backend interface {
	Get(key string) (string, error)
	List(key string) ([]string, error)
	Set(key, value string) error
	Delete(key string) error
}

var ErrKeyNotExist = errors.New("key does not exist")

type InMemoryBackend struct{ m map[string]string }

func NewInMemoryBackend(m map[string]string) Backend {
	if m == nil {
		m = make(map[string]string)
	}
	return InMemoryBackend{m}
}

func (b InMemoryBackend) Get(key string) (string, error) {
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}
	v, present := b.m[key]
	if !present {
		return "", ErrKeyNotExist
	}
	return v, nil
}

func (b InMemoryBackend) List(key string) ([]string, error) {
	key = slash(key)
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	var subkeys []string
	for k, _ := range b.m {
		if strings.HasPrefix(k, key) {
			subkeys = append(subkeys, strings.TrimPrefix(k, key))
		}
	}
	return subkeys, nil
}

func (b InMemoryBackend) Set(key, value string) error {
	key = slash(key)
	b.m[key] = value
	return nil
}

func (b InMemoryBackend) Delete(key string) error {
	key = slash(key)
	delete(b.m, key)
	return nil
}

type EtcdBackend struct {
	keyPrefix string
	etcd      *etcd.Client
}

func NewEtcdBackend(keyPrefix string, c *etcd.Client) Backend {
	keyPrefix = slash(strings.TrimSuffix(keyPrefix, "/"))
	return &EtcdBackend{keyPrefix, c}
}

func (c *EtcdBackend) Get(key string) (string, error) {
	key = c.fullKey(key)
	resp, err := c.etcd.Get(key, false, false)
	if isEtcdKeyNotExist(err) {
		return "", ErrKeyNotExist
	} else if err != nil {
		return "", err
	}
	return resp.Node.Value, nil
}

func (c *EtcdBackend) List(key string) ([]string, error) {
	key = c.fullKey(key)
	resp, err := c.etcd.Get(key, true, true)
	if isEtcdKeyNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	subkeys := make([]string, len(resp.Node.Nodes))
	for i, node := range resp.Node.Nodes {
		subkeys[i] = strings.TrimPrefix(node.Key, strings.TrimSuffix(key, "/")+"/")
	}
	return subkeys, nil
}

func (c *EtcdBackend) Set(key, value string) error {
	key = c.fullKey(key)
	_, err := c.etcd.Set(key, value, 0)
	return err
}

func (c *EtcdBackend) Delete(key string) error {
	key = c.fullKey(key)
	_, err := c.etcd.Delete(key, false)
	return err
}

func (c *EtcdBackend) fullKey(keyWithoutPrefix string) string {
	return c.keyPrefix + "/" + unslash(keyWithoutPrefix)
}

func isEtcdKeyNotExist(err error) bool {
	if err, ok := err.(*etcd.EtcdError); ok && err != nil && err.ErrorCode == 100 {
		return true
	}
	return false
}