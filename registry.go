package datad

import "strings"

// A Registry maps keys to the servers that hold them.
type Registry struct {
	backend Backend

	// keyPrefix always ends with a "/".
	keyPrefix string
}

func NewRegistry(b Backend, keyPrefix string) *Registry {
	keyPrefix = strings.TrimSuffix(keyPrefix, "/")
	return &Registry{b, keyPrefix}
}

func (r *Registry) providersDir(key string) string {
	return r.keyPrefix + "/" + unslash(key) + "/__$providers"
}

func (r *Registry) ProviderVersions(key string) (map[string]string, error) {
	key = unslash(key)
	providers, err := r.backend.List(r.providersDir(key))
	if err != nil {
		return nil, err
	}

	pvs := make(map[string]string, len(providers))
	for _, p := range providers {
		p, err = decodeURLForKey(p)
		if err != nil {
			return nil, err
		}

		ver, err := r.ProviderVersion(key, p)
		if err != nil {
			return nil, err
		}

		pvs[p] = ver
	}

	return pvs, nil
}

func (r *Registry) ProviderVersion(key, providerURL string) (string, error) {
	return r.backend.Get(r.providersDir(key) + "/" + encodeURLForKey(providerURL))
}

func (r *Registry) AddProvider(key, providerURL, providerKeyVersion string) error {
	return r.backend.Set(r.providersDir(key)+"/"+encodeURLForKey(providerURL), providerKeyVersion)
}
