package datad

// A Registry maps keys to the servers that hold them.
type Registry struct {
	backend Backend
}

func NewRegistry(b Backend) *Registry {
	return &Registry{b}
}

func (r *Registry) providersDir(key string) string {
	return "/registry/" + unslash(key) + "/__$providers"
}

func (r *Registry) Providers(key string) ([]string, error) {
	providers, err := r.backend.List(r.providersDir(key))
	if err != nil {
		return nil, err
	}

	for i, p := range providers {
		providers[i], err = decodeURLForKey(p)
		if err != nil {
			return nil, err
		}
	}

	return providers, nil
}

func (r *Registry) AddProvider(key, providerURL string) error {
	return r.backend.Set(r.providersDir(key)+"/"+encodeURLForKey(providerURL), "")
}

func (r *Registry) RemoveProvider(key, providerURL string) error {
	return r.backend.Delete(r.providersDir(key) + "/" + encodeURLForKey(providerURL))
}
