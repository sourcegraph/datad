package datad

import "log"

// A Publisher ensures that the provider's keys are registered and coordinates
// distribution of data among the other publishers in the cluster.
type Publisher struct {
	ProviderURL string
	Provider    Provider

	backend  Backend
	registry *Registry

	Log *log.Logger
}

// NewPublisher creates a new publisher to publish data from a provider to the
// cluster. The providerURL is advertised to the cluster and therefore must be
// accessible by the other clients and publishers in the cluster.
//
// Call Start on this publisher to begin publishing its keys to the cluster.
func NewPublisher(providerURL string, b Backend, p Provider) *Publisher {
	return &Publisher{
		ProviderURL: providerURL,
		Provider:    p,
		backend:     b,
		registry:    NewRegistry(b),
	}
}

// Start begins advertising this publisher's provider's keys to the
// cluster.
func (p *Publisher) Start() error {
	p.logf("Starting publisher for provider %s.", p.ProviderURL)

	err := p.addProviderToCluster()
	if err != nil {
		return err
	}

	err = p.registerExistingKeys()
	if err != nil {
		return err
	}

	return nil
}

// Stop deregisters this publisher's keys and stops background processes
// for this publisher.
func (p *Publisher) Stop() error {
	// TODO(sqs): implement this
	return nil
}

// addProviderToCluster adds this publisher's provider to the cluster, making it
// available to receive requests for and be assigned keys.
func (p *Publisher) addProviderToCluster() error {
	return p.backend.Set(providersPrefix+"/"+encodeURLForKey(p.ProviderURL), "")
}

// registerExistingKeys registers this publisher's provider as holding the keys
// that are on this publisher's persistent storage. This means that when the
// publisher starts up, it's immediately able to receive requests for the data
// it already has on disk. Without this, the cluster would not know that this
// publisher's provider has these keys.
func (p *Publisher) registerExistingKeys() error {
	keys, err := p.Provider.Keys("")
	if err != nil {
		return err
	}

	p.logf("Found %d existing keys in %s: %v. Registering existing keys to this provider (%s)...", len(keys), keys, p.ProviderURL)
	for _, key := range keys {
		err := p.registry.AddProvider(key, p.ProviderURL)
		if err != nil {
			return err
		}
	}
	p.logf("Finished registering existing %d keys to this provider (%s).", len(keys), p.ProviderURL)

	return nil
}

func (p *Publisher) logf(format string, a ...interface{}) {
	if p.Log != nil {
		p.Log.Printf(format, a...)
	}
}
