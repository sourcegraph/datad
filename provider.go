package datad

// A Provider makes data accessible to the datad cluster.
type Provider interface {
	KeyVersions(keyPrefix string) (map[string]string, error)
	KeyVersion(key string) (string, error)
	Update(key, version string) error
}
