package metadata

import "strings"

func isValidName(v string) bool {
	cleaned := strings.TrimSpace(v)
	return cleaned != "" // TODO: Deeper checking
}

// QualifiedName returns name prefixed with the keyspace.
func QualifiedName(keyspace string, name string) (string, error) {
	if !isValidName(keyspace) {
		return "", ErrNoKeyspace
	}
	return keyspace + "." + name, nil
}
