package metadata

import "strings"

func isValidName(v string) bool {
	cleaned := strings.TrimSpace(v)
	return cleaned != "" // TODO: Deeper checking
}
