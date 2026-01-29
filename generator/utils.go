package generator

import (
	"fmt"
	"regexp"
)

var reValidIdentifier = regexp.MustCompile(`^[a-zA-Z0-9_]*$`)

func IsValidIdentifier(username string) error {
	if !reValidIdentifier.MatchString(username) {
		return fmt.Errorf("%q must match %q", username, reValidIdentifier.String())
	}
	return nil
}
