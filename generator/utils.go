package generator

import (
	"fmt"
	"regexp"

	"github.com/go-crypt/crypt/algorithm"
	"github.com/go-crypt/crypt/algorithm/shacrypt"
)

var reValidIdentifier = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func IsValidIdentifier(username string) error {
	if !reValidIdentifier.MatchString(username) {
		return fmt.Errorf("%q must match %q: %w", username, reValidIdentifier.String(), ErrInvalidInput)
	}
	return nil
}

func HashPassword(password string) (string, error) {
	var (
		digest algorithm.Digest
		hasher *shacrypt.Hasher // scylla supports bcrypt, sha512crypt, sha256crypt & md5crypt
		err    error
	)

	hasher, err = shacrypt.New(
		shacrypt.WithSHA512(),
	)
	if err != nil {
		return "<invalid>", err
	}

	if digest, err = hasher.Hash(password); err != nil {
		return "<invalid>", err
	}

	return digest.Encode(), nil
}
