package generator

import (
	"bytes"
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

func ptrTo[T any](n T) *T {
	res := new(T)
	*res = n
	return res
}

type Set[T comparable] interface {
	Has(T) bool
}

type hashSet[T comparable] map[T]struct{}

func SetOf[T comparable](items ...T) Set[T] {
	if len(items) == 0 {
		return nil
	}

	h := make(hashSet[T], len(items))

	for _, item := range items {
		h[item] = struct{}{}
	}

	return h
}

func (s hashSet[T]) Has(item T) bool {
	_, ok := s[item]
	return ok
}

func EscapePassword(password string) string {
	b := &bytes.Buffer{}

	for _, c := range password {
		switch c {
		case '\'':
			b.WriteRune('\\')
		case '\\':
			b.WriteRune('\\')
		default:
			// Do nothing special
		}
		b.WriteRune(c)
	}

	return b.String()
}

func Redact(str string) string {
	re := regexp.MustCompile(`(?i:PASSWORD)\s*=\s*'.*'`)
	str = re.ReplaceAllString(str, "PASSWORD = '<reacted>'")
	return str
}
