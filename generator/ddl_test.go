package generator

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/zeroflucs-given/charybdis/metadata"
)

type OpTest func(got metadata.DDLOperation) error

func ExactMatchOpTest(want metadata.DDLOperation) OpTest {
	return func(got metadata.DDLOperation) error {
		if want.Description != got.Description || want.Command != got.Command {
			return errors.New("ddl operation doesn't match the expected value")
		}
		return nil
	}
}

func CommandMatchOpTest(want string) OpTest {
	return func(got metadata.DDLOperation) error {
		if want != got.Command {
			return fmt.Errorf("ddl command doesn't match the expected value '%s' v '%s'", got.Command, want)
		}
		return nil
	}
}

func CommandMatchRegExOpTest(pattern string) OpTest {
	re := regexp.MustCompile(pattern)
	return func(got metadata.DDLOperation) error {
		if !re.MatchString(got.Command) {
			return fmt.Errorf("ddl command %q doesn't match the expected regular expression %q", got.Command, re.String())
		}
		return nil
	}
}
