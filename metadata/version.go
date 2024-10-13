package metadata

import (
	"context"
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"strconv"
	"strings"
)

// Version holds software version information that typically specified in the form: z.y.z-ww
type Version struct {
	Major int
	Minor int
	Patch int
	Tag   string
}

// GetScyllaVersion returns the version of Scylla server that we're connected to
func GetScyllaVersion(ctx context.Context, sess gocqlx.Session) (Version, error) {
	v := Version{}

	var version string
	err := sess.ContextQuery(ctx, "SELECT version FROM system.versions", nil).Consistency(gocql.One).Get(&version)
	if err != nil {
		return v, err
	}

	return parseVersion(version), nil
}

// Parse version information from a string
// The general form of a version string that's accepted is xx[.yy[.zz]][-ww]
// (this matches all known Scylla version strings to date)
func parseVersion(version string) Version {
	v := Version{}

	parts := strings.SplitN(version, ".", 3)

	parseValue := func(part string) (int, string) {
		var t string

		p := strings.SplitN(part, "-", 2)
		if len(p) == 0 {
			return 0, ""
		}

		i, e := strconv.ParseInt(p[0], 10, 32)
		if e != nil {
			return 0, p[0]
		}

		if len(p) > 1 {
			t = p[1]
		}

		return int(i), t
	}

	var t string
	switch len(parts) {
	case 3:
		v.Patch, t = parseValue(parts[2])
		if t != "" {
			v.Tag = t
		}
		fallthrough
	case 2:
		v.Minor, t = parseValue(parts[1])
		if t != "" {
			v.Tag = t
		}
		fallthrough
	case 1:
		v.Major, t = parseValue(parts[0])
		if t != "" {
			v.Tag = t
		}
	}

	return v
}
