// Package config loads service configuration from the environment.
//
// The loader accumulates errors rather than failing on the first one, so a
// misconfigured deployment reports every problem in a single startup log line
// instead of one per restart cycle.
//
// Rule enforced here: every address of a peer service is Required. There are
// deliberately no localhost defaults anywhere in this codebase — a service that
// silently falls back to 127.0.0.1 works perfectly on a laptop and fails in
// confusing ways the day the cluster spans two hosts.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Loader reads environment variables and collects any parse failures.
type Loader struct {
	prefix string
	errs   []error
}

// New returns a Loader that prepends prefix to every key it reads.
func New(prefix string) *Loader {
	return &Loader{prefix: prefix}
}

func (l *Loader) key(name string) string {
	if l.prefix == "" {
		return name
	}
	return l.prefix + "_" + name
}

func (l *Loader) lookup(name string) (string, bool) {
	v, ok := os.LookupEnv(l.key(name))
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}
	return v, true
}

func (l *Loader) fail(name string, err error) {
	l.errs = append(l.errs, fmt.Errorf("%s: %w", l.key(name), err))
}

// Err returns all accumulated errors, or nil if the configuration is valid.
func (l *Loader) Err() error {
	return errors.Join(l.errs...)
}

// String returns the value of name, or def if unset.
func (l *Loader) String(name, def string) string {
	if v, ok := l.lookup(name); ok {
		return v
	}
	return def
}

// Required returns the value of name and records an error if it is unset.
// Use this for anything that identifies a peer: addresses, DSNs, node IDs.
func (l *Loader) Required(name string) string {
	v, ok := l.lookup(name)
	if !ok {
		l.fail(name, errors.New("is required but not set"))
	}
	return v
}

// Int returns name parsed as an int, or def if unset.
func (l *Loader) Int(name string, def int) int {
	v, ok := l.lookup(name)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.fail(name, fmt.Errorf("invalid integer %q", v))
		return def
	}
	return n
}

// Bool returns name parsed as a bool, or def if unset.
func (l *Loader) Bool(name string, def bool) bool {
	v, ok := l.lookup(name)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.fail(name, fmt.Errorf("invalid boolean %q", v))
		return def
	}
	return b
}

// Duration returns name parsed as a Go duration (e.g. "3s", "168h"), or def.
func (l *Loader) Duration(name string, def time.Duration) time.Duration {
	v, ok := l.lookup(name)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.fail(name, fmt.Errorf("invalid duration %q", v))
		return def
	}
	return d
}

// Bytes returns name parsed as a byte size. Accepts a plain integer or a
// suffixed value such as "8MiB", "13GiB", "20MB". See ParseBytes.
func (l *Loader) Bytes(name string, def int64) int64 {
	v, ok := l.lookup(name)
	if !ok {
		return def
	}
	n, err := ParseBytes(v)
	if err != nil {
		l.fail(name, err)
		return def
	}
	return n
}
