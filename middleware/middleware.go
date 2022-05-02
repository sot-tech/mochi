// Package middleware implements the TrackerLogic interface by executing
// a series of middleware hooks.
package middleware

import (
	"errors"
	"sync"

	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/storage"
)

var (
	driversM sync.RWMutex
	drivers  = make(map[string]Builder)

	// ErrBuilderDoesNotExist is the error returned by NewMiddleware when a
	// middleware driver with that name does not exist.
	ErrBuilderDoesNotExist = errors.New("middleware builder with that name does not exist")
)

// Builder is the interface used to initialize a new type of middleware.
//
// The `options` parameter is map of parameters that should be unmarshalled into
// the hook's custom configuration.
type Builder func(options conf.MapConfig, storage storage.PeerStorage) (Hook, error)

// RegisterBuilder makes a Builder available by the provided name.
//
// If called twice with the same name, the name is blank, or if the provided
// Builder is nil, this function panics.
func RegisterBuilder(name string, d Builder) {
	if name == "" {
		panic("middleware: could not register a Builder with an empty name")
	}
	if d == nil {
		panic("middleware: could not register a nil Builder")
	}

	driversM.Lock()
	defer driversM.Unlock()

	if _, dup := drivers[name]; dup {
		panic("middleware: RegisterBuilder called twice for " + name)
	}

	drivers[name] = d
}

// NewHook attempts to initialize a new middleware instance from the
// list of registered Builders.
//
// If a driver does not exist, returns ErrBuilderDoesNotExist.
func NewHook(name string, options conf.MapConfig, storage storage.PeerStorage) (Hook, error) {
	driversM.RLock()
	defer driversM.RUnlock()

	var newHook Builder
	newHook, ok := drivers[name]
	if !ok {
		return nil, ErrBuilderDoesNotExist
	}

	return newHook(options, storage)
}

// Config is the generic configuration format used for all registered Hooks.
type Config struct {
	Name    string
	Options conf.MapConfig
}

// NewHooks is a utility function for initializing Hooks in bulk.
// each element of configs must contain pairs `name` - string and `options` - map[string]any
func NewHooks(configs []conf.MapConfig, storage storage.PeerStorage) (hooks []Hook, err error) {
	for _, cfg := range configs {
		var c Config

		if err = cfg.Unmarshal(&c); err != nil {
			break
		}

		var h Hook
		h, err = NewHook(c.Name, c.Options, storage)
		if err != nil {
			break
		}

		hooks = append(hooks, h)
	}

	return
}
