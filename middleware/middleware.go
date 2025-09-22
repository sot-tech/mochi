// Package middleware implements the Logic interface by executing
// a series of middleware hooks.
package middleware

import (
	"fmt"
	"sync"

	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/storage"
)

var (
	logger     = log.NewLogger("middleware")
	buildersMU sync.RWMutex
	builders   = make(map[string]Builder)
)

// Builder is the function used to initialize a new Hook
// with provided configuration.
type Builder func(conf.MapConfig, storage.PeerStorage) (Hook, error)

// RegisterBuilder makes a Builder available by the provided name.
//
// If called twice with the same name, the name is blank, or if the provided
// Builder is nil, this function panics.
func RegisterBuilder(name string, b Builder) {
	if name == "" {
		panic("middleware: could not register Builder with an empty name")
	}
	if b == nil {
		panic("middleware: could not register a nil Builder")
	}

	buildersMU.Lock()
	defer buildersMU.Unlock()

	if _, dup := builders[name]; dup {
		panic("middleware: RegisterBuilder called twice for " + name)
	}

	builders[name] = b
}

// NewHooks is a utility function for initializing Hooks in bulk.
func NewHooks(configs []conf.NamedMapConfig, storage storage.PeerStorage) (hooks []Hook, err error) {
	buildersMU.RLock()
	defer buildersMU.RUnlock()
	for _, c := range configs {
		logger.Debug().Str("name", c.Name).Object("hook", c).Msg("starting hook")
		newHook, ok := builders[c.Name]
		if !ok {
			err = fmt.Errorf("hook with name '%s' does not exists", c.Name)
			break
		}
		var h Hook
		if h, err = newHook(c.Config, storage); err != nil {
			break
		}
		hooks = append(hooks, h)
		logger.Info().Str("name", c.Name).Msg("hook started")
	}

	return hooks, err
}
