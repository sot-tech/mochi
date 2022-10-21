// Package frontend defines interface which should satisfy
// every network frontend
package frontend

import (
	"fmt"
	"sync"

	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/stop"
)

var (
	logger     = log.NewLogger("frontend")
	buildersMU sync.RWMutex
	builders   = make(map[string]Builder)
)

// Builder is the function used to initialize a new Frontend
// with provided configuration.
type Builder func(conf.MapConfig, *middleware.Logic) (Frontend, error)

// RegisterBuilder makes a Builder available by the provided name.
//
// If called twice with the same name, the name is blank, or if the provided
// Builder is nil, this function panics.
func RegisterBuilder(name string, b Builder) {
	if name == "" {
		panic("frontend: could not register Builder with an empty name")
	}
	if b == nil {
		panic("frontend: could not register a nil Builder")
	}

	buildersMU.Lock()
	defer buildersMU.Unlock()

	if _, dup := builders[name]; dup {
		panic("frontend: RegisterBuilder called twice for " + name)
	}

	builders[name] = b
}

type Frontend interface {
	stop.Stopper
}

// NewFrontends is a utility function for initializing Frontend-s in bulk.
// Returns nil hook and error if frontend with name provided in config
// does not exists.
func NewFrontends(configs []conf.NamedMapConfig, logic *middleware.Logic) (fs []Frontend, err error) {
	buildersMU.RLock()
	defer buildersMU.RUnlock()
	for _, c := range configs {
		logger.Debug().Object("frontend", c).Msg("starting frontend")
		newFrontend, ok := builders[c.Name]
		if !ok {
			err = fmt.Errorf("hook with name '%s' does not exists", c.Name)
			break
		}
		var f Frontend
		if f, err = newFrontend(c.Config, logic); err != nil {
			break
		}
		fs = append(fs, f)
		logger.Info().Str("name", c.Name).Msg("frontend started")
	}
	return
}
