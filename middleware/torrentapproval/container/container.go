package container

import (
	"errors"
	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/pkg/stop"
	"gopkg.in/yaml.v2"
	"sync"
)

type Builder interface {
	New() (Container, error)
}

var (
	buildersMU sync.Mutex
	builders   = make(map[string]Builder)

	ErrContainerDoesNotExist = errors.New("torrent hash container with that name does not exist")
)

func Register(n string, c Builder) {
	if len(n) == 0 {
		panic("middleware: could not register a Container with an empty name")
	}
	if c == nil {
		panic("middleware: could not register a Container with nil builder")
	}

	buildersMU.Lock()
	defer buildersMU.Unlock()
	builders[n] = c
}

type Container interface {
	stop.Stopper
	Contains(bittorrent.InfoHash) bool
}

func GetContainer(name string, confBytes []byte) (Container, error) {
	buildersMU.Lock()
	defer buildersMU.Unlock()
	var err error
	var cn Container
	if builder, exist := builders[name]; !exist {
		err = ErrContainerDoesNotExist
	} else {
		if err = yaml.Unmarshal(confBytes, &cn); err == nil {
			cn, err = builder.New()
		}
	}
	return cn, err
}
