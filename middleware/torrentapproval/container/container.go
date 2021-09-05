package container

import (
	"errors"
	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/pkg/stop"
	"gopkg.in/yaml.v2"
	"sync"
)

type Constructor func () Configuration

type Configuration interface {
	Build() (Container, error)
}

var (
	constructorsMU sync.Mutex
	constructors   = make(map[string]Constructor)

	ErrContainerDoesNotExist = errors.New("torrent hash container with that name does not exist")
)

func Register(n string, c Constructor) {
	if len(n) == 0 {
		panic("middleware: could not register a Container with an empty name")
	}
	if c == nil {
		panic("middleware: could not register a Container with nil builder constructor")
	}

	constructorsMU.Lock()
	defer constructorsMU.Unlock()
	constructors[n] = c
}

type Container interface {
	stop.Stopper
	Contains(bittorrent.InfoHash) bool
}

func GetContainer(name string, confBytes []byte) (Container, error) {
	constructorsMU.Lock()
	defer constructorsMU.Unlock()
	var err error
	var cn Container
	if getConfig, exist := constructors[name]; !exist {
		err = ErrContainerDoesNotExist
	} else {
		conf := getConfig()
		if err = yaml.Unmarshal(confBytes, &conf); err == nil {
			cn, err = conf.Build()
		}
	}
	return cn, err
}
