package container

import (
	"errors"
	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/storage"
	"sync"
)

const DefaultStorageCtxName = "MW_APPROVAL"

type Builder func([]byte, storage.Storage) (Container, error)

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
		panic("middleware: could not register a Container with nil builder constructor")
	}

	buildersMU.Lock()
	defer buildersMU.Unlock()
	builders[n] = c
}

type Container interface {
	Approved(bittorrent.InfoHash) bool
}

func GetContainer(name string, confBytes []byte, storage storage.Storage) (Container, error) {

	buildersMU.Lock()
	defer buildersMU.Unlock()
	var err error
	var cn Container
	if builder, exist := builders[name]; !exist {
		err = ErrContainerDoesNotExist
	} else {
		cn, err = builder(confBytes, storage)
	}
	return cn, err
}
