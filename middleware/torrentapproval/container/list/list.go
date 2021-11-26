// Package list implements container with pre-defined
// list of torrent hashes from config file
package list

import (
	"fmt"
	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container"
	"github.com/chihaya/chihaya/pkg/log"
	"github.com/chihaya/chihaya/storage"
	"gopkg.in/yaml.v2"
)

const Name = "list"

func init() {
	container.Register(Name, build)
}

type Config struct {
	HashList   []string `yaml:"hash_list"`
	Invert     bool     `yaml:"invert"`
	StorageCtx string   `yaml:"storage_ctx"`
}

const DUMMY = true

func build(confBytes []byte, st storage.Storage) (container.Container, error) {
	c := new(Config)
	if err := yaml.Unmarshal(confBytes, c); err != nil {
		return nil, fmt.Errorf("unable to deserialise configuration: %v", err)
	}
	l := &List{
		Invert:     c.Invert,
		Storage:    st,
		StorageCtx: c.StorageCtx,
	}

	if len(l.StorageCtx) == 0 {
		log.Info("Storage context not set, using default value: " + container.DefaultStorageCtxName)
		l.StorageCtx = container.DefaultStorageCtxName
	}

	if len(c.HashList) > 0 {
		init := make([]storage.Pair, 0, len(c.HashList))
		for _, hashString := range c.HashList {
			ih, err := bittorrent.NewInfoHash(hashString)
			if err != nil {
				return nil, fmt.Errorf("whitelist : %s : %v", hashString, err)
			}
			init = append(init, storage.Pair{Left: ih.RawString(), Right: DUMMY})
			if len(ih) == bittorrent.InfoHashV2Len {
				init = append(init, storage.Pair{Left: ih.TruncateV1().RawString(), Right: DUMMY})
			}
		}
		l.Storage.BulkPut(l.StorageCtx, init...)
	}
	return l, nil
}

type List struct {
	Invert     bool
	Storage    storage.Storage
	StorageCtx string
}

func (l *List) Approved(hash bittorrent.InfoHash) bool {
	b := l.Storage.Contains(l.StorageCtx, hash.RawString())
	if len(hash) == bittorrent.InfoHashV2Len {
		b = b || l.Storage.Contains(l.StorageCtx, hash.TruncateV1().RawString())
	}
	return b != l.Invert
}
