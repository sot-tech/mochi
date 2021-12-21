// Package list implements container with pre-defined
// list of torrent hashes from config file
package list

import (
	"fmt"
	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/storage"
	"gopkg.in/yaml.v2"
)

// Name of this container for registry.
const Name = "list"

func init() {
	container.Register(Name, build)
}

// Config - implementation of list container configuration.
type Config struct {
	// HashList static list of HEX-encoded InfoHashes.
	HashList []string `yaml:"hash_list"`
	// If Invert set to true, all InfoHashes stored in HashList should be blacklisted.
	Invert bool `yaml:"invert"`
	// StorageCtx is the name of storage context where to store hash list.
	// It might be table name, REDIS record key or something else, depending on storage.
	StorageCtx string `yaml:"storage_ctx"`
}

// DUMMY used as value placeholder if storage needs some value with
const DUMMY = "_"

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

// List work structure of hash list. Might be reused in another containers.
type List struct {
	// Invert see Config.Invert description.
	Invert bool
	// Storage implementation where hashes are stored for approval checks.
	Storage storage.Storage
	// StorageCtx see Config.StorageCtx description.
	StorageCtx string
}

// Approved checks if specified hash is approved or not.
// If List.Invert set to true and hash found in storage, function will return false,
// that means that hash is blacklisted.
func (l *List) Approved(hash bittorrent.InfoHash) bool {
	b := l.Storage.Contains(l.StorageCtx, hash.RawString())
	if len(hash) == bittorrent.InfoHashV2Len {
		b = b || l.Storage.Contains(l.StorageCtx, hash.TruncateV1().RawString())
	}
	return b != l.Invert
}
