// Package list implements container with pre-defined
// list of torrent hashes from config file
package list

import (
	"fmt"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/storage"
)

// Name of this container for registry.
const Name = "list"

func init() {
	container.Register(Name, build)
}

// Config - implementation of list container configuration.
type Config struct {
	// HashList static list of HEX-encoded InfoHashes.
	HashList []string `cfg:"hash_list"`
	// If Invert set to true, all InfoHashes stored in HashList should be blacklisted.
	Invert bool
	// StorageCtx is the name of storage context where to store hash list.
	// It might be table name, REDIS record key or something else, depending on storage.
	StorageCtx string `cfg:"storage_ctx"`
}

// DUMMY used as value placeholder if storage needs some value with
const DUMMY = "_"

func build(conf conf.MapConfig, st storage.DataStorage) (container.Container, error) {
	c := new(Config)
	if err := conf.Unmarshal(c); err != nil {
		return nil, fmt.Errorf("unable to deserialise configuration: %w", err)
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
		init := make([]storage.Entry, 0, len(c.HashList))
		for _, hashString := range c.HashList {
			ih, err := bittorrent.NewInfoHash(hashString)
			if err != nil {
				return nil, fmt.Errorf("whitelist : %s : %w", hashString, err)
			}
			init = append(init, storage.Entry{Key: ih.RawString(), Value: DUMMY})
			if len(ih) == bittorrent.InfoHashV2Len {
				init = append(init, storage.Entry{Key: ih.TruncateV1().RawString(), Value: DUMMY})
			}
		}
		if err := l.Storage.Put(l.StorageCtx, init...); err != nil {
			return nil, fmt.Errorf("unable to put initial data: %w", err)
		}
	}
	return l, nil
}

// List work structure of hash list. Might be reused in another containers.
type List struct {
	// Invert see Config.Invert description.
	Invert bool
	// Storage implementation where hashes are stored for approval checks.
	Storage storage.DataStorage
	// StorageCtx see Config.StorageCtx description.
	StorageCtx string
}

// Approved checks if specified hash is approved or not.
// If List.Invert set to true and hash found in storage, function will return false,
// that means that hash is blacklisted.
func (l *List) Approved(hash bittorrent.InfoHash) (contains bool) {
	var err error
	if contains, err = l.Storage.Contains(l.StorageCtx, hash.RawString()); err == nil {
		if len(hash) == bittorrent.InfoHashV2Len {
			if containsV2, errV2 := l.Storage.Contains(l.StorageCtx, hash.TruncateV1().RawString()); err == nil {
				contains = contains || containsV2
			} else {
				err = errV2
			}
		}
	}
	if err != nil {
		log.Err(err)
	}
	return contains != l.Invert
}
