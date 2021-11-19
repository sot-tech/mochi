// Package list implements container with pre-defined
// list of torrent hashes from config file
package list

import (
	"encoding/hex"
	"fmt"
	bittorrent "github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/middleware/torrentapproval/container"
	"github.com/chihaya/chihaya/storage"
	"gopkg.in/yaml.v2"
	"sync"
)

func init() {
	container.Register("list", build)
}

type Config struct {
	Whitelist []string `yaml:"whitelist"`
	Blacklist []string `yaml:"blacklist"`
}

var DUMMY struct{}

// TODO: change sync map to provided storage
func build(confBytes []byte, storage storage.Storage) (container.Container, error) {
	c := new(Config)
	if err := yaml.Unmarshal(confBytes, c); err != nil {
		return nil, fmt.Errorf("unable to deserialise configuration: %v", err)
	}
	if len(c.Whitelist) > 0 && len(c.Blacklist) > 0 {
		return nil, fmt.Errorf("using both whitelist and blacklist is invalid")
	}
	l := &List{
		Hashes: sync.Map{},
		Invert: len(c.Whitelist) == 0,
	}

	hashList := c.Whitelist
	if l.Invert {
		hashList = c.Blacklist
	}

	for _, hashString := range hashList {
		hashBytes, err := hex.DecodeString(hashString)
		if err != nil {
			return nil, fmt.Errorf("whitelist : invalid hash %s, %v", hashString, err)
		}
		ih, err := bittorrent.NewInfoHash(hashBytes)
		if err != nil {
			return nil, fmt.Errorf("whitelist : %s : %v", hashString, err)
		}
		l.Hashes.Store(ih, DUMMY)
	}
	return l, nil
}

type List struct {
	Invert bool
	Hashes sync.Map
}

func (l *List) Contains(hash bittorrent.InfoHash) bool {
	_, result := l.Hashes.Load(hash)
	return result != l.Invert
}
