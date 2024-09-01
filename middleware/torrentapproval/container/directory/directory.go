// Package directory implements container which
// checks if hash present in any of torrent file
// placed in some directory.
// Note: Unlike List, this container also stores torrent name as value
package directory

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/sha256-simd"

	"github.com/zeebo/bencode"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container/list"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/str2bytes"
	"github.com/sot-tech/mochi/storage"
)

var logger = log.NewLogger("middleware/torrent approval/directory")

const (
	defaultPeriod  = time.Minute
	maxTorrentSize = 10 * 1024 * 1024
)

func init() {
	container.Register("directory", build)
}

// Config - implementation of directory container configuration.
// Extends list.Config because uses the same storage and Approved function.
type Config struct {
	list.Config
	// Path in filesystem where torrent files stored and should be watched
	Path string
	// Period is time between two Path checks
	Period time.Duration
}

func build(conf conf.MapConfig, st storage.DataStorage) (container.Container, error) {
	c := new(Config)
	if err := conf.Unmarshal(c); err != nil {
		return nil, fmt.Errorf("unable to deserialise configuration: %w", err)
	}
	var err error
	d := &directory{
		List: list.List{
			Invert:     c.Invert,
			Storage:    st,
			StorageCtx: c.StorageCtx,
		},
		closed: make(chan bool),
	}
	if len(d.StorageCtx) == 0 {
		logger.Warn().
			Str("name", "StorageCtx").
			Str("provided", d.StorageCtx).
			Str("default", container.DefaultStorageCtxName).
			Msg("falling back to default configuration")
		d.StorageCtx = container.DefaultStorageCtxName
	}
	if c.Period == 0 {
		logger.Warn().
			Str("name", "Period").
			Dur("provided", 0).
			Dur("default", defaultPeriod).
			Msg("falling back to default configuration")
		c.Period = defaultPeriod
	}
	go d.runScan(c.Path, c.Period)
	return d, err
}

// BencodeRawBytes wrapper for byte slice to get raw 'info' section from
// torrent file
type BencodeRawBytes []byte

// UnmarshalBencode just appends raw byte slice to result
func (ba *BencodeRawBytes) UnmarshalBencode(in []byte) error {
	*ba = append([]byte(nil), in...)
	return nil
}

type torrentRawInfoStruct struct {
	Info BencodeRawBytes `bencode:"info"`
}

type torrentNameInfoStruct struct {
	Name string `bencode:"name"`
}

func (d *directory) runScan(path string, period time.Duration) {
	t := time.NewTicker(period)
	defer t.Stop()
	files := make(map[string][2]bittorrent.InfoHash)
	tmpFiles := make(map[string]bool)
	// nolint:gosec
	s1, s2 := sha1.New(), sha256.New()
	for {
		select {
		case <-d.closed:
			return
		case <-t.C:
			logger.Debug().Msg("starting directory scan")
			if entries, err := os.ReadDir(path); err == nil {
				for _, e := range entries {
					if !e.IsDir() && strings.ToLower(filepath.Ext(e.Name())) == ".torrent" {
						tmpFiles[filepath.Join(path, e.Name())] = true
					}
				}
				for p := range tmpFiles {
					if _, exists := files[p]; !exists {
						var f *os.File
						if f, err = os.Open(p); err == nil {
							var info torrentRawInfoStruct
							err = bencode.NewDecoder(io.LimitReader(f, maxTorrentSize)).Decode(&info)
							_ = f.Close()
							if err == nil {
								s1.Write(info.Info)
								h1, _ := bittorrent.NewInfoHash(s1.Sum(nil))
								s1.Reset()

								s2.Write(info.Info)
								h2, _ := bittorrent.NewInfoHash(s2.Sum(nil))
								s2.Reset()

								files[p] = [2]bittorrent.InfoHash{h1, h2}
								var name torrentNameInfoStruct
								if err := bencode.DecodeBytes(info.Info, &name); err != nil {
									logger.Warn().
										Err(err).
										Str("file", p).
										Msg("unable to unmarshal torrent info")
								}
								if len(name.Name) == 0 {
									name.Name = list.DUMMY
								}
								bName := str2bytes.StringToBytes(name.Name)
								logger.Err(d.Storage.Put(context.Background(), d.StorageCtx, storage.Entry{
									Key:   h1.RawString(),
									Value: bName,
								}, storage.Entry{
									Key:   h2.RawString(),
									Value: bName,
								}, storage.Entry{
									Key:   h2.TruncateV1().RawString(),
									Value: bName,
								})).
									Str("file", p).
									Stringer("infoHash", h1).
									Stringer("infoHashV2", h2).
									Msg("added torrent to approval list")
							}
						}
						if err != nil {
							logger.Warn().Err(err).Str("file", p).Msg("unable to read file")
						}
					}
				}
				for p, ih := range files {
					if _, isOk := tmpFiles[p]; !isOk {
						delete(files, p)
						logger.Err(d.Storage.Delete(context.Background(), d.StorageCtx, ih[0].RawString(),
							ih[1].RawString(), ih[1].TruncateV1().RawString())).
							Str("file", p).
							Stringer("infoHash", ih[1]).
							Stringer("infoHashV2", ih[1]).
							Msg("deleted torrent from approval list")
					}
				}
				clear(tmpFiles)
			} else {
				logger.Warn().Err(err).Msg("unable to get directory content")
			}
		}
	}
}

type directory struct {
	list.List
	closed chan bool
}

// Close closes watching of torrent directory
func (d *directory) Close() error {
	if d.closed != nil {
		close(d.closed)
	}
	return nil
}
