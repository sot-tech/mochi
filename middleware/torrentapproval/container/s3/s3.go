package s3

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container/list"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/pkg/str2bytes"
	"github.com/sot-tech/mochi/storage"
	"github.com/zeebo/bencode"
)

var logger = log.NewLogger("middleware/torrent approval/s3")

const (
	defaultPeriod  = time.Minute
	maxTorrentSize = 10 * 1024 * 1024
)

// Config - implementation of directory container configuration.
// Extends list.Config because uses the same storage and Approved function.
type Config struct {
	list.Config
	Bucket string
	Path string
	Period time.Duration
}

type s3 struct {
	list.List
	closed chan bool
}

func init() {
	container.Register("s3", build)
}

func build(conf conf.MapConfig, st storage.DataStorage) (container.Container, error) {
	c := new(Config)
	if err := conf.Unmarshal(c); err != nil {
		return nil, fmt.Errorf("unable to deserialise configuration: %w", err)
	}
	var err error
	s := &s3{
		List: list.List{
			Invert:     c.Invert,
			Storage:    st,
			StorageCtx: c.StorageCtx,
		},
		closed: make(chan bool),
	}
	if len(s.StorageCtx) == 0 {
		logger.Warn().
			Str("name", "StorageCtx").
			Str("provided", s.StorageCtx).
			Str("default", container.DefaultStorageCtxName).
			Msg("falling back to default configuration")
		s.StorageCtx = container.DefaultStorageCtxName
	}
	if c.Period == 0 {
		logger.Warn().
			Str("name", "Period").
			Dur("provided", 0).
			Dur("default", defaultPeriod).
			Msg("falling back to default configuration")
		c.Period = defaultPeriod
	}

	ctx := context.Background()
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable load aws sdk configuration: %w", err)
	}
	s3Client := awss3.NewFromConfig(sdkConfig)

	go s.runScan(ctx, c.Bucket, c.Path, s3Client, c.Period)
	return s, err
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

func (s *s3) runScan(ctx context.Context, bucket, prefix string, s3Client *awss3.Client,  period time.Duration) {
	t := time.NewTicker(period)
	defer t.Stop()
	files := make(map[string][2]bittorrent.InfoHash)
	tmpFiles := make(map[string]bool)
	// nolint:gosec
	s1, s2 := sha1.New(), sha256.New()
	for {
		select {
		case <-s.closed:
			return
		case <-t.C:
			logger.Debug().Msg("starting directory scan")
			listObj := &awss3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix}
			if entries, err := s3Client.ListObjectsV2(ctx, listObj); err == nil {
				for _, e := range entries.Contents {
					if strings.ToLower(filepath.Ext(*e.Key)) == ".torrent" {
						tmpFiles[filepath.Join(prefix, *e.Key)] = true
					}
				}
				for p := range tmpFiles {
					if _, exists := files[p]; !exists {
						requestInput := &awss3.GetObjectInput{
					        Bucket: aws.String(bucket),
					        Key:    aws.String(p),
					    }

					    result, err := s3Client.GetObject(ctx, requestInput)
					    if err != nil {
					        log.Print(err)
					    }
						var info torrentRawInfoStruct
						err = bencode.NewDecoder(io.LimitReader(result.Body, maxTorrentSize)).Decode(&info)
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
							logger.Err(s.Storage.Put(ctx, s.StorageCtx, storage.Entry{
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
				for p, ih := range files {
					if _, isOk := tmpFiles[p]; !isOk {
						delete(files, p)
						logger.Err(s.Storage.Delete(ctx, s.StorageCtx, ih[0].RawString(),
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
