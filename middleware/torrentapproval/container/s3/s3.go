// Package s3 implements container which
// checks if hash present in any of torrent file
// placed in S3-like storage.
package s3

import (
	"context"
	"fmt"
	"io"
	"iter"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/sot-tech/mochi/middleware/torrentapproval/container"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container/directory"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container/list"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/storage"
)

var logger = log.NewLogger("middleware/torrent approval/s3")

const defaultPeriod = time.Minute

// Config - implementation of S3 container configuration.
// Extends list.Config because uses the same storage and Approved function.
type Config struct {
	list.Config
	Bucket string
	Prefix string
	Period time.Duration
}

func init() {
	container.Register("s3", build)
}

func build(conf conf.MapConfig, st storage.DataStorage) (container.Container, error) {
	c := new(Config)
	if err := conf.Unmarshal(c); err != nil {
		return nil, fmt.Errorf("unable to deserialise configuration: %w", err)
	}
	if c.Period == 0 {
		logger.Warn().
			Str("name", "Period").
			Dur("provided", 0).
			Dur("default", defaultPeriod).
			Msg("falling back to default configuration")
		c.Period = defaultPeriod
	}

	sdkConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("unable load AWS S3 SDK configuration: %w", err)
	}

	s := directory.NewScanner(list.List{
		Invert:     c.Invert,
		Storage:    st,
		StorageCtx: c.StorageCtx,
	}, s3{client: awss3.NewFromConfig(sdkConfig), bucket: c.Bucket, prefix: c.Prefix})
	go s.Run(c.Period)

	return s, err
}

type s3 struct {
	client         *awss3.Client
	bucket, prefix string
}

var _ directory.PathReader = s3{}

func (s s3) ReadDir() (it iter.Seq[string], err error) {
	entries, err := s.client.ListObjectsV2(context.Background(), &awss3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: &s.prefix,
	})
	if err == nil {
		it = func(yield func(string) bool) {
			for _, e := range entries.Contents {
				if e.Key != nil && strings.ToLower(filepath.Ext(*e.Key)) == ".torrent" {
					if !yield(filepath.Join(s.prefix, *e.Key)) {
						return
					}
				}
			}
		}
	}
	return it, err
}

func (s s3) ReadData(entry string) (data io.ReadCloser, err error) {
	var result *awss3.GetObjectOutput
	result, err = s.client.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(entry),
	})
	if err == nil {
		data = result.Body
	}
	return
}
