// Package s3 implements container which
// checks if hash present in any of torrent file
// placed in S3-like storage.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"

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
	Endpoint     string
	Region       string
	KeyID        string `cfg:"key_id"`
	KeySecret    string `cfg:"key_secret"`
	SessionToken string `cfg:"session_token"`
	Bucket       string
	Prefix       string
	Suffix       string
	Period       time.Duration
}

func init() {
	container.Register("s3", build)
}

func build(conf conf.MapConfig, st storage.DataStorage) (container.Container, error) {
	c := new(Config)
	if err := conf.Unmarshal(c); err != nil {
		return nil, fmt.Errorf("unable to deserialise configuration: %w", err)
	}

	if len(c.Bucket) == 0 {
		return nil, errors.New("no bucket provided")
	}

	if c.Period == 0 {
		logger.Warn().
			Str("name", "Period").
			Dur("provided", 0).
			Dur("default", defaultPeriod).
			Msg("falling back to default configuration")
		c.Period = defaultPeriod
	}

	modifiers := make([]func(*config.LoadOptions) error, 1, 4)

	modifiers[0] = config.WithLogger(logging.LoggerFunc(func(
		classification logging.Classification, format string, v ...interface{},
	) {
		if classification == logging.Debug {
			logger.Debug().CallerSkipFrame(1).Msg(fmt.Sprintf(format, v...))
		} else if classification == logging.Warn {
			logger.Warn().CallerSkipFrame(1).Msg(fmt.Sprintf(format, v...))
		}
	}))

	if len(c.Endpoint) > 0 {
		modifiers = append(modifiers, config.WithBaseEndpoint(c.Endpoint))
	}

	if len(c.Region) > 0 {
		modifiers = append(modifiers, config.WithRegion(c.Endpoint))
	}

	if len(c.KeyID) > 0 || len(c.KeySecret) > 0 || len(c.SessionToken) > 0 {
		modifiers = append(modifiers, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.KeyID, c.KeySecret, c.SessionToken)),
		)
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), modifiers...)
	if err != nil {
		return nil, fmt.Errorf("unable load AWS S3 SDK configuration: %w", err)
	}

	if len(c.KeyID) > 0 || len(c.KeySecret) > 0 || len(c.SessionToken) > 0 {
		awsCfg.Credentials = credentials.NewStaticCredentialsProvider(c.KeyID, c.KeySecret, c.SessionToken)
	}

	s := directory.NewScanner(list.List{
		Invert:     c.Invert,
		Storage:    st,
		StorageCtx: c.StorageCtx,
	}, s3{
		client: awss3.NewFromConfig(awsCfg),
		bucket: c.Bucket,
		prefix: c.Prefix,
		suffix: c.Suffix,
	}, c.Period)
	go s.Run()

	return s, err
}

type s3Client interface {
	ListObjectsV2(
		ctx context.Context, input *awss3.ListObjectsV2Input, f ...func(*awss3.Options),
	) (*awss3.ListObjectsV2Output, error)
	GetObject(
		ctx context.Context, params *awss3.GetObjectInput, optFns ...func(*awss3.Options),
	) (*awss3.GetObjectOutput, error)
}

type s3 struct {
	client                 s3Client
	bucket, prefix, suffix string
}

var _ directory.PathReader = s3{}

func (s s3) ReadDir() (it iter.Seq[string], err error) {
	search := &awss3.ListObjectsV2Input{Bucket: &s.bucket}
	if len(s.prefix) > 0 {
		search.Prefix = &s.prefix
	}
	entries, err := s.client.ListObjectsV2(context.Background(), search)
	if err == nil {
		it = func(yield func(string) bool) {
			for _, e := range entries.Contents {
				logger.Trace().Any("content", e).Msg("s3 read")
				if e.Key != nil && strings.HasSuffix(*e.Key, s.suffix) {
					if !yield(*e.Key) {
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
		Bucket: &s.bucket,
		Key:    &entry,
	})
	if err == nil {
		data = result.Body
	}
	return
}
