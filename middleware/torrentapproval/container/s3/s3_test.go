package s3

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"github.com/sot-tech/mochi/middleware/torrentapproval/container/directory"
	"github.com/sot-tech/mochi/middleware/torrentapproval/container/list"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/storage/memory"
)

type testData struct {
	data []byte
	hash string
}

func unHEX(s string) string {
	b, _ := hex.DecodeString(s)
	return string(b)
}

func unBase64(s string) []byte {
	b, _ := base64.StdEncoding.DecodeString(s)
	return b
}

// contains created torrent file data from simple txt documents.
// data base64 encoded because `pieces` section in torrent file is raw bytes
var files = map[string]testData{
	"test0.torrent": {
		data: unBase64(`ZDEwOmNyZWF0ZWQgYnkzMTpUcmFuc21pc3Npb24vNC4wLjYgKDM4YzE2NDkzM2UpMTM6Y3JlYXRp
b24gZGF0ZWkxNzU1ODYxOTI3ZTg6ZW5jb2Rpbmc1OlVURi04NDppbmZvZDY6bGVuZ3RoaTVlNDpu
YW1lODp0ZXN0LnR4dDEyOnBpZWNlIGxlbmd0aGkzMjc2OGU2OnBpZWNlczIwOk4SQ70ixm52wrqe
3cH5E5Tlf5+DZWU=`),
		hash: unHEX("a10e8e9e81702bf8482f251551ff4fe011cba6a7"),
	},
	"test1.torrent": {
		data: unBase64(`ZDEwOmNyZWF0ZWQgYnkzMTpUcmFuc21pc3Npb24vNC4wLjYgKDM4YzE2NDkzM2UpMTM6Y3JlYXRp
b24gZGF0ZWkxNzU2MTIzNzEwZTg6ZW5jb2Rpbmc1OlVURi04NDppbmZvZDY6bGVuZ3RoaTRlNDpu
YW1lOTp0ZXN0MC50eHQxMjpwaWVjZSBsZW5ndGhpMzI3NjhlNjpwaWVjZXMyMDqo/cIFqfGcwcdQ
emDE8BsT0R1/0GVl`),
		hash: unHEX("e86d393bd458d2acc46d5467bc8cb8b30b1bfa77"),
	},
}

func init() {
	_ = log.ConfigureLogger("", "warn", false, false)
}

type mockS3 struct {
	objs []types.Object
}

func (m *mockS3) ListObjectsV2(
	context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options),
) (*awss3.ListObjectsV2Output, error) {
	return &awss3.ListObjectsV2Output{
		Contents: m.objs,
	}, nil
}

var _ s3Client = &mockS3{}

func (m *mockS3) GetObject(
	_ context.Context, params *awss3.GetObjectInput, _ ...func(*awss3.Options),
) (*awss3.GetObjectOutput, error) {
	if params == nil || params.Key == nil {
		return nil, nil
	}
	v := files[*params.Key]
	return &awss3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(v.data)),
	}, nil
}

func TestScanMock(t *testing.T) {
	cl := &mockS3{make([]types.Object, 0, len(files))}
	for k := range files {
		cl.objs = append(cl.objs, types.Object{Key: &k})
	}
	st, _ := memory.Builder{}.NewDataStorage(make(conf.MapConfig))
	d := directory.NewScanner(list.List{
		Invert:     false,
		Storage:    st,
		StorageCtx: "TEST",
	}, s3{
		client: cl,
	}, time.Millisecond*10)
	go d.Run()
	t.Cleanup(func() {
		_ = d.Close()
	})

	time.Sleep(time.Millisecond * 100)
	for name, f := range files {
		contains, _ := d.Storage.Contains(context.Background(), "TEST", f.hash)
		require.True(t, contains, "%s must present", name)
		for i := 0; i < len(cl.objs); i++ {
			if *cl.objs[i].Key == name {
				cl.objs = append(cl.objs[:i], cl.objs[i+1:]...)
			}
		}
	}

	time.Sleep(time.Millisecond * 100)
	for name, f := range files {
		contains, _ := d.Storage.Contains(context.Background(), "TEST", f.hash)
		require.False(t, contains, "%s must absent", name)
	}
}

var (
	minioEndpoint = "http://127.0.0.1:9000"
	minioBucket   = "test"
	minioPrefix   = "test/"
)

const (
	minioKeyID  = "minioadmin"
	minioSecret = "minioadmin"
	minioRegion = "us-east-1"
)

// TestScanMinio requires real minio instance listening 127.0.0.1:9000
// with default login, password and region (minioadmin/minioadmin, us-east-1)
func TestScanMinio(t *testing.T) {
	st, _ := memory.Builder{}.NewDataStorage(make(conf.MapConfig))
	awsCfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	awsCfg.BaseEndpoint = &minioEndpoint
	awsCfg.Region = minioRegion
	awsCfg.Credentials = credentials.NewStaticCredentialsProvider(minioKeyID, minioSecret, "")
	cl := awss3.NewFromConfig(awsCfg)

	_, _ = cl.CreateBucket(context.Background(), &awss3.CreateBucketInput{Bucket: &minioBucket})

	for name, data := range files {
		_, err = cl.PutObject(context.Background(), &awss3.PutObjectInput{
			Bucket: &minioBucket,
			Key:    &name,
			Body:   bytes.NewReader(data.data),
		})
		if err != nil {
			t.Fatal(err)
		}
		name = minioPrefix + name
		_, err = cl.PutObject(context.Background(), &awss3.PutObjectInput{
			Bucket: &minioBucket,
			Key:    &name,
			Body:   bytes.NewReader(data.data),
		})
		if err != nil {
			t.Fatal(err)
		}
		name += "1"
		_, err = cl.PutObject(context.Background(), &awss3.PutObjectInput{
			Bucket: &minioBucket,
			Key:    &name,
			Body:   bytes.NewReader(data.data),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	s3Dir := s3{
		client: cl,
		bucket: minioBucket,
		prefix: minioPrefix,
		suffix: ".torrent",
	}

	if content, err := s3Dir.ReadDir(); err == nil {
		var i int
		for range content {
			i++
		}
		require.Equal(t, len(files), i, "S3 content data not the same as test data")
	} else {
		t.Fatal(err)
	}

	d := directory.NewScanner(list.List{
		Invert:     false,
		Storage:    st,
		StorageCtx: "TEST",
	}, s3Dir, time.Millisecond*100)

	go d.Run()
	t.Cleanup(func() {
		_ = d.Close()
	})
	time.Sleep(time.Millisecond * 200)

	for name, f := range files {
		contains, _ := d.Storage.Contains(context.Background(), "TEST", f.hash)
		require.True(t, contains, "%s must present", name)
		name = minioPrefix + name
		_, err = cl.DeleteObject(context.Background(), &awss3.DeleteObjectInput{
			Bucket: &minioBucket,
			Key:    &name,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(time.Millisecond * 200)

	for name, f := range files {
		contains, _ := d.Storage.Contains(context.Background(), "TEST", f.hash)
		require.False(t, contains, "%s must absent", name)
	}
}
