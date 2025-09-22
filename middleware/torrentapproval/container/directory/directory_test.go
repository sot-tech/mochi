package directory

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sot-tech/mochi/middleware/torrentapproval/container/list"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/storage/memory"
)

type testData struct {
	name string
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
var files = [2]testData{
	{
		name: "test0.torrent",
		data: unBase64(`ZDEwOmNyZWF0ZWQgYnkzMTpUcmFuc21pc3Npb24vNC4wLjYgKDM4YzE2NDkzM2UpMTM6Y3JlYXRp
b24gZGF0ZWkxNzU1ODYxOTI3ZTg6ZW5jb2Rpbmc1OlVURi04NDppbmZvZDY6bGVuZ3RoaTVlNDpu
YW1lODp0ZXN0LnR4dDEyOnBpZWNlIGxlbmd0aGkzMjc2OGU2OnBpZWNlczIwOk4SQ70ixm52wrqe
3cH5E5Tlf5+DZWU=`),
		hash: unHEX("a10e8e9e81702bf8482f251551ff4fe011cba6a7"),
	},
	{
		name: "test1.torrent",
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

func writeTmp() (string, error) {
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return "", err
	}
	for _, f := range files {
		if err = os.WriteFile(filepath.Join(tmpDir, f.name), f.data, 0o600); err != nil {
			break
		}
	}
	return tmpDir, err
}

func TestScan(t *testing.T) {
	tmpDir, err := writeTmp()
	t.Cleanup(func() {
		err := os.RemoveAll(tmpDir)
		if err != nil {
			t.Log(err)
		}
	})
	if err != nil {
		t.Error(err)
		return
	}
	st, _ := memory.Builder{}.NewDataStorage(make(conf.MapConfig))
	d := NewScanner(list.List{
		Invert:     false,
		Storage:    st,
		StorageCtx: "TEST",
	}, path(tmpDir), time.Millisecond*10)
	go d.Run()
	t.Cleanup(func() {
		_ = d.Close()
	})
	time.Sleep(time.Millisecond * 100)
	for _, f := range files {
		contains, _ := d.Storage.Contains(context.Background(), "TEST", f.hash)
		require.True(t, contains, "%s must present", f.name)
		_ = os.Remove(filepath.Join(tmpDir, f.name))
	}

	time.Sleep(time.Millisecond * 100)
	for _, f := range files {
		contains, _ := d.Storage.Contains(context.Background(), "TEST", f.hash)
		require.False(t, contains, "%s must absent", f.name)
	}
}
