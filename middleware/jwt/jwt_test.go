package jwt

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cr "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/minio/sha256-simd"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
)

const (
	privKeyPEM = `
-----BEGIN PRIVATE KEY-----
MEECAQAwEwYHKoZIzj0CAQYIKoZIzj0DAQcEJzAlAgEBBCCI7Zc2IUKazCBCK5VY
WxxE6lVhGR+exaWgrh0Yq9t4gQ==
-----END PRIVATE KEY-----
`
)

var (
	privKey  *ecdsa.PrivateKey
	infoHash bittorrent.InfoHash
	jwksData JWKSKeys
)

type JWKSKey struct {
	KeyType   string `json:"kty"`
	Usage     string `json:"use"`
	KeyID     string `json:"kid"`
	Algorithm string `json:"alg"`
	Curve     string `json:"crv"`
	X         string `json:"x"`
	Y         string `json:"y"`
}

type JWKSKeys struct {
	Keys []JWKSKey `json:"keys"`
}

type params map[string]string

func (p params) GetString(key string) (out string, found bool) {
	out, found = p[key]
	return
}

func (params) MarshalZerologObject(*zerolog.Event) {}

func init() {
	_ = log.ConfigureLogger("", "info", false, false)
	privKey, _ = jwt.ParseECPrivateKeyFromPEM([]byte(privKeyPEM))
	ihBytes := make([]byte, bittorrent.InfoHashV1Len)
	if _, err := cr.Read(ihBytes); err != nil {
		panic(err)
	}
	infoHash, _ = bittorrent.NewInfoHash(ihBytes)
	s2 := sha256.New()
	s2.Write(elliptic.Marshal(privKey.PublicKey.Curve, privKey.PublicKey.X, privKey.PublicKey.Y))
	jwksData = JWKSKeys{Keys: []JWKSKey{
		{
			KeyType:   "EC",
			Usage:     "sig",
			KeyID:     base64.RawURLEncoding.EncodeToString(s2.Sum(nil)),
			Algorithm: jwt.SigningMethodES256.Name,
			Curve:     privKey.Curve.Params().Name,
			X:         base64.RawURLEncoding.EncodeToString(privKey.PublicKey.X.Bytes()),
			Y:         base64.RawURLEncoding.EncodeToString(privKey.PublicKey.Y.Bytes()),
		},
	}}
}

func TestHook_HandleAnnounceValid(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(jwksData)
	}))
	defer s.Close()

	token := jwt.NewWithClaims(jwt.SigningMethodES256, announceClaims{
		registeredClaimsWrapper: registeredClaimsWrapper{
			RegisteredClaims: jwt.RegisteredClaims{
				Issuer:    "CN=test",
				Subject:   "CN=test",
				Audience:  []string{"test"},
				ExpiresAt: &jwt.NumericDate{Time: time.Now().Add(time.Hour)},
				NotBefore: &jwt.NumericDate{Time: time.Now().Add(-time.Hour)},
				ID:        strconv.FormatInt(rand.Int63(), 16),
			},
		},
		InfoHash: infoHash.String(),
	})

	token.Header["kid"] = jwksData.Keys[0].KeyID
	tokenString, err := token.SignedString(privKey)
	require.Nil(t, err)
	//goland:noinspection HttpUrlsUsage
	cfg := conf.MapConfig{
		"handle_announce":         true,
		"issuer":                  "CN=test",
		"audience":                "test",
		"jwk_set_url":             "http://" + s.Listener.Addr().String(),
		"jwk_set_update_interval": time.Minute,
	}
	h, err := build(cfg, nil)
	require.Nil(t, err)
	data := make(params)
	data[authorizationHeader] = bearerAuthPrefix + tokenString
	_, err = h.HandleAnnounce(context.Background(), &bittorrent.AnnounceRequest{
		InfoHash:    infoHash,
		RequestPeer: bittorrent.RequestPeer{},
		Params:      data,
	}, nil)
	require.Nil(t, err)
}

func TestHook_HandleAnnounceInvalid(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(jwksData)
	}))
	defer s.Close()

	// now we wll use HMAC-SHA256 with invalid random key
	// all errors should be nil except announce request
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, announceClaims{
		registeredClaimsWrapper: registeredClaimsWrapper{
			RegisteredClaims: jwt.RegisteredClaims{
				Issuer:    "CN=test",
				Subject:   "CN=test",
				Audience:  []string{"test"},
				ExpiresAt: &jwt.NumericDate{Time: time.Now().Add(time.Hour)},
				NotBefore: &jwt.NumericDate{Time: time.Now().Add(-time.Hour)},
				ID:        strconv.FormatInt(rand.Int63(), 16),
			},
		},
		InfoHash: infoHash.String(),
	})

	token.Header["kid"] = jwksData.Keys[0].KeyID
	k := make([]byte, 20)
	if _, err := cr.Read(k); err != nil {
		panic(err)
	}
	tokenString, err := token.SignedString(k)
	require.Nil(t, err)
	//goland:noinspection HttpUrlsUsage
	cfg := conf.MapConfig{
		"handle_announce":         true,
		"header":                  "jwt",
		"issuer":                  "CN=test",
		"audience":                "test",
		"jwk_set_url":             "http://" + s.Listener.Addr().String(),
		"jwk_set_update_interval": time.Minute,
	}
	h, err := build(cfg, nil)
	require.Nil(t, err)
	data := make(params)
	data["jwt"] = tokenString
	_, err = h.HandleAnnounce(context.Background(), &bittorrent.AnnounceRequest{
		InfoHash:    infoHash,
		RequestPeer: bittorrent.RequestPeer{},
		Params:      data,
	}, nil)
	require.NotNil(t, err)
}

func TestHook_HandleScrapeValid(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(jwksData)
	}))
	defer s.Close()

	ihs := make(bittorrent.InfoHashes, rand.Intn(10)+1)
	ihss := make([]string, len(ihs))
	for i := range ihs {
		bb := []byte(infoHash)
		bb[i] = byte(i)
		ihs[i] = bittorrent.InfoHash(bb)
		ihss[i] = ihs[i].String()
	}

	token := jwt.NewWithClaims(jwt.SigningMethodES256, scrapeClaims{
		registeredClaimsWrapper: registeredClaimsWrapper{
			RegisteredClaims: jwt.RegisteredClaims{
				Issuer:    "CN=test",
				Subject:   "CN=test",
				Audience:  []string{"test"},
				ExpiresAt: &jwt.NumericDate{Time: time.Now().Add(time.Hour)},
				NotBefore: &jwt.NumericDate{Time: time.Now().Add(-time.Hour)},
				ID:        strconv.FormatInt(rand.Int63(), 16),
			},
		},
		InfoHashes: ihss,
	})

	token.Header["kid"] = jwksData.Keys[0].KeyID
	tokenString, err := token.SignedString(privKey)
	require.Nil(t, err)
	//goland:noinspection HttpUrlsUsage
	cfg := conf.MapConfig{
		"handle_scrape":           true,
		"issuer":                  "CN=test",
		"audience":                "test",
		"jwk_set_url":             "http://" + s.Listener.Addr().String(),
		"jwk_set_update_interval": time.Minute,
	}
	h, err := build(cfg, nil)
	require.Nil(t, err)
	data := make(params)
	data[authorizationHeader] = bearerAuthPrefix + tokenString
	_, err = h.HandleScrape(context.Background(), &bittorrent.ScrapeRequest{
		InfoHashes:       ihs,
		RequestAddresses: bittorrent.RequestAddresses{},
		Params:           data,
	}, nil)
	require.Nil(t, err)
}
