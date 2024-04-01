// Package jwt implements a Hook that fails on Announce or Scrape if the client's request
// is missing a valid JSON Web Token.
//
// JWTs are validated against the standard claims in RFC7519 along with an
// extra "infohash(es)" claim that verifies the client has access to the Swarm.
package jwt

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/MicahParks/jwkset"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"

	"github.com/sot-tech/mochi/bittorrent"
	"github.com/sot-tech/mochi/middleware"
	"github.com/sot-tech/mochi/pkg/conf"
	"github.com/sot-tech/mochi/pkg/log"
	"github.com/sot-tech/mochi/storage"
)

const (
	authorizationHeader = "authorization"
	bearerAuthPrefix    = "bearer "
)

func init() {
	middleware.RegisterBuilder("jwt", build)
}

var (
	logger = log.NewLogger("middleware/jwt")
	// ErrMissingJWT is returned when a JWT is missing from a request.
	ErrMissingJWT = bittorrent.ClientError("request not allowed by mochi: missing jwt")

	// ErrInvalidJWT is returned when a JWT fails to verify.
	ErrInvalidJWT = bittorrent.ClientError("request not allowed by mochi: invalid jwt")

	errJWKsNotSet = errors.New("required parameters not provided: Issuer/Audience/JWKSetURL")

	hmacAlgorithms = jwt.WithValidMethods([]string{
		jwt.SigningMethodHS256.Alg(), jwt.SigningMethodHS384.Alg(), jwt.SigningMethodHS512.Alg(),
		jwt.SigningMethodRS256.Alg(), jwt.SigningMethodRS384.Alg(), jwt.SigningMethodRS512.Alg(),
		jwt.SigningMethodPS256.Alg(), jwt.SigningMethodPS384.Alg(), jwt.SigningMethodPS512.Alg(),
		jwt.SigningMethodES256.Alg(), jwt.SigningMethodES384.Alg(), jwt.SigningMethodES512.Alg(),
		jwt.SigningMethodEdDSA.Alg(),
	})
)

// Config represents all the values required by this middleware to fetch JWKs
// and verify JWTs.
type Config struct {
	Header            string
	Issuer            string
	Audience          string
	JWKSetURL         string        `cfg:"jwk_set_url"`
	JWKUpdateInterval time.Duration `cfg:"jwk_set_update_interval"`
	HandleAnnounce    bool          `cfg:"handle_announce"`
	HandleScrape      bool          `cfg:"handle_scrape"`
}

type hook struct {
	cfg    Config
	jwks   keyfunc.Keyfunc
	parser *jwt.Parser
}

func build(config conf.MapConfig, _ storage.PeerStorage) (h middleware.Hook, err error) {
	var cfg Config

	if err = config.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to deserialise configuration: %w", err)
	}

	if len(cfg.JWKSetURL) > 0 && len(cfg.Issuer) > 0 && len(cfg.Audience) > 0 {
		if len(cfg.Header) == 0 {
			cfg.Header = authorizationHeader
			logger.Warn().
				Str("name", "Header").
				Str("provided", "").
				Str("default", cfg.Header).
				Msg("falling back to default configuration")
		}

		var jwks keyfunc.Keyfunc
		if cfg.HandleAnnounce || cfg.HandleScrape {
			var jwkURL *url.URL
			jwkURL, err = url.Parse(cfg.JWKSetURL)
			if err == nil {
				var httpStorage jwkset.Storage
				httpStorage, err = jwkset.NewStorageFromHTTP(jwkURL, jwkset.HTTPClientStorageOptions{
					NoErrorReturnFirstHTTPReq: true,
					RefreshErrorHandler: func(_ context.Context, err error) {
						logger.Error().Err(err).Msg("error occurred while updating JWKs")
					},
					RefreshInterval: cfg.JWKUpdateInterval,
					Storage:         nil,
				})
				if err == nil {
					jwks, err = keyfunc.New(keyfunc.Options{Storage: httpStorage})
				}
			}
		} else {
			logger.Warn().Msg("both announce and scrape handle disabled")
		}
		if err == nil {
			h = &hook{
				cfg:    cfg,
				jwks:   jwks,
				parser: jwt.NewParser(jwt.WithAudience(cfg.Audience), jwt.WithIssuer(cfg.Issuer), hmacAlgorithms),
			}
		}
	} else {
		err = errJWKsNotSet
	}

	return
}

type announceClaims struct {
	jwt.RegisteredClaims
	InfoHash string `json:"infohash,omitempty"`
}

func (h *hook) HandleAnnounce(ctx context.Context, req *bittorrent.AnnounceRequest, _ *bittorrent.AnnounceResponse) (context.Context, error) {
	if !h.cfg.HandleAnnounce {
		return ctx, nil
	}
	var err error

	if jwtParam := h.getJWTString(req.Params); len(jwtParam) == 0 {
		err = ErrMissingJWT
	} else {
		claims := new(announceClaims)
		if _, jwtErr := h.parser.ParseWithClaims(jwtParam, claims, h.jwks.KeyfuncCtx(ctx)); jwtErr == nil {
			var claimIH bittorrent.InfoHash
			if claimIH, err = bittorrent.NewInfoHashString(claims.InfoHash); err != nil {
				logger.Info().
					Err(err).
					Object("source", req.RequestPeer).
					Msg("'infohash' claim parse failed")
				err = ErrInvalidJWT
			}
			if req.InfoHash != claimIH {
				logger.Info().
					Stringer("claimInfoHash", claimIH).
					Stringer("requestInfoHash", req.InfoHash).
					Object("source", req.RequestPeer).
					Msg("unequal 'infohash' claim when validating JWT")
				err = ErrInvalidJWT
			}
		} else {
			logger.Info().
				Err(jwtErr).
				Object("source", req.RequestPeer).
				Msg("JWT validation failed")
			err = ErrInvalidJWT
		}
	}

	return ctx, err
}

type scrapeClaims struct {
	jwt.RegisteredClaims
	InfoHashes []string `json:"infohashes,omitempty"`
}

func (h *hook) HandleScrape(ctx context.Context, req *bittorrent.ScrapeRequest, _ *bittorrent.ScrapeResponse) (context.Context, error) {
	if !h.cfg.HandleScrape {
		return ctx, nil
	}

	var err error

	if jwtParam := h.getJWTString(req.Params); len(jwtParam) == 0 {
		err = ErrMissingJWT
	} else {
		claims := new(scrapeClaims)
		if _, jwtErr := h.parser.ParseWithClaims(jwtParam, claims, h.jwks.KeyfuncCtx(ctx)); jwtErr == nil {
			var claimIHs bittorrent.InfoHashes
			for _, s := range claims.InfoHashes {
				if providedIh, err := bittorrent.NewInfoHashString(s); err == nil {
					claimIHs = append(claimIHs, providedIh)
				} else {
					logger.Info().
						Err(err).
						Array("addresses", &req.RequestAddresses).
						Msg("'infohashes' claim parse failed")
				}
			}
			eq := len(req.InfoHashes) == len(claimIHs)
			if eq {
				for _, rIH := range req.InfoHashes {
					found := false
					for _, cIH := range claimIHs {
						if rIH == cIH {
							found = true
							break
						}
					}
					if !found {
						eq = false
						break
					}
				}
			}
			if !eq {
				logger.Info().
					Array("claimInfoHashes", claimIHs).
					Array("requestInfoHashes", req.InfoHashes).
					Array("addresses", &req.RequestAddresses).
					Msg("unequal 'infohashes' claim when validating JWT")
				err = ErrInvalidJWT
			}
		} else {
			logger.Info().
				Err(jwtErr).
				Array("addresses", &req.RequestAddresses).
				Msg("JWT validation failed")
			err = ErrInvalidJWT
		}
	}

	return ctx, err
}

func (h *hook) getJWTString(params bittorrent.Params) (jwt string) {
	if params != nil {
		var found bool
		if jwt, found = params.GetString(h.cfg.Header); found {
			if strings.HasPrefix(strings.ToLower(jwt), bearerAuthPrefix) {
				jwt = jwt[len(bearerAuthPrefix):]
			}
		}
	}
	return
}
