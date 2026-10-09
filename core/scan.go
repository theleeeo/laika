package core

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"time"

	"github.com/theleeeo/laika/core/resource"
)

const (
	// defaultScanMaxAge is Config.ScanMaxAge's default.
	defaultScanMaxAge = time.Hour
	// maxScanPageSize is a scan page's default and largest size.
	maxScanPageSize = 1000
)

// scanToken is what a scan's page token carries: the scan's start, the
// schema version and concrete index it pinned on its first page, the
// fingerprint of its request, and the backend's cursor. It is trusted to the
// consumer, neither encrypted nor signed: its checks catch a consumer's
// mistakes, not an attacker.
type scanToken struct {
	Start       int64  `json:"s"` // Unix nanoseconds
	Version     int    `json:"v"`
	Index       string `json:"i"`
	Fingerprint string `json:"f"`
	Cursor      string `json:"c"`
}

// encodeScanToken is base64url (unpadded) of the token's JSON.
func encodeScanToken(tok scanToken) string {
	b, err := json.Marshal(tok)
	if err != nil {
		panic(fmt.Sprintf("marshal scan token: %v", err)) // a struct of strings and ints always marshals
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeScanToken reverses encodeScanToken; anything that doesn't decode to a
// token with a start, a version and an index is ErrInvalidPageToken.
func decodeScanToken(s string) (scanToken, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return scanToken{}, fmt.Errorf("%w: %v", ErrInvalidPageToken, err)
	}
	var tok scanToken
	if err := json.Unmarshal(b, &tok); err != nil {
		return scanToken{}, fmt.Errorf("%w: %v", ErrInvalidPageToken, err)
	}
	if tok.Start == 0 || tok.Version == 0 || tok.Index == "" {
		return scanToken{}, fmt.Errorf("%w: incomplete token", ErrInvalidPageToken)
	}
	return tok, nil
}

// scanState is one scan request's pinning, carried in the context from
// Indexer.Search down the chain. Every stage that would read the read
// version's config reads vc instead, and searchBase searches index instead of
// the alias.
type scanState struct {
	resource string
	version  int
	vc       *resource.VersionConfig
	// index is the concrete index the scan pinned; empty when the read alias
	// didn't exist on its first page, which answers an empty result.
	index string
	start time.Time
	// pageToken is the request's PageToken as Indexer.Search decoded it, and
	// token its decoding; nil on the first page.
	pageToken string
	token     *scanToken
}

type scanStateKey struct{}

// withScanState replaces the context's scan state; nil clears it, so a
// search that isn't a scan never inherits an outer scan's.
func withScanState(ctx context.Context, st *scanState) context.Context {
	return context.WithValue(ctx, scanStateKey{}, st)
}

// scanStateFrom returns the context's scan state, nil when there is none.
func scanStateFrom(ctx context.Context) *scanState {
	st, _ := ctx.Value(scanStateKey{}).(*scanState)
	return st
}

// scanStateFor returns the scan state a stage below Indexer.Search runs req
// with. A stage never falls back to the read version for a scan: a state that
// is missing, for another resource, or without a config is ErrScanFault.
func scanStateFor(ctx context.Context, req SearchRequest) (*scanState, error) {
	st := scanStateFrom(ctx)
	switch {
	case st == nil:
		return nil, fmt.Errorf("%w: a scan of %q reached the search chain without its state", ErrScanFault, req.Resource)
	case st.resource != req.Resource:
		return nil, fmt.Errorf("%w: a scan of %q carries the state of a scan of %q", ErrScanFault, req.Resource, st.resource)
	case st.vc == nil:
		return nil, fmt.Errorf("%w: the scan of %q has no pinned config", ErrScanFault, req.Resource)
	}
	return st, nil
}

// searchVersionConfig is the config a search stage reads for req: the scan's
// pinned version during a scan, the read version otherwise. nil (and no
// error) for an unknown resource, which searchBase reports.
func (idx *Indexer) searchVersionConfig(ctx context.Context, req SearchRequest) (*resource.VersionConfig, error) {
	if req.Scan {
		st, err := scanStateFor(ctx, req)
		if err != nil {
			return nil, err
		}
		return st.vc, nil
	}
	cfg := idx.resources.Get(req.Resource)
	if cfg == nil {
		return nil, nil
	}
	return cfg.ReadVersionConfig(), nil
}

// beginScan builds a scan request's state before anything is validated
// against a config. A first page pins what the read alias serves; a later
// page what its token pinned, after the token's checks.
func (idx *Indexer) beginScan(ctx context.Context, cfg *resource.Config, req SearchRequest) (*scanState, error) {
	if req.Page != 0 {
		return nil, &InvalidArgumentError{Msg: "a scan pages by token: page must be 0"}
	}
	if req.PageToken == "" {
		return idx.pinReadAlias(ctx, cfg)
	}

	tok, err := decodeScanToken(req.PageToken)
	if err != nil {
		return nil, err
	}
	start := time.Unix(0, tok.Start)
	if age := idx.now().Sub(start); age > idx.scanMaxAge {
		return nil, fmt.Errorf("%w: the scan started %s ago, longer than the %s a scan may run", ErrCursorExpired, age, idx.scanMaxAge)
	}
	vc := cfg.GetVersion(tok.Version)
	if vc == nil {
		return nil, fmt.Errorf("%w: schema version %d of %q is no longer configured", ErrCursorExpired, tok.Version, cfg.Resource)
	}
	if IndexName(cfg.Resource, tok.Version) != tok.Index {
		return nil, fmt.Errorf("%w: index %q is not schema version %d's", ErrCursorExpired, tok.Index, tok.Version)
	}
	if tok.Cursor == "" {
		return nil, fmt.Errorf("%w: no backend cursor", ErrInvalidPageToken)
	}
	return &scanState{
		resource:  cfg.Resource,
		version:   tok.Version,
		vc:        vc,
		index:     tok.Index,
		start:     start,
		pageToken: req.PageToken,
		token:     &tok,
	}, nil
}

// pinReadAlias pins a scan's first page to the one index the read alias
// serves, under the configured version whose IndexName is exactly that
// index. An alias that doesn't exist yet pins the read version with no index.
func (idx *Indexer) pinReadAlias(ctx context.Context, cfg *resource.Config) (*scanState, error) {
	alias := AliasName(cfg.Resource)
	targets, err := idx.es.GetAliasTargets(ctx, alias)
	if err != nil {
		return nil, fmt.Errorf("resolve read alias %q: %w", alias, err)
	}
	st := &scanState{resource: cfg.Resource, start: idx.now()}
	switch len(targets) {
	case 0:
		st.version = cfg.ReadVersion
		st.vc = cfg.ReadVersionConfig()
		return st, nil
	case 1:
	default:
		return nil, fmt.Errorf("%w: read alias %q points to %d indices %v", ErrScanFault, alias, len(targets), targets)
	}
	for _, v := range cfg.SortedVersions() {
		if IndexName(cfg.Resource, v) == targets[0] {
			st.version = v
			st.vc = cfg.GetVersion(v)
			st.index = targets[0]
			return st, nil
		}
	}
	return nil, fmt.Errorf("%w: read alias %q points to %q, the index of no configured version", ErrScanFault, alias, targets[0])
}

// scanPage is the internal middleware New places after the registered
// middlewares, so it sees a scan's request as they leave it. It checks they
// kept the scan's resource, Scan and token, normalizes the page size,
// fingerprints the request and, on a later page, compares the fingerprint
// with the token's. Below it the request carries the backend's cursor; on the
// way out a non-empty backend cursor is wrapped in the scan's next token. A
// search that isn't a scan passes through.
func (idx *Indexer) scanPage(next SearchHandler) SearchHandler {
	return func(ctx context.Context, req SearchRequest) (SearchResponse, error) {
		st := scanStateFrom(ctx)
		if st == nil {
			if req.Scan {
				return SearchResponse{}, fmt.Errorf("%w: a scan of %q reached the search chain without its state", ErrScanFault, req.Resource)
			}
			return next(ctx, req)
		}
		switch {
		case req.Resource != st.resource:
			return SearchResponse{}, fmt.Errorf("%w: a middleware changed the scan's resource from %q to %q", ErrScanFault, st.resource, req.Resource)
		case !req.Scan:
			return SearchResponse{}, fmt.Errorf("%w: a middleware cleared Scan on a scan of %q", ErrScanFault, st.resource)
		case req.PageToken != st.pageToken:
			return SearchResponse{}, fmt.Errorf("%w: a middleware changed the page token of a scan of %q", ErrScanFault, st.resource)
		}

		req = normalizeScanPageSize(req)
		fp := scanFingerprint(req, st.version, st.index, st.vc)
		req.PageToken = ""
		if st.token != nil {
			if fp != st.token.Fingerprint {
				return SearchResponse{}, fmt.Errorf("%w: the token was issued for another request", ErrInvalidPageToken)
			}
			req.PageToken = st.token.Cursor
		}

		resp, err := next(ctx, req)
		if err != nil {
			return SearchResponse{}, err
		}
		if resp.NextPageToken != "" {
			resp.NextPageToken = encodeScanToken(scanToken{
				Start:       st.start.UnixNano(),
				Version:     st.version,
				Index:       st.index,
				Fingerprint: fp,
				Cursor:      resp.NextPageToken,
			})
		}
		return resp, nil
	}
}

// normalizeScanPageSize defaults a scan's page size to, and caps it at,
// maxScanPageSize.
func normalizeScanPageSize(req SearchRequest) SearchRequest {
	if req.PageSize <= 0 || req.PageSize > maxScanPageSize {
		req.PageSize = maxScanPageSize
	}
	return req
}

// scanFingerprint is SHA-256 over a canonical, length-prefixed encoding of
// every SearchRequest field except PageToken, then the pinned version, the
// pinned index and a hash of the pinned config's JSON, so a token is good
// only for the request, pinning and config that issued it.
// TestScanFingerprint_CoversEveryRequestField fails when SearchRequest,
// Filter or SortOption gains a field this doesn't encode.
func scanFingerprint(req SearchRequest, version int, index string, vc *resource.VersionConfig) string {
	h := sha256.New()
	str := func(s string) {
		writeUint(h, uint64(len(s)))
		h.Write([]byte(s))
	}
	num := func(n int64) { writeUint(h, uint64(n)) }
	flag := func(b bool) {
		if b {
			num(1)
		} else {
			num(0)
		}
	}

	str(req.Resource)
	str(req.Query)
	num(int64(req.Page))
	num(int64(req.PageSize))
	num(int64(len(req.Filters)))
	for _, f := range req.Filters {
		str(f.Field)
		num(int64(f.Op))
		str(f.Value)
		num(int64(len(f.Values)))
		for _, v := range f.Values {
			str(v)
		}
		str(f.NestedPath)
	}
	num(int64(len(req.Sort)))
	for _, s := range req.Sort {
		str(s.Field)
		flag(s.Desc)
	}
	str(req.Scope)
	flag(req.Scan)

	num(int64(version))
	str(index)
	vcJSON, err := json.Marshal(vc)
	if err != nil {
		panic(fmt.Sprintf("marshal version config: %v", err)) // plain structs, slices and strings always marshal
	}
	vcSum := sha256.Sum256(vcJSON)
	h.Write(vcSum[:])

	return hex.EncodeToString(h.Sum(nil))
}

func writeUint(h hash.Hash, n uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	h.Write(b[:])
}
