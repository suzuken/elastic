// Copyright 2012-2014 Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

const (
	defaultKeepAlive = "5m"
)

var (
	// End of stream (or scan)
	EOS = errors.New("EOS")

	// No ScrollId
	ErrNoScrollId = errors.New("elastic: No scrollId")
)

// ScanService manages a cursor through documents in Elasticsearch.
type ScanService struct {
	client    *Client
	indices   []string
	types     []string
	keepAlive string
	query     Query
	size      *int
	pretty    bool
	debug     bool
}

func NewScanService(client *Client) *ScanService {
	builder := &ScanService{
		client: client,
		query:  NewMatchAllQuery(),
		debug:  false,
		pretty: false,
	}
	return builder
}

func (s *ScanService) Index(index string) *ScanService {
	if s.indices == nil {
		s.indices = make([]string, 0)
	}
	s.indices = append(s.indices, index)
	return s
}

func (s *ScanService) Indices(indices ...string) *ScanService {
	if s.indices == nil {
		s.indices = make([]string, 0)
	}
	s.indices = append(s.indices, indices...)
	return s
}

func (s *ScanService) Type(typ string) *ScanService {
	if s.types == nil {
		s.types = make([]string, 0)
	}
	s.types = append(s.types, typ)
	return s
}

func (s *ScanService) Types(types ...string) *ScanService {
	if s.types == nil {
		s.types = make([]string, 0)
	}
	s.types = append(s.types, types...)
	return s
}

// Scroll is an alias for KeepAlive, the time to keep
// the cursor alive (e.g. "5m" for 5 minutes).
func (s *ScanService) Scroll(keepAlive string) *ScanService {
	s.keepAlive = keepAlive
	return s
}

// KeepAlive sets the maximum time the cursor will be
// available before expiration (e.g. "5m" for 5 minutes).
func (s *ScanService) KeepAlive(keepAlive string) *ScanService {
	s.keepAlive = keepAlive
	return s
}

func (s *ScanService) Query(query Query) *ScanService {
	s.query = query
	return s
}

func (s *ScanService) Pretty(pretty bool) *ScanService {
	s.pretty = pretty
	return s
}

func (s *ScanService) Debug(debug bool) *ScanService {
	s.debug = debug
	return s
}

func (s *ScanService) Size(size int) *ScanService {
	s.size = &size
	return s
}

func (s *ScanService) Do() (*ScanCursor, error) {
	// Build url
	urls := "/"

	// Indices part
	indexPart := make([]string, 0)
	for _, index := range s.indices {
		indexPart = append(indexPart, cleanPathString(index))
	}
	if len(indexPart) > 0 {
		urls += strings.Join(indexPart, ",")
	}

	// Types
	typesPart := make([]string, 0)
	for _, typ := range s.types {
		typesPart = append(typesPart, cleanPathString(typ))
	}
	if len(typesPart) > 0 {
		urls += "/" + strings.Join(typesPart, ",")
	}

	// Search
	urls += "/_search"

	// Parameters
	params := make(url.Values)
	params.Set("search_type", "scan")
	if s.pretty {
		params.Set("pretty", fmt.Sprintf("%v", s.pretty))
	}
	if s.keepAlive != "" {
		params.Set("scroll", s.keepAlive)
	} else {
		params.Set("scroll", defaultKeepAlive)
	}
	if s.size != nil && *s.size > 0 {
		params.Set("size", fmt.Sprintf("%d", *s.size))
	}
	if len(params) > 0 {
		urls += "?" + params.Encode()
	}

	// Set up a new request
	req, err := s.client.NewRequest("POST", urls)
	if err != nil {
		return nil, err
	}

	// Set body
	body := make(map[string]interface{})

	// Query
	if s.query != nil {
		body["query"] = s.query.Source()
	}

	req.SetBodyJson(body)

	if s.debug {
		out, _ := httputil.DumpRequestOut((*http.Request)(req), true)
		fmt.Printf("%s\n", string(out))
	}

	// Get response
	res, err := s.client.c.Do((*http.Request)(req))
	if err != nil {
		return nil, err
	}
	if err := checkResponse(res); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if s.debug {
		out, _ := httputil.DumpResponse(res, true)
		fmt.Printf("%s\n", string(out))
	}

	searchResult := new(SearchResult)
	if err := json.NewDecoder(res.Body).Decode(searchResult); err != nil {
		return nil, err
	}

	cursor := NewScanCursor(s.client, s.keepAlive, s.pretty, s.debug, searchResult)

	return cursor, nil
}

// scanCursor represents a single page of results from
// an Elasticsearch Scan operation.
type ScanCursor struct {
	Results *SearchResult

	client      *Client
	keepAlive   string
	pretty      bool
	debug       bool
	currentPage int
}

// newScanCursor returns a new initialized instance
// of scanCursor.
func NewScanCursor(client *Client, keepAlive string, pretty, debug bool, searchResult *SearchResult) *ScanCursor {
	return &ScanCursor{
		client:    client,
		keepAlive: keepAlive,
		pretty:    pretty,
		debug:     debug,
		Results:   searchResult,
	}
}

// TotalHits is a convenience method that returns the number
// of hits the cursor will iterate through.
func (c *ScanCursor) TotalHits() int64 {
	if c.Results.Hits == nil {
		return 0
	}
	return c.Results.Hits.TotalHits
}

// Next returns the next search result or nil when all
// documents have been scanned.
//
// Usage:
//
//   for {
//     res, err := cursor.Next()
//     if err == elastic.EOS {
//       // End of stream (or scan)
//       break
//     }
//     if err != nil {
//       // Handle error
//     }
//     // Work with res
//   }
//
func (c *ScanCursor) Next() (*SearchResult, error) {
	if c.currentPage > 0 {
		if c.Results.Hits == nil || len(c.Results.Hits.Hits) == 0 || c.Results.Hits.TotalHits == 0 {
			return nil, EOS
		}
	}
	if c.Results.ScrollId == "" {
		return nil, ErrNoScrollId
	}

	// Build url
	urls := "/_search/scroll"

	// Parameters
	params := make(url.Values)
	if c.pretty {
		params.Set("pretty", fmt.Sprintf("%v", c.pretty))
	}
	if c.keepAlive != "" {
		params.Set("scroll", c.keepAlive)
	} else {
		params.Set("scroll", defaultKeepAlive)
	}
	urls += "?" + params.Encode()

	// Set up a new request
	req, err := c.client.NewRequest("POST", urls)
	if err != nil {
		return nil, err
	}

	// Set body
	req.SetBodyString(c.Results.ScrollId)

	if c.debug {
		out, _ := httputil.DumpRequestOut((*http.Request)(req), true)
		log.Printf("%s\n", string(out))
	}

	// Get response
	res, err := c.client.c.Do((*http.Request)(req))
	if err != nil {
		return nil, err
	}
	if err := checkResponse(res); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if c.debug {
		out, _ := httputil.DumpResponse(res, true)
		log.Printf("%s\n", string(out))
	}

	if err := json.NewDecoder(res.Body).Decode(c.Results); err != nil {
		return nil, err
	}

	c.currentPage += 1

	return c.Results, nil
}
