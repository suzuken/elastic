package elastic

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/olivere/elastic/uritemplates"
)

// MultiTermvectorService returns information and statistics on terms in the
// fields of a particular document. The document could be stored in the
// index or artificially provided by the user.
//
// See https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-termvectors.html
// for documentation.
type MultiTermvectorService struct {
	client     *Client
	pretty     bool
	index      string
	typ        string
	preference string
	realtime   *bool
	refresh    *bool
	docs       []*MultiTermvectorItem
}

// NewMultiTermvectorService creates a new MultiTermvectorService.
func NewMultiTermvectorService(client *Client) *MultiTermvectorService {
	return &MultiTermvectorService{
		client: client,
	}
}

// Pretty indicates that the JSON response be indented and human readable.
func (s *MultiTermvectorService) Pretty(pretty bool) *MultiTermvectorService {
	s.pretty = pretty
	return s
}

// Add adds documents to MultiTermvectors service.
func (s *MultiTermvectorService) Add(docs ...*MultiTermvectorItem) *MultiTermvectorService {
	s.docs = append(s.docs, docs...)
	return s
}

// Index in which the document resides.
func (s *MultiTermvectorService) Index(index string) *MultiTermvectorService {
	s.index = index
	return s
}

// Type of the document.
func (s *MultiTermvectorService) Type(typ string) *MultiTermvectorService {
	s.typ = typ
	return s
}

//MultiTermvectorItem is a single document to retrive via MultiTermvectorService.
type MultiTermvectorItem struct {
	index            string
	typ              string
	id               string
	doc              interface{}
	fieldStatistics  *bool
	fields           []string
	perFieldAnalyzer map[string]string
	offsets          *bool
	parent           string
	payloads         *bool
	positions        *bool
	preference       string
	realtime         *bool
	routing          string
	termStatistics   *bool
}

func NewMultiTermvectorItem() *MultiTermvectorItem {
	return &MultiTermvectorItem{}
}

func (s *MultiTermvectorItem) Index(index string) *MultiTermvectorItem {
	s.index = index
	return s
}

func (s *MultiTermvectorItem) Type(typ string) *MultiTermvectorItem {
	s.typ = typ
	return s
}

func (s *MultiTermvectorItem) Id(id string) *MultiTermvectorItem {
	s.id = id
	return s
}

// Doc is the document to analyze.
func (s *MultiTermvectorItem) Doc(doc interface{}) *MultiTermvectorItem {
	s.doc = doc
	return s
}

// FieldStatistics specifies if document count, sum of document frequencies
// and sum of total term frequencies should be returned.
func (s *MultiTermvectorItem) FieldStatistics(fieldStatistics bool) *MultiTermvectorItem {
	s.fieldStatistics = &fieldStatistics
	return s
}

// Fields a list of fields to return.
func (s *MultiTermvectorItem) Fields(fields ...string) *MultiTermvectorItem {
	if s.fields == nil {
		s.fields = make([]string, 0)
	}
	s.fields = append(s.fields, fields...)
	return s
}

// PerFieldAnalyzer allows to specify a different analyzer than the one
// at the field.
func (s *MultiTermvectorItem) PerFieldAnalyzer(perFieldAnalyzer map[string]string) *MultiTermvectorItem {
	s.perFieldAnalyzer = perFieldAnalyzer
	return s
}

// Offsets specifies if term offsets should be returned.
func (s *MultiTermvectorItem) Offsets(offsets bool) *MultiTermvectorItem {
	s.offsets = &offsets
	return s
}

// Parent id of documents.
func (s *MultiTermvectorItem) Parent(parent string) *MultiTermvectorItem {
	s.parent = parent
	return s
}

// Payloads specifies if term payloads should be returned.
func (s *MultiTermvectorItem) Payloads(payloads bool) *MultiTermvectorItem {
	s.payloads = &payloads
	return s
}

// Positions specifies if term positions should be returned.
func (s *MultiTermvectorItem) Positions(positions bool) *MultiTermvectorItem {
	s.positions = &positions
	return s
}

// Preference specify the node or shard the operation
// should be performed on (default: random).
func (s *MultiTermvectorItem) Preference(preference string) *MultiTermvectorItem {
	s.preference = preference
	return s
}

// Realtime specifies if request is real-time as opposed to
// near-real-time (default: true).
func (s *MultiTermvectorItem) Realtime(realtime bool) *MultiTermvectorItem {
	s.realtime = &realtime
	return s
}

// Routing is a specific routing value.
func (s *MultiTermvectorItem) Routing(routing string) *MultiTermvectorItem {
	s.routing = routing
	return s
}

// TermStatistics specifies if total term frequency and document frequency
// should be returned.
func (s *MultiTermvectorItem) TermStatistics(termStatistics bool) *MultiTermvectorItem {
	s.termStatistics = &termStatistics
	return s
}

// Source returns the serialized JSON to be sent to Elasticsearch as
// part of a MultiTermvector.
func (s *MultiTermvectorItem) Source() interface{} {
	source := make(map[string]interface{})

	source["_id"] = s.id

	if s.index != "" {
		source["_index"] = s.index
	}
	if s.typ != "" {
		source["_type"] = s.typ
	}
	if s.fields != nil {
		source["fields"] = s.fields
	}
	if s.fieldStatistics != nil {
		source["field_statistics"] = fmt.Sprintf("%v", *s.fieldStatistics)
	}
	if s.offsets != nil {
		source["offsets"] = s.offsets
	}
	if s.parent != "" {
		source["parant"] = s.parent
	}
	if s.payloads != nil {
		source["payloads"] = fmt.Sprintf("%v", *s.payloads)
	}
	if s.positions != nil {
		source["positions"] = fmt.Sprintf("%v", *s.positions)
	}
	if s.preference != "" {
		source["preference"] = s.preference
	}
	if s.realtime != nil {
		source["realtime"] = fmt.Sprintf("%v", *s.realtime)
	}
	if s.routing != "" {
		source["routing"] = s.routing
	}
	if s.termStatistics != nil {
		source["term_statistics"] = fmt.Sprintf("%v", *s.termStatistics)
	}
	if s.doc != nil {
		source["doc"] = s.doc
	}
	if s.perFieldAnalyzer != nil && len(s.perFieldAnalyzer) > 0 {
		source["per_field_analyzer"] = s.perFieldAnalyzer
	}

	return source
}

func (s *MultiTermvectorService) Source() interface{} {
	source := make(map[string]interface{})
	docs := make([]interface{}, len(s.docs))
	for i, doc := range s.docs {
		docs[i] = doc.Source()
	}
	source["docs"] = docs
	return source
}

// buildURL builds the URL for the operation.
func (s *MultiTermvectorService) buildURL() (string, url.Values, error) {
	var path string
	var err error

	if s.index != "" && s.typ != "" {
		path, err = uritemplates.Expand("/{index}/{type}/_mtermvectors", map[string]string{
			"index": s.index,
			"type":  s.typ,
		})
	} else if s.index != "" {
		path, err = uritemplates.Expand("/{index}/_mtermvectors", map[string]string{
			"index": s.index,
		})
	} else {
		path = "/_mtermvectors"
	}
	if err != nil {
		return "", url.Values{}, err
	}

	// Add query string parameters
	params := url.Values{}
	if s.pretty {
		params.Set("pretty", "1")
	}
	if s.preference != "" {
		params.Set("preference", s.preference)
	}
	if s.realtime != nil {
		params.Set("realtime", fmt.Sprintf("%v", *s.realtime))
	}
	return path, params, nil
}

// Validate checks if the operation is valid.
func (s *MultiTermvectorService) Validate() error {
	var invalid []string
	if s.index == "" {
		invalid = append(invalid, "Index")
	}
	if s.typ == "" {
		invalid = append(invalid, "Type")
	}
	if len(invalid) > 0 {
		return fmt.Errorf("missing required fields: %v", invalid)
	}
	return nil
}

// Do executes the operation.
func (s *MultiTermvectorService) Do() (*MultiTermvectorResponse, error) {
	// Check pre-conditions
	if err := s.Validate(); err != nil {
		return nil, err
	}

	// Get URL for request
	path, params, err := s.buildURL()
	if err != nil {
		return nil, err
	}
	body := s.Source()

	// Get HTTP response
	res, err := s.client.PerformRequest("GET", path, params, body)
	if err != nil {
		return nil, err
	}

	// Return operation response
	ret := new(MultiTermvectorResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// MultiTermvectorResponse is the response of MultiTermvectorService.Do.
type MultiTermvectorResponse struct {
	Docs []*TermvectorsResponse `json:"docs,omitempty"`
}
