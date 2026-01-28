package client

import "encoding/json"

// SearchResult is the result of a search operation.
type SearchResult struct {
	TookInMillis int64
	Hits         *SearchHits
	Aggregations Aggregations
}

// SearchHits contains the search hits.
type SearchHits struct {
	TotalHits *TotalHits
	Hits      []*SearchHit
}

// TotalHits contains the total number of hits.
type TotalHits struct {
	Value    int64
	Relation string // "eq" or "gte"
}

// SearchHit represents a single search hit.
type SearchHit struct {
	Index  string
	ID     string
	Source json.RawMessage
	Sort   []any
}

// Aggregations is a map of aggregation results.
type Aggregations map[string]json.RawMessage

// GetResult is the result of a get operation.
type GetResult struct {
	Index   string
	ID      string
	Found   bool
	Source  json.RawMessage
	Version *int64
}

// BulkResponse is the response from a bulk request.
type BulkResponse struct {
	Took   int
	Errors bool
	Items  []map[string]*BulkResponseItem
}

// BulkResponseItem is the result of a single bulk operation.
type BulkResponseItem struct {
	Index   string `json:"_index"`
	ID      string `json:"_id"`
	Version int64  `json:"_version"`
	Result  string `json:"result"`
	Status  int    `json:"status"`
	Error   *ErrorDetails
}

// ErrorDetails contains error information from Elasticsearch.
type ErrorDetails struct {
	Type         string         `json:"type"`
	Reason       string         `json:"reason"`
	ResourceType string         `json:"resource.type,omitempty"`
	ResourceID   string         `json:"resource.id,omitempty"`
	Index        string         `json:"index,omitempty"`
	RootCause    []*ErrorDetails `json:"root_cause,omitempty"`
	CausedBy     *ErrorDetails  `json:"caused_by,omitempty"`
}

// Error returns the error as a string.
func (e *ErrorDetails) Error() string {
	if e == nil {
		return ""
	}
	return e.Reason
}

// CatIndicesResponseRow represents a single row from the _cat/indices API.
type CatIndicesResponseRow struct {
	Health       string `json:"health"`
	Status       string `json:"status"`
	Index        string `json:"index"`
	UUID         string `json:"uuid"`
	Pri          string `json:"pri"`
	Rep          string `json:"rep"`
	DocsCount    string `json:"docs.count"`
	DocsDeleted  string `json:"docs.deleted"`
	StoreSize    string `json:"store.size"`
	PriStoreSize string `json:"pri.store.size"`
}

// CatIndicesResponse is the response from the _cat/indices API.
type CatIndicesResponse []CatIndicesResponseRow

// IndicesGetSettingsResponse contains settings for an index.
type IndicesGetSettingsResponse struct {
	Settings map[string]any `json:"settings"`
}
