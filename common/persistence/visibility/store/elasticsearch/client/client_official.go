package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client/query"
)

type (
	// clientImpl implements Client using the official Elasticsearch client
	clientImpl struct {
		esClient *elasticsearch.Client
		baseURL  string
	}
)

var _ Client = (*clientImpl)(nil)
var _ CLIClient = (*clientImpl)(nil)
var _ IntegrationTestsClient = (*clientImpl)(nil)

// newClient creates a new ES client using the official elasticsearch-go client
func newClient(cfg *Config, httpClient *http.Client, logger log.Logger) (*clientImpl, error) {
	var addresses []string
	if len(cfg.URLs) > 0 {
		addresses = make([]string, len(cfg.URLs))
		for i, u := range cfg.URLs {
			addresses[i] = u.String()
		}
	} else {
		addresses = []string{cfg.URL.String()}
	}

	if httpClient == nil {
		if configHTTPClient := cfg.GetHttpClient(); configHTTPClient != nil {
			httpClient = configHTTPClient
		} else if cfg.TLS != nil && cfg.TLS.Enabled {
			tlsHttpClient, err := buildTLSHTTPClient(cfg.TLS)
			if err != nil {
				return nil, fmt.Errorf("unable to create TLS HTTP client: %w", err)
			}
			httpClient = tlsHttpClient
		} else {
			httpClient = http.DefaultClient
		}
	}

	// Close idle connections periodically if configured
	if cfg.CloseIdleConnectionsInterval != time.Duration(0) {
		interval := cfg.CloseIdleConnectionsInterval
		if interval < minimumCloseIdleConnectionsInterval {
			interval = minimumCloseIdleConnectionsInterval
		}
		go func(interval time.Duration, httpClient *http.Client) {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for range ticker.C {
				httpClient.CloseIdleConnections()
			}
		}(interval, httpClient)
	}

	esCfg := elasticsearch.Config{
		Addresses:         addresses,
		Username:          cfg.Username,
		Password:          cfg.Password,
		Transport:         httpClient.Transport,
		DisableRetry:      false,
		EnableDebugLogger: strings.EqualFold(cfg.LogLevel, "trace"),
		CompressRequestBody: !cfg.DisableGzip,
	}

	// Configure retry behavior similar to olivere client
	if !cfg.EnableSniff {
		esCfg.DiscoverNodesOnStart = false
	}

	esClient, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create Elasticsearch client: %w", err)
	}

	// Perform health check if configured
	if cfg.EnableHealthcheck {
		res, err := esClient.Cluster.Health(
			esClient.Cluster.Health.WithContext(context.Background()),
			esClient.Cluster.Health.WithWaitForStatus("yellow"),
			esClient.Cluster.Health.WithTimeout(10*time.Second),
		)
		if err != nil {
			return nil, fmt.Errorf("Elasticsearch health check failed: %w", err)
		}
		res.Body.Close()
	}

	return &clientImpl{
		esClient: esClient,
		baseURL:  addresses[0],
	}, nil
}

// buildTLSHTTPClient creates an HTTP client with TLS configuration
func buildTLSHTTPClient(config *auth.TLS) (*http.Client, error) {
	tlsConfig, err := auth.NewTLSConfig(config)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	tlsClient := &http.Client{Transport: transport}

	return tlsClient, nil
}

func (c *clientImpl) Get(ctx context.Context, index string, docID string) (*GetResult, error) {
	res, err := c.esClient.Get(index, docID, c.esClient.Get.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, c.parseError(res)
	}

	var result GetResult
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode get response: %w", err)
	}
	return &result, nil
}

func (c *clientImpl) Search(ctx context.Context, p *SearchParameters) (*SearchResult, error) {
	// Build search body
	searchBody := make(map[string]any)

	if p.Query != nil {
		querySource, err := p.Query.Source()
		if err != nil {
			return nil, fmt.Errorf("failed to build query: %w", err)
		}
		searchBody["query"] = querySource
	}

	if p.PageSize > 0 {
		searchBody["size"] = p.PageSize
	}

	if len(p.Sorter) > 0 {
		sorts := make([]any, 0, len(p.Sorter))
		for _, sorter := range p.Sorter {
			sortSource, err := sorter.Source()
			if err != nil {
				return nil, fmt.Errorf("failed to build sorter: %w", err)
			}
			sorts = append(sorts, sortSource)
		}
		searchBody["sort"] = sorts
	}

	if len(p.SearchAfter) > 0 {
		searchBody["search_after"] = p.SearchAfter
	}

	// Disable total hits tracking for performance
	searchBody["track_total_hits"] = false

	bodyBytes, err := json.Marshal(searchBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search body: %w", err)
	}

	res, err := c.esClient.Search(
		c.esClient.Search.WithContext(ctx),
		c.esClient.Search.WithIndex(p.Index),
		c.esClient.Search.WithBody(bytes.NewReader(bodyBytes)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, c.parseError(res)
	}

	return c.parseSearchResponse(res.Body)
}

func (c *clientImpl) Count(ctx context.Context, index string, q query.Query) (int64, error) {
	countBody := make(map[string]any)
	if q != nil {
		querySource, err := q.Source()
		if err != nil {
			return 0, fmt.Errorf("failed to build query: %w", err)
		}
		countBody["query"] = querySource
	}

	bodyBytes, err := json.Marshal(countBody)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal count body: %w", err)
	}

	res, err := c.esClient.Count(
		c.esClient.Count.WithContext(ctx),
		c.esClient.Count.WithIndex(index),
		c.esClient.Count.WithBody(bytes.NewReader(bodyBytes)),
	)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, c.parseError(res)
	}

	var result struct {
		Count int64 `json:"count"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode count response: %w", err)
	}
	return result.Count, nil
}

func (c *clientImpl) CountGroupBy(
	ctx context.Context,
	index string,
	q query.Query,
	aggName string,
	agg query.Aggregation,
) (*SearchResult, error) {
	searchBody := map[string]any{
		"size":             0,
		"track_total_hits": false,
	}

	if q != nil {
		querySource, err := q.Source()
		if err != nil {
			return nil, fmt.Errorf("failed to build query: %w", err)
		}
		searchBody["query"] = querySource
	}

	if agg != nil {
		aggSource, err := agg.Source()
		if err != nil {
			return nil, fmt.Errorf("failed to build aggregation: %w", err)
		}
		searchBody["aggs"] = map[string]any{
			aggName: aggSource,
		}
	}

	bodyBytes, err := json.Marshal(searchBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search body: %w", err)
	}

	res, err := c.esClient.Search(
		c.esClient.Search.WithContext(ctx),
		c.esClient.Search.WithIndex(index),
		c.esClient.Search.WithBody(bytes.NewReader(bodyBytes)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, c.parseError(res)
	}

	return c.parseSearchResponse(res.Body)
}

func (c *clientImpl) RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (BulkProcessor, error) {
	// Create a new HTTP client using the same transport as the ES client
	// The bulk processor needs direct HTTP access to the ES bulk API
	httpClient := &http.Client{
		Transport: http.DefaultTransport,
	}
	return newBulkProcessorImpl(
		httpClient,
		c.baseURL,
		p,
	), nil
}

func (c *clientImpl) PutMapping(ctx context.Context, index string, mapping map[string]enumspb.IndexedValueType) (bool, error) {
	body := buildMappingBody(mapping)
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return false, fmt.Errorf("failed to marshal mapping body: %w", err)
	}

	res, err := c.esClient.Indices.PutMapping(
		[]string{index},
		bytes.NewReader(bodyBytes),
		c.esClient.Indices.PutMapping.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, c.parseError(res)
	}

	var result struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode put mapping response: %w", err)
	}
	return result.Acknowledged, nil
}

func (c *clientImpl) WaitForYellowStatus(ctx context.Context, index string) (string, error) {
	res, err := c.esClient.Cluster.Health(
		c.esClient.Cluster.Health.WithContext(ctx),
		c.esClient.Cluster.Health.WithIndex(index),
		c.esClient.Cluster.Health.WithWaitForStatus("yellow"),
	)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if res.IsError() {
		return "", c.parseError(res)
	}

	var result struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode cluster health response: %w", err)
	}
	return result.Status, nil
}

func (c *clientImpl) GetMapping(ctx context.Context, index string) (map[string]string, error) {
	res, err := c.esClient.Indices.GetMapping(
		c.esClient.Indices.GetMapping.WithContext(ctx),
		c.esClient.Indices.GetMapping.WithIndex(index),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, c.parseError(res)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("failed to decode get mapping response: %w", err)
	}

	return convertMappingBody(body, index), nil
}

func (c *clientImpl) IndexExists(ctx context.Context, indexName string) (bool, error) {
	res, err := c.esClient.Indices.Exists(
		[]string{indexName},
		c.esClient.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	return res.StatusCode == 200, nil
}

func (c *clientImpl) CreateIndex(ctx context.Context, index string, body map[string]any) (bool, error) {
	if body == nil {
		body = make(map[string]any)
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return false, fmt.Errorf("failed to marshal create index body: %w", err)
	}

	res, err := c.esClient.Indices.Create(
		index,
		c.esClient.Indices.Create.WithContext(ctx),
		c.esClient.Indices.Create.WithBody(bytes.NewReader(bodyBytes)),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, c.parseError(res)
	}

	var result struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode create index response: %w", err)
	}
	return result.Acknowledged, nil
}

func (c *clientImpl) DeleteIndex(ctx context.Context, indexName string) (bool, error) {
	res, err := c.esClient.Indices.Delete(
		[]string{indexName},
		c.esClient.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, c.parseError(res)
	}

	var result struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode delete index response: %w", err)
	}
	return result.Acknowledged, nil
}

func (c *clientImpl) CatIndices(ctx context.Context, target string) (CatIndicesResponse, error) {
	res, err := c.esClient.Cat.Indices(
		c.esClient.Cat.Indices.WithContext(ctx),
		c.esClient.Cat.Indices.WithIndex(target),
		c.esClient.Cat.Indices.WithFormat("json"),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, c.parseError(res)
	}

	var result CatIndicesResponse
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode cat indices response: %w", err)
	}
	return result, nil
}

// CLIClient methods

func (c *clientImpl) Delete(ctx context.Context, indexName string, docID string, version int64) error {
	res, err := c.esClient.Delete(
		indexName,
		docID,
		c.esClient.Delete.WithContext(ctx),
		c.esClient.Delete.WithVersion(int(version)),
		c.esClient.Delete.WithVersionType("external"),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() && res.StatusCode != 404 {
		return c.parseError(res)
	}
	return nil
}

func (c *clientImpl) IndexPutTemplate(ctx context.Context, templateName string, bodyString string) (bool, error) {
	res, err := c.esClient.Indices.PutTemplate(
		templateName,
		strings.NewReader(bodyString),
		c.esClient.Indices.PutTemplate.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, c.parseError(res)
	}

	var result struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode put template response: %w", err)
	}
	return result.Acknowledged, nil
}

func (c *clientImpl) IndexPutMapping(ctx context.Context, indexName string, bodyString string) (bool, error) {
	res, err := c.esClient.Indices.PutMapping(
		[]string{indexName},
		strings.NewReader(bodyString),
		c.esClient.Indices.PutMapping.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, c.parseError(res)
	}

	var result struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode put mapping response: %w", err)
	}
	return result.Acknowledged, nil
}

func (c *clientImpl) ClusterPutSettings(ctx context.Context, bodyString string) (bool, error) {
	res, err := c.esClient.Cluster.PutSettings(
		strings.NewReader(bodyString),
		c.esClient.Cluster.PutSettings.WithContext(ctx),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, c.parseError(res)
	}

	var result struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode cluster put settings response: %w", err)
	}
	return result.Acknowledged, nil
}

func (c *clientImpl) Ping(ctx context.Context) error {
	res, err := c.esClient.Ping(c.esClient.Ping.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return c.parseError(res)
	}
	return nil
}

// IntegrationTestsClient methods

func (c *clientImpl) IndexPutSettings(ctx context.Context, indexName string, bodyString string) (bool, error) {
	res, err := c.esClient.Indices.PutSettings(
		strings.NewReader(bodyString),
		c.esClient.Indices.PutSettings.WithContext(ctx),
		c.esClient.Indices.PutSettings.WithIndex(indexName),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, c.parseError(res)
	}

	var result struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("failed to decode put settings response: %w", err)
	}
	return result.Acknowledged, nil
}

func (c *clientImpl) IndexGetSettings(ctx context.Context, indexName string) (map[string]*IndicesGetSettingsResponse, error) {
	res, err := c.esClient.Indices.GetSettings(
		c.esClient.Indices.GetSettings.WithContext(ctx),
		c.esClient.Indices.GetSettings.WithIndex(indexName),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, c.parseError(res)
	}

	var result map[string]*IndicesGetSettingsResponse
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode get settings response: %w", err)
	}
	return result, nil
}

// Helper functions

func (c *clientImpl) parseError(res *esapi.Response) error {
	body, _ := io.ReadAll(res.Body)

	var errResp struct {
		Error *ErrorDetails `json:"error"`
	}
	if json.Unmarshal(body, &errResp) == nil && errResp.Error != nil {
		return &ESError{
			Status:  res.StatusCode,
			Message: string(body),
			Details: errResp.Error,
		}
	}

	return &ESError{
		Status:  res.StatusCode,
		Message: string(body),
	}
}

func (c *clientImpl) parseSearchResponse(body io.Reader) (*SearchResult, error) {
	// Use json.Number to preserve int64 precision
	decoder := json.NewDecoder(body)
	decoder.UseNumber()

	var raw struct {
		Took         json.Number `json:"took"`
		Hits         *struct {
			Total *struct {
				Value    json.Number `json:"value"`
				Relation string      `json:"relation"`
			} `json:"total"`
			Hits []struct {
				Index  string          `json:"_index"`
				ID     string          `json:"_id"`
				Source json.RawMessage `json:"_source"`
				Sort   []any           `json:"sort"`
			} `json:"hits"`
		} `json:"hits"`
		Aggregations map[string]json.RawMessage `json:"aggregations"`
	}

	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	took, _ := raw.Took.Int64()
	result := &SearchResult{
		TookInMillis: took,
		Aggregations: raw.Aggregations,
	}

	if raw.Hits != nil {
		result.Hits = &SearchHits{
			Hits: make([]*SearchHit, 0, len(raw.Hits.Hits)),
		}
		if raw.Hits.Total != nil {
			totalValue, _ := raw.Hits.Total.Value.Int64()
			result.Hits.TotalHits = &TotalHits{
				Value:    totalValue,
				Relation: raw.Hits.Total.Relation,
			}
		}
		for _, hit := range raw.Hits.Hits {
			result.Hits.Hits = append(result.Hits.Hits, &SearchHit{
				Index:  hit.Index,
				ID:     hit.ID,
				Source: hit.Source,
				Sort:   hit.Sort,
			})
		}
	}

	return result, nil
}

func buildMappingBody(mapping map[string]enumspb.IndexedValueType) map[string]interface{} {
	properties := make(map[string]interface{}, len(mapping))
	for fieldName, fieldType := range mapping {
		var typeMap map[string]interface{}
		switch fieldType {
		case enumspb.INDEXED_VALUE_TYPE_TEXT:
			typeMap = map[string]interface{}{"type": "text"}
		case enumspb.INDEXED_VALUE_TYPE_KEYWORD, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
			typeMap = map[string]interface{}{"type": "keyword"}
		case enumspb.INDEXED_VALUE_TYPE_INT:
			typeMap = map[string]interface{}{"type": "long"}
		case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
			typeMap = map[string]interface{}{
				"type":           "scaled_float",
				"scaling_factor": 10000,
			}
		case enumspb.INDEXED_VALUE_TYPE_BOOL:
			typeMap = map[string]interface{}{"type": "boolean"}
		case enumspb.INDEXED_VALUE_TYPE_DATETIME:
			typeMap = map[string]interface{}{"type": "date_nanos"}
		}
		if typeMap != nil {
			properties[fieldName] = typeMap
		}
	}

	body := map[string]interface{}{
		"properties": properties,
	}
	return body
}

func convertMappingBody(esMapping map[string]interface{}, indexName string) map[string]string {
	result := make(map[string]string)
	index, ok := esMapping[indexName]
	if !ok {
		return result
	}
	indexMap, ok := index.(map[string]interface{})
	if !ok {
		return result
	}
	mappings, ok := indexMap["mappings"]
	if !ok {
		return result
	}
	mappingsMap, ok := mappings.(map[string]interface{})
	if !ok {
		return result
	}

	properties, ok := mappingsMap["properties"]
	if !ok {
		return result
	}
	propMap, ok := properties.(map[string]interface{})
	if !ok {
		return result
	}

	for fieldName, fieldProp := range propMap {
		fieldPropMap, ok := fieldProp.(map[string]interface{})
		if !ok {
			continue
		}
		tYpe, ok := fieldPropMap["type"]
		if !ok {
			continue
		}
		typeStr, ok := tYpe.(string)
		if !ok {
			continue
		}
		result[fieldName] = typeStr
	}

	return result
}

// IsNotFoundError checks if the error is a 404 Not Found error
func IsNotFoundError(err error) bool {
	if esErr, ok := err.(*ESError); ok {
		return esErr.Status == 404
	}
	return false
}
