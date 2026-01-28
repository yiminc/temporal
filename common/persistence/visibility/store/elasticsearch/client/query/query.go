// Package query provides Elasticsearch query builders that output map[string]any.
// This package is independent of any specific Elasticsearch client library.
package query

// Query is the interface for all Elasticsearch query types.
// It provides a Source() method that returns the query as a map suitable for JSON serialization.
type Query interface {
	// Source returns the query as a map[string]any for JSON serialization.
	Source() (map[string]any, error)
}

// Sorter is the interface for Elasticsearch sort options.
type Sorter interface {
	// Source returns the sorter as a map[string]any for JSON serialization.
	Source() (map[string]any, error)
}

// Aggregation is the interface for Elasticsearch aggregations.
type Aggregation interface {
	// Source returns the aggregation as a map[string]any for JSON serialization.
	Source() (map[string]any, error)
}
