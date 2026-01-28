package query

import "fmt"

// BoolQuery is a query that matches documents matching boolean combinations of other queries.
type BoolQuery struct {
	mustClauses        []Query
	mustNotClauses     []Query
	filterClauses      []Query
	shouldClauses      []Query
	minimumShouldMatch string
}

var _ Query = (*BoolQuery)(nil)

// NewBoolQuery creates a new BoolQuery.
func NewBoolQuery() *BoolQuery {
	return &BoolQuery{}
}

// Must adds queries that must appear in matching documents.
func (q *BoolQuery) Must(queries ...Query) *BoolQuery {
	q.mustClauses = append(q.mustClauses, queries...)
	return q
}

// MustNot adds queries that must not appear in matching documents.
func (q *BoolQuery) MustNot(queries ...Query) *BoolQuery {
	q.mustNotClauses = append(q.mustNotClauses, queries...)
	return q
}

// Filter adds queries that must appear in matching documents but won't contribute to scoring.
func (q *BoolQuery) Filter(queries ...Query) *BoolQuery {
	q.filterClauses = append(q.filterClauses, queries...)
	return q
}

// Should adds queries that should appear in matching documents.
func (q *BoolQuery) Should(queries ...Query) *BoolQuery {
	q.shouldClauses = append(q.shouldClauses, queries...)
	return q
}

// MinimumNumberShouldMatch sets the minimum number of should clauses that must match.
func (q *BoolQuery) MinimumNumberShouldMatch(min int) *BoolQuery {
	q.minimumShouldMatch = fmt.Sprintf("%d", min)
	return q
}

// MinimumShouldMatch sets the minimum should match as a string (allows percentages).
func (q *BoolQuery) MinimumShouldMatch(min string) *BoolQuery {
	q.minimumShouldMatch = min
	return q
}

// MustClauses returns the must clauses.
func (q *BoolQuery) MustClauses() []Query {
	return q.mustClauses
}

// MustNotClauses returns the must_not clauses.
func (q *BoolQuery) MustNotClauses() []Query {
	return q.mustNotClauses
}

// FilterClauses returns the filter clauses.
func (q *BoolQuery) FilterClauses() []Query {
	return q.filterClauses
}

// ShouldClauses returns the should clauses.
func (q *BoolQuery) ShouldClauses() []Query {
	return q.shouldClauses
}

// Source returns the query as a map for JSON serialization.
func (q *BoolQuery) Source() (map[string]any, error) {
	boolClause := make(map[string]any)

	if len(q.mustClauses) > 0 {
		clauses, err := sourcesFromQueries(q.mustClauses)
		if err != nil {
			return nil, err
		}
		boolClause["must"] = clauses
	}

	if len(q.mustNotClauses) > 0 {
		clauses, err := sourcesFromQueries(q.mustNotClauses)
		if err != nil {
			return nil, err
		}
		boolClause["must_not"] = clauses
	}

	if len(q.filterClauses) > 0 {
		clauses, err := sourcesFromQueries(q.filterClauses)
		if err != nil {
			return nil, err
		}
		boolClause["filter"] = clauses
	}

	if len(q.shouldClauses) > 0 {
		clauses, err := sourcesFromQueries(q.shouldClauses)
		if err != nil {
			return nil, err
		}
		boolClause["should"] = clauses
	}

	if q.minimumShouldMatch != "" {
		boolClause["minimum_should_match"] = q.minimumShouldMatch
	}

	return map[string]any{
		"bool": boolClause,
	}, nil
}

// sourcesFromQueries converts a slice of Query to their source representations.
func sourcesFromQueries(queries []Query) ([]any, error) {
	sources := make([]any, 0, len(queries))
	for _, query := range queries {
		src, err := query.Source()
		if err != nil {
			return nil, err
		}
		sources = append(sources, src)
	}
	return sources, nil
}
