package elasticsearch

import (
	"fmt"

	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client/query"
)

// This is a wrapper for query.BoolQuery so we can access the clauses and be able to combine
// queries and avoid nesting queries when possible.
type boolQuery struct {
	mustNotClauses     []query.Query
	filterClauses      []query.Query
	shouldClauses      []query.Query
	minimumShouldMatch string
}

var _ query.Query = (*boolQuery)(nil)

func newBoolQuery() *boolQuery {
	return &boolQuery{}
}

func (q *boolQuery) MustNot(queries ...query.Query) *boolQuery {
	q.mustNotClauses = append(q.mustNotClauses, queries...)
	return q
}

func (q *boolQuery) Filter(filters ...query.Query) *boolQuery {
	q.filterClauses = append(q.filterClauses, filters...)
	return q
}

func (q *boolQuery) Should(queries ...query.Query) *boolQuery {
	q.shouldClauses = append(q.shouldClauses, queries...)
	return q
}

func (q *boolQuery) MinimumNumberShouldMatch(minimumNumberShouldMatch int) *boolQuery {
	q.minimumShouldMatch = fmt.Sprintf("%d", minimumNumberShouldMatch)
	return q
}

func (q *boolQuery) Source() (map[string]any, error) {
	return query.NewBoolQuery().
		MustNot(q.mustNotClauses...).
		Filter(q.filterClauses...).
		Should(q.shouldClauses...).
		MinimumShouldMatch(q.minimumShouldMatch).
		Source()
}
