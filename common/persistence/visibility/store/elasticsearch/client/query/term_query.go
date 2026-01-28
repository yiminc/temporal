package query

// TermQuery finds documents that contain the exact term specified in the inverted index.
type TermQuery struct {
	name  string
	value any
	boost *float64
}

var _ Query = (*TermQuery)(nil)

// NewTermQuery creates a new TermQuery.
func NewTermQuery(name string, value any) *TermQuery {
	return &TermQuery{
		name:  name,
		value: value,
	}
}

// Boost sets the boost for this query.
func (q *TermQuery) Boost(boost float64) *TermQuery {
	q.boost = &boost
	return q
}

// Source returns the query as a map for JSON serialization.
func (q *TermQuery) Source() (map[string]any, error) {
	if q.boost == nil {
		return map[string]any{
			"term": map[string]any{
				q.name: q.value,
			},
		}, nil
	}
	return map[string]any{
		"term": map[string]any{
			q.name: map[string]any{
				"value": q.value,
				"boost": *q.boost,
			},
		},
	}, nil
}

// TermsQuery filters documents that have fields matching any of the provided terms.
type TermsQuery struct {
	name   string
	values []any
	boost  *float64
}

var _ Query = (*TermsQuery)(nil)

// NewTermsQuery creates a new TermsQuery.
func NewTermsQuery(name string, values ...any) *TermsQuery {
	return &TermsQuery{
		name:   name,
		values: values,
	}
}

// Boost sets the boost for this query.
func (q *TermsQuery) Boost(boost float64) *TermsQuery {
	q.boost = &boost
	return q
}

// Source returns the query as a map for JSON serialization.
func (q *TermsQuery) Source() (map[string]any, error) {
	source := map[string]any{
		q.name: q.values,
	}
	if q.boost != nil {
		source["boost"] = *q.boost
	}
	return map[string]any{
		"terms": source,
	}, nil
}
