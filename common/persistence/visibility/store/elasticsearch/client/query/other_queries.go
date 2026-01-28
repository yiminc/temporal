package query

// ExistsQuery finds documents that have at least one non-null value in the specified field.
type ExistsQuery struct {
	name string
}

var _ Query = (*ExistsQuery)(nil)

// NewExistsQuery creates a new ExistsQuery.
func NewExistsQuery(name string) *ExistsQuery {
	return &ExistsQuery{name: name}
}

// Source returns the query as a map for JSON serialization.
func (q *ExistsQuery) Source() (map[string]any, error) {
	return map[string]any{
		"exists": map[string]any{
			"field": q.name,
		},
	}, nil
}

// MatchQuery is a full-text query that matches documents using analysis.
type MatchQuery struct {
	name     string
	text     any
	operator string
	boost    *float64
}

var _ Query = (*MatchQuery)(nil)

// NewMatchQuery creates a new MatchQuery.
func NewMatchQuery(name string, text any) *MatchQuery {
	return &MatchQuery{
		name: name,
		text: text,
	}
}

// Operator sets the boolean operator (AND or OR).
func (q *MatchQuery) Operator(operator string) *MatchQuery {
	q.operator = operator
	return q
}

// Boost sets the boost for this query.
func (q *MatchQuery) Boost(boost float64) *MatchQuery {
	q.boost = &boost
	return q
}

// Source returns the query as a map for JSON serialization.
func (q *MatchQuery) Source() (map[string]any, error) {
	matchParams := make(map[string]any)
	matchParams["query"] = q.text

	if q.operator != "" {
		matchParams["operator"] = q.operator
	}
	if q.boost != nil {
		matchParams["boost"] = *q.boost
	}

	// If only query is set, use simple form
	if q.operator == "" && q.boost == nil {
		return map[string]any{
			"match": map[string]any{
				q.name: q.text,
			},
		}, nil
	}

	return map[string]any{
		"match": map[string]any{
			q.name: matchParams,
		},
	}, nil
}

// PrefixQuery matches documents that contain terms with a specified prefix.
type PrefixQuery struct {
	name  string
	value string
	boost *float64
}

var _ Query = (*PrefixQuery)(nil)

// NewPrefixQuery creates a new PrefixQuery.
func NewPrefixQuery(name string, value string) *PrefixQuery {
	return &PrefixQuery{
		name:  name,
		value: value,
	}
}

// Boost sets the boost for this query.
func (q *PrefixQuery) Boost(boost float64) *PrefixQuery {
	q.boost = &boost
	return q
}

// Source returns the query as a map for JSON serialization.
func (q *PrefixQuery) Source() (map[string]any, error) {
	if q.boost == nil {
		return map[string]any{
			"prefix": map[string]any{
				q.name: q.value,
			},
		}, nil
	}
	return map[string]any{
		"prefix": map[string]any{
			q.name: map[string]any{
				"value": q.value,
				"boost": *q.boost,
			},
		},
	}, nil
}

// MatchAllQuery matches all documents.
type MatchAllQuery struct {
	boost *float64
}

var _ Query = (*MatchAllQuery)(nil)

// NewMatchAllQuery creates a new MatchAllQuery.
func NewMatchAllQuery() *MatchAllQuery {
	return &MatchAllQuery{}
}

// Boost sets the boost for this query.
func (q *MatchAllQuery) Boost(boost float64) *MatchAllQuery {
	q.boost = &boost
	return q
}

// Source returns the query as a map for JSON serialization.
func (q *MatchAllQuery) Source() (map[string]any, error) {
	if q.boost == nil {
		return map[string]any{
			"match_all": map[string]any{},
		}, nil
	}
	return map[string]any{
		"match_all": map[string]any{
			"boost": *q.boost,
		},
	}, nil
}
