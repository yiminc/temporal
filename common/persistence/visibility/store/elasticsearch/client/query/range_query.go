package query

// RangeQuery matches documents with fields that have terms within a certain range.
type RangeQuery struct {
	name   string
	from   any
	to     any
	gt     any
	gte    any
	lt     any
	lte    any
	boost  *float64
	format string
}

var _ Query = (*RangeQuery)(nil)

// NewRangeQuery creates a new RangeQuery.
func NewRangeQuery(name string) *RangeQuery {
	return &RangeQuery{name: name}
}

// Gt sets the greater-than value.
func (q *RangeQuery) Gt(val any) *RangeQuery {
	q.gt = val
	return q
}

// Gte sets the greater-than-or-equal value.
func (q *RangeQuery) Gte(val any) *RangeQuery {
	q.gte = val
	return q
}

// Lt sets the less-than value.
func (q *RangeQuery) Lt(val any) *RangeQuery {
	q.lt = val
	return q
}

// Lte sets the less-than-or-equal value.
func (q *RangeQuery) Lte(val any) *RangeQuery {
	q.lte = val
	return q
}

// From sets the from value for the range.
func (q *RangeQuery) From(val any) *RangeQuery {
	q.from = val
	return q
}

// To sets the to value for the range.
func (q *RangeQuery) To(val any) *RangeQuery {
	q.to = val
	return q
}

// Boost sets the boost for this query.
func (q *RangeQuery) Boost(boost float64) *RangeQuery {
	q.boost = &boost
	return q
}

// Format sets the format for date fields.
func (q *RangeQuery) Format(format string) *RangeQuery {
	q.format = format
	return q
}

// Source returns the query as a map for JSON serialization.
func (q *RangeQuery) Source() (map[string]any, error) {
	rangeParams := make(map[string]any)

	if q.gt != nil {
		rangeParams["gt"] = q.gt
	}
	if q.gte != nil {
		rangeParams["gte"] = q.gte
	}
	if q.lt != nil {
		rangeParams["lt"] = q.lt
	}
	if q.lte != nil {
		rangeParams["lte"] = q.lte
	}
	if q.from != nil {
		rangeParams["from"] = q.from
	}
	if q.to != nil {
		rangeParams["to"] = q.to
	}
	if q.boost != nil {
		rangeParams["boost"] = *q.boost
	}
	if q.format != "" {
		rangeParams["format"] = q.format
	}

	return map[string]any{
		"range": map[string]any{
			q.name: rangeParams,
		},
	}, nil
}
