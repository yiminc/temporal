package query

// TermsAggregation is a multi-bucket value source based aggregation
// where buckets are dynamically built - one per unique value.
type TermsAggregation struct {
	field           string
	size            *int
	subAggregations map[string]Aggregation
}

var _ Aggregation = (*TermsAggregation)(nil)

// NewTermsAggregation creates a new TermsAggregation.
func NewTermsAggregation() *TermsAggregation {
	return &TermsAggregation{
		subAggregations: make(map[string]Aggregation),
	}
}

// Field sets the field to aggregate on.
func (a *TermsAggregation) Field(field string) *TermsAggregation {
	a.field = field
	return a
}

// Size sets the number of term buckets to return.
func (a *TermsAggregation) Size(size int) *TermsAggregation {
	a.size = &size
	return a
}

// SubAggregation adds a sub-aggregation.
func (a *TermsAggregation) SubAggregation(name string, subAgg Aggregation) *TermsAggregation {
	a.subAggregations[name] = subAgg
	return a
}

// Source returns the aggregation as a map for JSON serialization.
func (a *TermsAggregation) Source() (map[string]any, error) {
	terms := make(map[string]any)
	if a.field != "" {
		terms["field"] = a.field
	}
	if a.size != nil {
		terms["size"] = *a.size
	}

	source := map[string]any{
		"terms": terms,
	}

	if len(a.subAggregations) > 0 {
		aggs := make(map[string]any)
		for name, subAgg := range a.subAggregations {
			src, err := subAgg.Source()
			if err != nil {
				return nil, err
			}
			aggs[name] = src
		}
		source["aggs"] = aggs
	}

	return source, nil
}
