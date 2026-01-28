package query

// FieldSort sorts by a specific field.
type FieldSort struct {
	fieldName string
	ascending bool
	missing   any // "_first", "_last", or a custom value
}

var _ Sorter = (*FieldSort)(nil)

// NewFieldSort creates a new FieldSort for the given field.
// By default, sorts in ascending order.
func NewFieldSort(fieldName string) *FieldSort {
	return &FieldSort{
		fieldName: fieldName,
		ascending: true,
	}
}

// Asc sets the sort order to ascending.
func (s *FieldSort) Asc() *FieldSort {
	s.ascending = true
	return s
}

// Desc sets the sort order to descending.
func (s *FieldSort) Desc() *FieldSort {
	s.ascending = false
	return s
}

// Missing sets how to handle missing values: "_first", "_last", or a custom value.
func (s *FieldSort) Missing(missing any) *FieldSort {
	s.missing = missing
	return s
}

// FieldName returns the field name.
func (s *FieldSort) FieldName() string {
	return s.fieldName
}

// Ascending returns whether the sort is ascending.
func (s *FieldSort) Ascending() bool {
	return s.ascending
}

// Source returns the sorter as a map for JSON serialization.
func (s *FieldSort) Source() (map[string]any, error) {
	sortParams := make(map[string]any)

	if s.ascending {
		sortParams["order"] = "asc"
	} else {
		sortParams["order"] = "desc"
	}

	if s.missing != nil {
		sortParams["missing"] = s.missing
	}

	return map[string]any{
		s.fieldName: sortParams,
	}, nil
}

// DocSort sorts by document index order (_doc).
type DocSort struct{}

var _ Sorter = (*DocSort)(nil)

// NewDocSort creates a sort by _doc (internal document order).
func NewDocSort() *DocSort {
	return &DocSort{}
}

// Source returns the sorter as a map for JSON serialization.
func (s *DocSort) Source() (map[string]any, error) {
	return map[string]any{
		"_doc": map[string]any{},
	}, nil
}
