package elasticsearch

import (
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client/query"
	storequery "go.temporal.io/server/common/persistence/visibility/store/query"
)

type queryConverter struct{}

var _ storequery.StoreQueryConverter[query.Query] = (*queryConverter)(nil)

func (c *queryConverter) GetDatetimeFormat() string {
	return time.RFC3339Nano
}

func (c *queryConverter) BuildParenExpr(expr query.Query) (query.Query, error) {
	if expr == nil {
		return nil, nil
	}
	return expr, nil
}

func (c *queryConverter) BuildNotExpr(expr query.Query) (query.Query, error) {
	if expr == nil {
		return nil, nil
	}
	if bq, ok := expr.(*boolQuery); ok && len(bq.shouldClauses) > 0 {
		// !(a || b) == !a && !b
		ret := newBoolQuery()
		ret.mustNotClauses = bq.shouldClauses
		return ret, nil
	}
	return newBoolQuery().MustNot(expr), nil
}

func (c *queryConverter) BuildAndExpr(exprs ...query.Query) (query.Query, error) {
	var reusableBoolQuery *boolQuery
	validExprs := make([]query.Query, 0, len(exprs))
	for _, e := range exprs {
		if e == nil {
			continue
		}
		if bq, ok := e.(*boolQuery); !ok || len(bq.filterClauses)+len(bq.mustNotClauses) == 0 {
			validExprs = append(validExprs, e)
		} else if reusableBoolQuery == nil {
			reusableBoolQuery = bq
		} else {
			reusableBoolQuery.Filter(bq.filterClauses...).MustNot(bq.mustNotClauses...)
		}
	}
	if reusableBoolQuery != nil {
		reusableBoolQuery.Filter(validExprs...)
		return reusableBoolQuery, nil
	}
	if len(validExprs) == 0 {
		return nil, nil
	}
	if len(validExprs) == 1 {
		return validExprs[0], nil
	}
	return newBoolQuery().Filter(validExprs...), nil
}

func (c *queryConverter) BuildOrExpr(exprs ...query.Query) (query.Query, error) {
	var reusableBoolQuery *boolQuery
	validExprs := make([]query.Query, 0, len(exprs))
	for _, e := range exprs {
		if e == nil {
			continue
		}
		if bq, ok := e.(*boolQuery); !ok || len(bq.shouldClauses) == 0 {
			validExprs = append(validExprs, e)
		} else if reusableBoolQuery == nil {
			reusableBoolQuery = bq
		} else {
			reusableBoolQuery.Should(bq.shouldClauses...)
		}
	}
	if reusableBoolQuery != nil {
		reusableBoolQuery.Should(validExprs...)
		return reusableBoolQuery, nil
	}
	if len(validExprs) == 0 {
		return nil, nil
	}
	if len(validExprs) == 1 {
		return validExprs[0], nil
	}
	return newBoolQuery().Should(validExprs...).MinimumNumberShouldMatch(1), nil
}

func (c *queryConverter) ConvertComparisonExpr(
	operator string,
	col *storequery.SAColumn,
	value any,
) (query.Query, error) {
	var res query.Query
	negate := false
	colName := col.FieldName
	switch operator {
	case sqlparser.GreaterEqualStr:
		res = query.NewRangeQuery(colName).Gte(value)
	case sqlparser.LessEqualStr:
		res = query.NewRangeQuery(colName).Lte(value)
	case sqlparser.GreaterThanStr:
		res = query.NewRangeQuery(colName).Gt(value)
	case sqlparser.LessThanStr:
		res = query.NewRangeQuery(colName).Lt(value)
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		res = query.NewTermQuery(colName, value)
		negate = operator == sqlparser.NotEqualStr
	case sqlparser.InStr, sqlparser.NotInStr:
		res = query.NewTermsQuery(colName, value.([]any)...)
		negate = operator == sqlparser.NotInStr
	default:
		return nil, storequery.NewOperatorNotSupportedError(col.Alias, col.ValueType, operator)
	}

	if negate {
		res, _ = c.BuildNotExpr(res)
	}
	return res, nil
}

func (c *queryConverter) ConvertKeywordComparisonExpr(
	operator string,
	col *storequery.SAColumn,
	value any,
) (query.Query, error) {
	colName := col.FieldName
	switch operator {
	case sqlparser.StartsWithStr, sqlparser.NotStartsWithStr:
		v, ok := value.(string)
		if !ok {
			return nil, storequery.NewConverterError(
				"%s: right-hand side of operator '%s' must be a string",
				storequery.InvalidExpressionErrMessage,
				strings.ToUpper(operator),
			)
		}
		var res query.Query = query.NewPrefixQuery(colName, v)
		if operator == sqlparser.NotStartsWithStr {
			res, _ = c.BuildNotExpr(res)
		}
		return res, nil
	default:
		return c.ConvertComparisonExpr(operator, col, value)
	}
}

func (c *queryConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *storequery.SAColumn,
	value any,
) (query.Query, error) {
	return c.ConvertKeywordComparisonExpr(operator, col, value)
}

func (c *queryConverter) ConvertTextComparisonExpr(
	operator string,
	col *storequery.SAColumn,
	value any,
) (query.Query, error) {
	colName := col.FieldName
	switch operator {
	case sqlparser.EqualStr:
		return query.NewMatchQuery(colName, value), nil
	case sqlparser.NotEqualStr:
		return newBoolQuery().MustNot(query.NewMatchQuery(colName, value)), nil
	default:
		return nil, storequery.NewOperatorNotSupportedError(col.Alias, col.ValueType, operator)
	}
}

func (c *queryConverter) ConvertRangeExpr(
	operator string,
	col *storequery.SAColumn,
	from, to any,
) (query.Query, error) {
	colName := col.FieldName
	switch operator {
	case sqlparser.BetweenStr:
		return query.NewRangeQuery(colName).Gte(from).Lte(to), nil
	case sqlparser.NotBetweenStr:
		return newBoolQuery().MustNot(query.NewRangeQuery(colName).Gte(from).Lte(to)), nil
	default:
		// This should be impossible since the query parser only calls this function with one of those
		// operators strings.
		return nil, storequery.NewConverterError(
			"%s: unexpected operator '%s' for range condition",
			storequery.MalformedSqlQueryErrMessage,
			strings.ToUpper(operator),
		)
	}
}

func (c *queryConverter) ConvertIsExpr(
	operator string,
	col *storequery.SAColumn,
) (query.Query, error) {
	colName := col.FieldName
	switch operator {
	case sqlparser.IsNullStr:
		return newBoolQuery().MustNot(query.NewExistsQuery(colName)), nil
	case sqlparser.IsNotNullStr:
		return query.NewExistsQuery(colName), nil
	default:
		// This should be impossible since the query parser only calls this function with one of those
		// operators strings.
		return nil, storequery.NewConverterError(
			"%s: 'IS' operator can only be used as 'IS NULL' or 'IS NOT NULL'",
			storequery.InvalidExpressionErrMessage,
		)
	}
}
