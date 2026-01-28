//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination bulk_processor_mock.go

package client

import (
	"time"
)

type BulkableRequestType uint8

const (
	BulkableRequestTypeIndex BulkableRequestType = iota
	BulkableRequestTypeDelete
)

type (
	BulkProcessor interface {
		Stop() error
		Add(request *BulkableRequest)
	}

	// BulkBeforeFunc defines the signature of a callback that is called
	// before a commit to Elasticsearch.
	BulkBeforeFunc func(executionID int64, requests []*BulkableRequest)

	// BulkAfterFunc defines the signature of a callback that is called
	// after a commit to Elasticsearch. The err parameter is only set when
	// the entire bulk request failed (e.g., network issues).
	BulkAfterFunc func(executionID int64, requests []*BulkableRequest, response *BulkResponse, err error)

	// BulkProcessorParameters holds all required and optional parameters for executing bulk service
	BulkProcessorParameters struct {
		Name          string
		NumOfWorkers  int
		BulkActions   int
		BulkSize      int
		FlushInterval time.Duration
		BeforeFunc    BulkBeforeFunc
		AfterFunc     BulkAfterFunc
	}

	BulkableRequest struct {
		RequestType BulkableRequestType
		Index       string
		ID          string
		Version     int64
		Doc         map[string]interface{}
	}
)
