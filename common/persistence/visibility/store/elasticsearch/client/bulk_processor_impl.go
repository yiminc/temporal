package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	bulkProcessorStopTimeout = 5 * time.Second
)

// bulkProcessorImpl is an internal bulk processor that batches requests
// and sends them to Elasticsearch in bulk.
type bulkProcessorImpl struct {
	httpClient    *http.Client
	baseURL       string
	params        *BulkProcessorParameters
	executionID   atomic.Int64
	requests      []*BulkableRequest
	requestsMu    sync.Mutex
	flushTicker   *time.Ticker
	stopChan      chan struct{}
	stoppedChan   chan struct{}
	workerWG      sync.WaitGroup
	requestsChan  chan *BulkableRequest
	flushing      atomic.Bool
}

// newBulkProcessorImpl creates a new bulk processor.
func newBulkProcessorImpl(
	httpClient *http.Client,
	baseURL string,
	params *BulkProcessorParameters,
) *bulkProcessorImpl {
	p := &bulkProcessorImpl{
		httpClient:   httpClient,
		baseURL:      baseURL,
		params:       params,
		requests:     make([]*BulkableRequest, 0, params.BulkActions),
		stopChan:     make(chan struct{}),
		stoppedChan:  make(chan struct{}),
		requestsChan: make(chan *BulkableRequest, params.BulkActions*params.NumOfWorkers),
	}

	if params.FlushInterval > 0 {
		p.flushTicker = time.NewTicker(params.FlushInterval)
	}

	// Start the worker goroutines
	for i := 0; i < params.NumOfWorkers; i++ {
		p.workerWG.Add(1)
		go p.worker()
	}

	// Start the flusher goroutine
	go p.flusher()

	return p
}

// Add adds a request to the bulk processor.
func (p *bulkProcessorImpl) Add(request *BulkableRequest) {
	select {
	case <-p.stopChan:
		return
	case p.requestsChan <- request:
	}
}

// Stop stops the bulk processor and flushes remaining requests.
func (p *bulkProcessorImpl) Stop() error {
	// Signal stop
	close(p.stopChan)

	// Wait with timeout
	timer := time.NewTimer(bulkProcessorStopTimeout)
	defer timer.Stop()

	select {
	case <-p.stoppedChan:
		return nil
	case <-timer.C:
		return errors.New("bulk processor Stop timed out")
	}
}

// worker processes incoming requests.
func (p *bulkProcessorImpl) worker() {
	defer p.workerWG.Done()

	for {
		select {
		case <-p.stopChan:
			return
		case req, ok := <-p.requestsChan:
			if !ok {
				return
			}
			p.addRequest(req)
		}
	}
}

// flusher handles periodic flushing and shutdown.
func (p *bulkProcessorImpl) flusher() {
	defer close(p.stoppedChan)

	var tickerChan <-chan time.Time
	if p.flushTicker != nil {
		tickerChan = p.flushTicker.C
	}

	for {
		select {
		case <-p.stopChan:
			// Stop the ticker
			if p.flushTicker != nil {
				p.flushTicker.Stop()
			}
			// Close the requests channel and wait for workers
			close(p.requestsChan)
			p.workerWG.Wait()
			// Final flush
			p.flush()
			return
		case <-tickerChan:
			p.flush()
		}
	}
}

// addRequest adds a request to the buffer and flushes if needed.
func (p *bulkProcessorImpl) addRequest(req *BulkableRequest) {
	p.requestsMu.Lock()
	p.requests = append(p.requests, req)
	shouldFlush := len(p.requests) >= p.params.BulkActions
	p.requestsMu.Unlock()

	if shouldFlush {
		p.flush()
	}
}

// flush sends all buffered requests to Elasticsearch.
func (p *bulkProcessorImpl) flush() {
	// Prevent concurrent flushes
	if !p.flushing.CompareAndSwap(false, true) {
		return
	}
	defer p.flushing.Store(false)

	p.requestsMu.Lock()
	if len(p.requests) == 0 {
		p.requestsMu.Unlock()
		return
	}
	requests := p.requests
	p.requests = make([]*BulkableRequest, 0, p.params.BulkActions)
	p.requestsMu.Unlock()

	execID := p.executionID.Add(1)

	// Call before callback
	if p.params.BeforeFunc != nil {
		p.params.BeforeFunc(execID, requests)
	}

	// Execute the bulk request
	response, err := p.executeBulk(context.Background(), requests)

	// Call after callback
	if p.params.AfterFunc != nil {
		p.params.AfterFunc(execID, requests, response, err)
	}
}

// executeBulk sends a bulk request to Elasticsearch.
func (p *bulkProcessorImpl) executeBulk(ctx context.Context, requests []*BulkableRequest) (*BulkResponse, error) {
	if len(requests) == 0 {
		return &BulkResponse{}, nil
	}

	// Build the bulk request body
	var buf bytes.Buffer
	for _, req := range requests {
		if err := p.writeBulkAction(&buf, req); err != nil {
			return nil, fmt.Errorf("failed to write bulk action: %w", err)
		}
	}

	// Send the request
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/_bulk", &buf)
	if err != nil {
		return nil, fmt.Errorf("failed to create bulk request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to execute bulk request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read bulk response: %w", err)
	}

	if resp.StatusCode >= 300 {
		return nil, &ESError{
			Status:  resp.StatusCode,
			Message: string(body),
		}
	}

	// Parse the response
	var bulkResp BulkResponse
	if err := json.Unmarshal(body, &bulkResp); err != nil {
		return nil, fmt.Errorf("failed to parse bulk response: %w", err)
	}

	return &bulkResp, nil
}

// writeBulkAction writes a single bulk action to the buffer.
func (p *bulkProcessorImpl) writeBulkAction(buf *bytes.Buffer, req *BulkableRequest) error {
	var action string
	switch req.RequestType {
	case BulkableRequestTypeIndex:
		action = "index"
	case BulkableRequestTypeDelete:
		action = "delete"
	default:
		return fmt.Errorf("unknown request type: %v", req.RequestType)
	}

	// Write action line
	meta := map[string]any{
		action: map[string]any{
			"_index":        req.Index,
			"_id":           req.ID,
			"version":       req.Version,
			"version_type":  versionTypeExternal,
		},
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	buf.Write(metaBytes)
	buf.WriteByte('\n')

	// Write document (only for index requests)
	if req.RequestType == BulkableRequestTypeIndex && req.Doc != nil {
		docBytes, err := json.Marshal(req.Doc)
		if err != nil {
			return err
		}
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	return nil
}

// ESError represents an Elasticsearch error response.
type ESError struct {
	Status  int
	Message string
	Details *ErrorDetails
}

func (e *ESError) Error() string {
	statusText := http.StatusText(e.Status)
	if statusText == "" {
		statusText = "Unknown Status"
	}
	if e.Details != nil && e.Details.Reason != "" {
		return fmt.Sprintf("elastic: Error %d (%s): %s [type=%s]", e.Status, statusText, e.Details.Reason, e.Details.Type)
	}
	return fmt.Sprintf("elastic: Error %d (%s)", e.Status, statusText)
}
