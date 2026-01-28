package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	// AWS service name for Elasticsearch/OpenSearch
	awsServiceES = "es"
)

// awsSigningTransport is an http.RoundTripper that signs requests with AWS Signature V4
type awsSigningTransport struct {
	credentials aws.CredentialsProvider
	region      string
	signer      *v4.Signer
	base        http.RoundTripper
}

func (t *awsSigningTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Get credentials
	creds, err := t.credentials.Retrieve(req.Context())
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve AWS credentials: %w", err)
	}

	// Read and buffer the body if present
	var bodyBytes []byte
	if req.Body != nil {
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body: %w", err)
		}
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	// Create the payload hash
	var payloadHash string
	if len(bodyBytes) > 0 {
		payloadHash = v4.GetPayloadHash(req.Context())
	}

	// Sign the request
	err = t.signer.SignHTTP(req.Context(), creds, req, payloadHash, awsServiceES, t.region, time.Now())
	if err != nil {
		return nil, fmt.Errorf("failed to sign request: %w", err)
	}

	// Reset the body for the actual request
	if len(bodyBytes) > 0 {
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	return t.base.RoundTrip(req)
}

// NewAwsHttpClient creates an HTTP client that signs requests with AWS Signature V4.
func NewAwsHttpClient(cfg ESAWSRequestSigningConfig) (*http.Client, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	region := cfg.Region
	if region == "" {
		region = os.Getenv("AWS_REGION")
		if region == "" {
			return nil, fmt.Errorf("unable to resolve AWS region for obtaining AWS Elastic signing credentials")
		}
	}

	var credProvider aws.CredentialsProvider

	switch strings.ToLower(cfg.CredentialProvider) {
	case "static":
		credProvider = credentials.NewStaticCredentialsProvider(
			cfg.Static.AccessKeyID,
			cfg.Static.SecretAccessKey,
			cfg.Static.Token,
		)
	case "environment":
		credProvider = aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider(
				os.Getenv("AWS_ACCESS_KEY_ID"),
				os.Getenv("AWS_SECRET_ACCESS_KEY"),
				os.Getenv("AWS_SESSION_TOKEN"),
			),
		)
	case "aws-sdk-default":
		awsCfg, err := config.LoadDefaultConfig(context.Background(),
			config.WithRegion(region),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config: %w", err)
		}
		credProvider = awsCfg.Credentials
	default:
		return nil, fmt.Errorf("unknown AWS credential provider specified: %+v. Accepted options are 'static', 'environment' or 'aws-sdk-default'", cfg.CredentialProvider)
	}

	transport := &awsSigningTransport{
		credentials: credProvider,
		region:      region,
		signer:      v4.NewSigner(),
		base:        http.DefaultTransport,
	}

	return &http.Client{Transport: transport}, nil
}
