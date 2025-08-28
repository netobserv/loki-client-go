package grpc

import (
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClientNewDefaultConfig(t *testing.T) {
	serverAddr := "localhost:9095"

	cfg, err := NewDefaultConfig(serverAddr)
	require.NoError(t, err)
	assert.Equal(t, serverAddr, cfg.ServerAddress)
}

func TestNewWithInvalidAddress(t *testing.T) {
	cfg := Config{
		ServerAddress: "", // Invalid - empty address
		BatchWait:     DefaultBatchWait,
		BatchSize:     DefaultBatchSize,
		Timeout:       DefaultTimeout,
	}

	logger := log.NewNopLogger()
	_, err := NewWithLogger(cfg, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server address")
}

func TestGetTenantID(t *testing.T) {
	cfg := Config{
		ServerAddress: "localhost:9095",
		TenantID:      "config-tenant",
		BatchWait:     DefaultBatchWait,
		BatchSize:     DefaultBatchSize,
		Timeout:       DefaultTimeout,
	}

	client := &Client{
		cfg: cfg,
	}

	// Test with no tenant override
	labels := model.LabelSet{"job": "test"}
	tenantID := client.getTenantID(labels)
	assert.Equal(t, "config-tenant", tenantID)

	// Test with tenant override in labels
	labelsWithOverride := model.LabelSet{
		"job":                 "test",
		ReservedLabelTenantID: "override-tenant",
	}
	tenantID = client.getTenantID(labelsWithOverride)
	assert.Equal(t, "override-tenant", tenantID)

	// Test with no config tenant and no override
	client.cfg.TenantID = ""
	tenantID = client.getTenantID(labels)
	assert.Equal(t, "", tenantID)
}

func TestGetStatusCode(t *testing.T) {
	client := &Client{}

	// Test nil error (success) - now returns HTTP-compatible status codes
	assert.Equal(t, "200", client.getStatusCode(nil))

	// Test GRPC errors mapped to HTTP-compatible status codes
	err := status.Error(codes.InvalidArgument, "test error")
	assert.Equal(t, "3", client.getStatusCode(err)) // InvalidArgument maps to code 3

	// Test specific mapped codes
	err = status.Error(codes.Unavailable, "service unavailable")
	assert.Equal(t, "500", client.getStatusCode(err)) // Unavailable maps to 500

	err = status.Error(codes.ResourceExhausted, "rate limited")
	assert.Equal(t, "429", client.getStatusCode(err)) // ResourceExhausted maps to 429

	err = status.Error(codes.DeadlineExceeded, "timeout")
	assert.Equal(t, "504", client.getStatusCode(err)) // DeadlineExceeded maps to 504

	err = status.Error(codes.Internal, "internal error")
	assert.Equal(t, "500", client.getStatusCode(err)) // Internal maps to 500

	// Test non-GRPC error
	nonGrpcErr := assert.AnError
	assert.Equal(t, "Unknown", client.getStatusCode(nonGrpcErr))
}

func TestClientHandle(t *testing.T) {
	// Create a client without actually connecting (for unit test)
	cfg := Config{
		ServerAddress: "localhost:9095",
		BatchWait:     100 * time.Millisecond,
		BatchSize:     1024,
		Timeout:       time.Second,
	}

	client := &Client{
		cfg:            cfg,
		entries:        make(chan entry, 10),
		quit:           make(chan struct{}),
		externalLabels: model.LabelSet{"external": "label"},
	}

	// Test handling an entry
	labels := model.LabelSet{"job": "test"}
	timestamp := time.Now()
	line := "test log line"

	err := client.Handle(labels, timestamp, line)
	assert.NoError(t, err)

	// Check that entry was added to channel
	select {
	case entry := <-client.entries:
		assert.Equal(t, "", entry.tenantID) // No tenant ID configured
		expectedLabels := model.LabelSet{"job": "test", "external": "label"}
		assert.Equal(t, expectedLabels, entry.labels)
		assert.Equal(t, timestamp, entry.Timestamp)
		assert.Equal(t, line, entry.Line)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Entry was not received within timeout")
	}
}

func TestClientHandleWithTenantOverride(t *testing.T) {
	cfg := Config{
		ServerAddress: "localhost:9095",
		TenantID:      "default-tenant",
		BatchWait:     100 * time.Millisecond,
		BatchSize:     1024,
		Timeout:       time.Second,
	}

	client := &Client{
		cfg:     cfg,
		entries: make(chan entry, 10),
		quit:    make(chan struct{}),
	}

	// Test with tenant override
	labels := model.LabelSet{
		"job":                 "test",
		ReservedLabelTenantID: "override-tenant",
	}
	timestamp := time.Now()
	line := "test log line"

	err := client.Handle(labels, timestamp, line)
	assert.NoError(t, err)

	// Check entry
	select {
	case entry := <-client.entries:
		assert.Equal(t, "override-tenant", entry.tenantID)
		// The reserved label should be removed from the final labels
		_, hasReservedLabel := entry.labels[ReservedLabelTenantID]
		assert.False(t, hasReservedLabel)
		assert.Equal(t, model.LabelSet{"job": "test"}, entry.labels)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Entry was not received within timeout")
	}
}
