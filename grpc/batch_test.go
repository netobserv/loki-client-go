package grpc

import (
	"testing"
	"time"

	"github.com/netobserv/loki-client-go/pkg/logproto"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBatch(t *testing.T) {
	tenantID := "test-tenant"
	
	// Test empty batch
	b := newBatch(tenantID)
	assert.Equal(t, tenantID, b.tenantID)
	assert.Equal(t, 0, b.bytes)
	assert.True(t, b.isEmpty())
	assert.Equal(t, 0, b.streamCount())
	assert.Equal(t, 0, b.entryCount())
	
	// Test batch with initial entries
	entry1 := entry{
		tenantID: tenantID,
		labels:   model.LabelSet{"job": "test"},
		Entry: logproto.Entry{
			Timestamp: time.Now(),
			Line:      "test log line 1",
		},
	}
	
	entry2 := entry{
		tenantID: tenantID,
		labels:   model.LabelSet{"job": "test"},
		Entry: logproto.Entry{
			Timestamp: time.Now(),
			Line:      "test log line 2",
		},
	}
	
	b2 := newBatch(tenantID, entry1, entry2)
	assert.Equal(t, tenantID, b2.tenantID)
	assert.Equal(t, len("test log line 1")+len("test log line 2"), b2.bytes)
	assert.False(t, b2.isEmpty())
	assert.Equal(t, 1, b2.streamCount()) // Same labels, so same stream
	assert.Equal(t, 2, b2.entryCount())
}

func TestBatchAdd(t *testing.T) {
	tenantID := "test-tenant"
	b := newBatch(tenantID)
	
	entry := entry{
		tenantID: tenantID,
		labels:   model.LabelSet{"job": "test", "instance": "localhost"},
		Entry: logproto.Entry{
			Timestamp: time.Now(),
			Line:      "test log line",
		},
	}
	
	// Add first entry
	b.add(entry)
	assert.Equal(t, len("test log line"), b.bytes)
	assert.Equal(t, 1, b.streamCount())
	assert.Equal(t, 1, b.entryCount())
	
	// Add entry with same labels (should go to same stream)
	entry2 := entry
	entry2.Line = "another line"
	b.add(entry2)
	assert.Equal(t, len("test log line")+len("another line"), b.bytes)
	assert.Equal(t, 1, b.streamCount())
	assert.Equal(t, 2, b.entryCount())
	
	// Add entry with different labels (should create new stream)
	entry3 := entry
	entry3.labels = model.LabelSet{"job": "different"}
	entry3.Line = "different stream"
	b.add(entry3)
	assert.Equal(t, len("test log line")+len("another line")+len("different stream"), b.bytes)
	assert.Equal(t, 2, b.streamCount())
	assert.Equal(t, 3, b.entryCount())
}

func TestBatchSizeBytes(t *testing.T) {
	tenantID := "test-tenant"
	b := newBatch(tenantID)
	
	entry := entry{
		tenantID: tenantID,
		labels:   model.LabelSet{"job": "test"},
		Entry: logproto.Entry{
			Timestamp: time.Now(),
			Line:      "test",
		},
	}
	
	assert.Equal(t, 0, b.sizeBytes())
	
	expectedSize := len("test")
	assert.Equal(t, expectedSize, b.sizeBytesAfter(entry))
	
	b.add(entry)
	assert.Equal(t, expectedSize, b.sizeBytes())
}

func TestBatchAge(t *testing.T) {
	tenantID := "test-tenant"
	b := newBatch(tenantID)
	
	// Should be very recent
	age := b.age()
	assert.True(t, age < 100*time.Millisecond)
	
	// Wait a bit and check again
	time.Sleep(10 * time.Millisecond)
	age2 := b.age()
	assert.True(t, age2 > age)
}

func TestCreatePushRequest(t *testing.T) {
	tenantID := "test-tenant"
	timestamp := time.Now()
	
	entry1 := entry{
		tenantID: tenantID,
		labels:   model.LabelSet{"job": "test1"},
		Entry: logproto.Entry{
			Timestamp: timestamp,
			Line:      "line 1",
		},
	}
	
	entry2 := entry{
		tenantID: tenantID,
		labels:   model.LabelSet{"job": "test1"},
		Entry: logproto.Entry{
			Timestamp: timestamp.Add(time.Second),
			Line:      "line 2",
		},
	}
	
	entry3 := entry{
		tenantID: tenantID,
		labels:   model.LabelSet{"job": "test2"},
		Entry: logproto.Entry{
			Timestamp: timestamp.Add(2 * time.Second),
			Line:      "line 3",
		},
	}
	
	b := newBatch(tenantID, entry1, entry2, entry3)
	
	req, entriesCount := b.createPushRequest()
	require.NotNil(t, req)
	assert.Equal(t, 3, entriesCount)
	assert.Equal(t, 2, len(req.Streams)) // Two different label sets
	
	// Check streams
	streamsByLabel := make(map[string]logproto.Stream)
	for _, stream := range req.Streams {
		streamsByLabel[stream.Labels] = stream
	}
	
	// Check first stream (job=test1)
	stream1, exists := streamsByLabel[`{job="test1"}`]
	require.True(t, exists)
	assert.Equal(t, 2, len(stream1.Entries))
	assert.Equal(t, "line 1", stream1.Entries[0].Line)
	assert.Equal(t, "line 2", stream1.Entries[1].Line)
	
	// Check second stream (job=test2)
	stream2, exists := streamsByLabel[`{job="test2"}`]
	require.True(t, exists)
	assert.Equal(t, 1, len(stream2.Entries))
	assert.Equal(t, "line 3", stream2.Entries[0].Line)
}

func TestBatchIsEmpty(t *testing.T) {
	tenantID := "test-tenant"
	b := newBatch(tenantID)
	assert.True(t, b.isEmpty())
	
	entry := entry{
		tenantID: tenantID,
		labels:   model.LabelSet{"job": "test"},
		Entry: logproto.Entry{
			Timestamp: time.Now(),
			Line:      "test",
		},
	}
	
	b.add(entry)
	assert.False(t, b.isEmpty())
}