package grpc

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDefaultConfig(t *testing.T) {
	serverAddr := "localhost:9095"
	cfg, err := NewDefaultConfig(serverAddr)
	require.NoError(t, err)

	assert.Equal(t, serverAddr, cfg.ServerAddress)
	assert.Equal(t, DefaultBatchWait, cfg.BatchWait)
	assert.Equal(t, DefaultBatchSize, cfg.BatchSize)
	assert.Equal(t, DefaultTimeout, cfg.Timeout)
	assert.Equal(t, DefaultKeepAlive, cfg.KeepAlive)
	assert.Equal(t, DefaultKeepAliveTimeout, cfg.KeepAliveTimeout)
}

func TestConfigRegisterFlags(t *testing.T) {
	var cfg Config
	f := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.RegisterFlags(f)

	// Test setting flags
	args := []string{
		"-grpc.server-address=localhost:9095",
		"-grpc.batch-wait=5s",
		"-grpc.batch-size-bytes=2048",
		"-grpc.timeout=30s",
		"-grpc.tls.enabled=true",
		"-grpc.tls.server-name=loki.example.com",
		"-grpc.tenant-id=test-tenant",
	}

	err := f.Parse(args)
	require.NoError(t, err)

	assert.Equal(t, "localhost:9095", cfg.ServerAddress)
	assert.Equal(t, 5*time.Second, cfg.BatchWait)
	assert.Equal(t, 2048, cfg.BatchSize)
	assert.Equal(t, 30*time.Second, cfg.Timeout)
	assert.True(t, cfg.TLS.Enabled)
	assert.Equal(t, "loki.example.com", cfg.TLS.ServerName)
	assert.Equal(t, "test-tenant", cfg.TenantID)
}

func TestConfigRegisterFlagsWithPrefix(t *testing.T) {
	var cfg Config
	f := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.RegisterFlagsWithPrefix("loki.", f)

	args := []string{
		"-loki.grpc.server-address=localhost:9095",
		"-loki.grpc.batch-wait=2s",
	}

	err := f.Parse(args)
	require.NoError(t, err)

	assert.Equal(t, "localhost:9095", cfg.ServerAddress)
	assert.Equal(t, 2*time.Second, cfg.BatchWait)
}

func TestBuildDialOptions(t *testing.T) {
	cfg := Config{
		KeepAlive:        30 * time.Second,
		KeepAliveTimeout: 5 * time.Second,
		TLS: TLSConfig{
			Enabled:            false,
			InsecureSkipVerify: true,
		},
	}

	opts, err := cfg.BuildDialOptions()
	require.NoError(t, err)
	assert.NotEmpty(t, opts)
}

func TestBuildDialOptionsWithTLS(t *testing.T) {
	cfg := Config{
		KeepAlive:        30 * time.Second,
		KeepAliveTimeout: 5 * time.Second,
		TLS: TLSConfig{
			Enabled:            true,
			ServerName:         "loki.example.com",
			InsecureSkipVerify: true,
		},
	}

	opts, err := cfg.BuildDialOptions()
	require.NoError(t, err)
	assert.NotEmpty(t, opts)
}

func TestConfigUnmarshalYAML(t *testing.T) {
	var cfg Config
	err := cfg.UnmarshalYAML(func(v interface{}) error {
		// This is a simplified test - in real usage, this would be called by yaml.Unmarshal
		return nil
	})
	require.NoError(t, err)
}
