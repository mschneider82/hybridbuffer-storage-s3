package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
)

// mockS3Client implements S3Client interface for testing
type mockS3Client struct {
	objects map[string][]byte
	errors  map[string]error
	mu      sync.RWMutex
}

func newMockS3Client() *mockS3Client {
	return &mockS3Client{
		objects: make(map[string][]byte),
		errors:  make(map[string]error),
		mu:      sync.RWMutex{},
	}
}

// Helper to simulate errors for specific operations
func (m *mockS3Client) setError(operation, key string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors[operation+":"+key] = err
}

func (m *mockS3Client) getError(operation, key string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.errors[operation+":"+key]
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := fmt.Sprintf("%s:%s", *params.Bucket, *params.Key)

	if err := m.getError("put", key); err != nil {
		return nil, err
	}

	// Read the body (this now reads from the streaming pipe)
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.objects[key] = data
	m.mu.Unlock()
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := fmt.Sprintf("%s:%s", *params.Bucket, *params.Key)

	if err := m.getError("get", key); err != nil {
		return nil, err
	}

	m.mu.RLock()
	data, exists := m.objects[key]
	m.mu.RUnlock()
	if !exists {
		return nil, errors.New("NoSuchKey: The specified key does not exist")
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (m *mockS3Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	key := fmt.Sprintf("%s:%s", *params.Bucket, *params.Key)

	if err := m.getError("delete", key); err != nil {
		return nil, err
	}

	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	return &s3.DeleteObjectOutput{}, nil
}

func TestS3Backend_BasicOperations(t *testing.T) {
	client := newMockS3Client()
	bucket := "test-bucket"

	backend, err := newBackend(client, bucket)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Remove()

	// Test Create and Write
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	testData := []byte("Hello S3 Storage!")
	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Expected to write %d bytes, got %d", len(testData), n)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify data was stored in mock
	expectedKey := fmt.Sprintf("%s:%s", bucket, backend.key)
	storedData, exists := client.objects[expectedKey]
	if !exists {
		t.Fatal("Data was not stored in S3 mock")
	}
	if !bytes.Equal(storedData, testData) {
		t.Fatalf("Stored data mismatch: expected %q, got %q", string(testData), string(storedData))
	}

	// Test Open and Read
	reader, err := backend.Open()
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	readData := make([]byte, len(testData))
	n, err = reader.Read(readData)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Expected to read %d bytes, got %d", len(testData), n)
	}

	if string(readData) != string(testData) {
		t.Fatalf("Data mismatch: expected %q, got %q", string(testData), string(readData))
	}
}

func TestS3Backend_LargeData(t *testing.T) {
	client := newMockS3Client()
	bucket := "test-bucket"

	backend, err := newBackend(client, bucket)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Remove()

	// Create 1MB of test data
	testData := make([]byte, 1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write large data
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write large data: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Expected to write %d bytes, got %d", len(testData), n)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read large data back
	reader, err := backend.Open()
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read large data: %v", err)
	}

	if len(readData) != len(testData) {
		t.Fatalf("Expected to read %d bytes, got %d", len(testData), len(readData))
	}

	// Verify data integrity
	if !bytes.Equal(testData, readData) {
		t.Fatal("Large data integrity check failed")
	}
}

func TestS3Backend_WithOptions(t *testing.T) {
	client := newMockS3Client()
	bucket := "test-bucket"

	// Test with custom options
	backend, err := newBackend(client, bucket,
		WithKeyPrefix("custom/prefix"),
		WithTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("Failed to create backend with options: %v", err)
	}
	defer backend.Remove()

	// Verify options were set
	if backend.keyPrefix != "custom/prefix" {
		t.Fatalf("Expected key prefix 'custom/prefix', got %q", backend.keyPrefix)
	}
	if backend.timeout != 10*time.Second {
		t.Fatalf("Expected timeout 10s, got %v", backend.timeout)
	}

	// Test that key prefix is used
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	testData := []byte("test data with custom prefix")
	writer.Write(testData)
	writer.Close()

	// Verify the key has the correct prefix
	if !strings.HasPrefix(backend.key, "custom/prefix/") {
		t.Fatalf("Generated key doesn't have expected prefix: %s", backend.key)
	}
}

func TestS3Backend_Remove(t *testing.T) {
	client := newMockS3Client()
	bucket := "test-bucket"

	backend, err := newBackend(client, bucket)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	// Create and write some data
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	testData := []byte("data to be removed")
	writer.Write(testData)
	writer.Close()

	// Verify data exists
	expectedKey := fmt.Sprintf("%s:%s", bucket, backend.key)
	if _, exists := client.objects[expectedKey]; !exists {
		t.Fatal("Data should exist before removal")
	}

	// Remove the data
	err = backend.Remove()
	if err != nil {
		t.Fatalf("Failed to remove data: %v", err)
	}

	// Verify data is gone
	if _, exists := client.objects[expectedKey]; exists {
		t.Fatal("Data should be removed from S3 mock")
	}

	// Test that opening removed data fails
	_, err = backend.Open()
	if err == nil {
		t.Fatal("Expected error when opening removed data, got none")
	}
}

func TestS3Backend_ErrorCases(t *testing.T) {
	// Test with nil client
	_, err := newBackend(nil, "bucket")
	if err == nil {
		t.Fatal("Expected error with nil client, got none")
	}

	// Test with empty bucket
	client := newMockS3Client()
	_, err = newBackend(client, "")
	if err == nil {
		t.Fatal("Expected error with empty bucket, got none")
	}

	// Test proper backend
	backend, err := newBackend(client, "test-bucket")
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	// Test opening without creating first
	_, err = backend.Open()
	if err == nil {
		t.Fatal("Expected error when opening without creating, got none")
	}

	// Test removing without creating
	err = backend.Remove()
	if err != nil {
		t.Fatalf("Remove should not fail for non-existent object: %v", err)
	}
}

func TestS3Backend_S3Errors(t *testing.T) {
	client := newMockS3Client()
	bucket := "test-bucket"

	backend, err := newBackend(client, bucket)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	// Create writer
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Simulate S3 put error before writing
	expectedKey := fmt.Sprintf("%s:%s", bucket, backend.key)
	client.setError("put", expectedKey, errors.New("S3 put error"))

	testData := []byte("test data for error simulation")
	writer.Write(testData)

	// Closing should fail due to S3 error
	err = writer.Close()
	if err == nil {
		t.Fatal("Expected error during S3 put, got none")
	}
	if !strings.Contains(err.Error(), "failed to upload to S3") {
		t.Fatalf("Expected S3 upload error, got: %v", err)
	}

	// Clear the error and successfully upload
	client.setError("put", expectedKey, nil)
	writer2, _ := backend.Create()
	writer2.Write(testData)
	writer2.Close()

	// Update expected key after new Create()
	expectedKey = fmt.Sprintf("%s:%s", bucket, backend.key)

	// Simulate S3 get error
	client.setError("get", expectedKey, errors.New("S3 get error"))
	_, err = backend.Open()
	if err == nil {
		t.Fatal("Expected error during S3 get, got none")
	}

	// Simulate S3 delete error
	client.setError("get", expectedKey, nil) // Clear get error
	client.setError("delete", expectedKey, errors.New("S3 delete error"))
	err = backend.Remove()
	if err == nil {
		t.Fatal("Expected error during S3 delete, got none")
	}
}

func TestS3Backend_Factory(t *testing.T) {
	client := newMockS3Client()
	bucket := "test-bucket"

	// Test factory creation
	factory := New(client, bucket,
		WithKeyPrefix("factory-test"),
		WithTimeout(5*time.Second),
	)

	// Test backend creation from factory
	backend := factory()
	if backend == nil {
		t.Fatal("Factory should return a backend")
	}
	defer backend.Remove()

	// Test that backend works
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Backend Create failed: %v", err)
	}

	testData := []byte("factory test data")
	n, err := writer.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write via factory backend: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Expected to write %d bytes, got %d", len(testData), n)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify options were applied
	if backend.keyPrefix != "factory-test" {
		t.Fatalf("Expected key prefix 'factory-test', got %q", backend.keyPrefix)
	}
	if backend.timeout != 5*time.Second {
		t.Fatalf("Expected timeout 5s, got %v", backend.timeout)
	}
}

func TestS3Backend_KeyGeneration(t *testing.T) {
	client := newMockS3Client()
	bucket := "test-bucket"

	backend, err := newBackend(client, bucket,
		WithKeyPrefix("test/prefix"),
	)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	// Generate multiple keys to test uniqueness
	keys := make(map[string]bool)
	for i := 0; i < 10; i++ {
		key, err := backend.generateKey()
		if err != nil {
			t.Fatalf("Failed to generate key %d: %v", i, err)
		}

		// Check format
		if !strings.HasPrefix(key, "test/prefix/") {
			t.Fatalf("Key %d doesn't have expected prefix: %s", i, key)
		}
		if !strings.HasSuffix(key, ".bin") {
			t.Fatalf("Key %d doesn't have expected suffix: %s", i, key)
		}
		if strings.HasPrefix(key, "/") {
			t.Fatalf("Key %d should not start with '/': %s", i, key)
		}

		// Check uniqueness
		if keys[key] {
			t.Fatalf("Duplicate key generated: %s", key)
		}
		keys[key] = true
	}
}

func TestS3Backend_MultipleWrites(t *testing.T) {
	client := newMockS3Client()
	bucket := "test-bucket"

	backend, err := newBackend(client, bucket)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Remove()

	// Test multiple writes to the same writer
	writer, err := backend.Create()
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	parts := [][]byte{
		[]byte("Hello "),
		[]byte("S3 "),
		[]byte("Storage "),
		[]byte("Backend!"),
	}

	totalWritten := 0
	for i, part := range parts {
		n, err := writer.Write(part)
		if err != nil {
			t.Fatalf("Failed to write part %d: %v", i, err)
		}
		if n != len(part) {
			t.Fatalf("Part %d: expected to write %d bytes, got %d", i, len(part), n)
		}
		totalWritten += n
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read back and verify
	reader, err := backend.Open()
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	expectedData := "Hello S3 Storage Backend!"
	if string(readData) != expectedData {
		t.Fatalf("Expected %q, got %q", expectedData, string(readData))
	}
	if len(readData) != totalWritten {
		t.Fatalf("Expected %d bytes total, got %d", totalWritten, len(readData))
	}
}
