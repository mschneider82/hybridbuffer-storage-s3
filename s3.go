// Package s3 provides AWS S3 storage backend for HybridBuffer
package s3

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
	"schneider.vip/hybridbuffer/storage"
)

// S3Client interface for testing and flexibility
type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// Backend implements StorageBackend for AWS S3
type Backend struct {
	client    S3Client
	bucket    string
	keyPrefix string
	key       string
	timeout   time.Duration
}

// Option configures S3 storage backend
type Option func(*Backend)

// WithKeyPrefix sets the S3 key prefix for objects
func WithKeyPrefix(prefix string) Option {
	return func(s *Backend) {
		s.keyPrefix = prefix
	}
}

// WithTimeout sets the timeout for S3 operations
func WithTimeout(timeout time.Duration) Option {
	return func(s *Backend) {
		s.timeout = timeout
	}
}

// newBackend creates a new S3-based storage backend
func newBackend(client S3Client, bucket string, opts ...Option) (*Backend, error) {
	if client == nil {
		return nil, errors.New("S3 client cannot be nil")
	}
	if bucket == "" {
		return nil, errors.New("bucket name cannot be empty")
	}

	backend := &Backend{
		client:    client,
		bucket:    bucket,
		keyPrefix: "hybridbuffer",
		timeout:   30 * time.Second,
	}

	// Apply options
	for _, opt := range opts {
		opt(backend)
	}

	return backend, nil
}

// Create implements StorageBackend
func (s *Backend) Create() (io.WriteCloser, error) {
	// Generate unique key
	key, err := s.generateKey()
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate S3 key")
	}
	s.key = key

	return &s3WriteCloser{
		backend: s,
		pipeR:   nil,
		pipeW:   nil,
		done:    make(chan error, 1),
	}, nil
}

// Open implements StorageBackend
func (s *Backend) Open() (io.ReadCloser, error) {
	if s.key == "" {
		return nil, errors.New("no object created yet")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	input := &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &s.key,
	}

	result, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get S3 object")
	}

	return result.Body, nil
}

// Remove implements StorageBackend
func (s *Backend) Remove() error {
	if s.key == "" {
		return nil // Nothing to remove
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	input := &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &s.key,
	}

	_, err := s.client.DeleteObject(ctx, input)
	if err != nil {
		return errors.Wrap(err, "failed to delete S3 object")
	}

	return nil
}

// generateKey creates a unique S3 object key
func (s *Backend) generateKey() (string, error) {
	// Generate random suffix
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}

	timestamp := time.Now().Unix()
	randomSuffix := fmt.Sprintf("%x", randomBytes)

	key := fmt.Sprintf("%s/%d-%s.bin", s.keyPrefix, timestamp, randomSuffix)

	// Ensure key doesn't start with /
	key = strings.TrimPrefix(key, "/")

	return key, nil
}

// s3WriteCloser implements io.WriteCloser for S3 uploads with streaming
type s3WriteCloser struct {
	backend *Backend
	pipeR   *io.PipeReader
	pipeW   *io.PipeWriter
	done    chan error
	started bool
	closed  bool
}

// Write implements io.Writer
func (w *s3WriteCloser) Write(p []byte) (n int, err error) {
	if !w.started {
		w.pipeR, w.pipeW = io.Pipe()
		w.started = true

		// Start streaming upload in background
		go w.streamUpload()
	}

	return w.pipeW.Write(p)
}

// streamUpload performs the actual S3 upload in a goroutine
func (w *s3WriteCloser) streamUpload() {
	defer func() {
		if w.pipeR != nil {
			w.pipeR.Close()
		}
	}()
	
	ctx, cancel := context.WithTimeout(context.Background(), w.backend.timeout)
	defer cancel()

	input := &s3.PutObjectInput{
		Bucket: &w.backend.bucket,
		Key:    &w.backend.key,
		Body:   w.pipeR,
	}

	_, err := w.backend.client.PutObject(ctx, input)
	if err != nil {
		w.done <- errors.Wrap(err, "failed to upload to S3")
		return
	}

	w.done <- nil
}

// Close implements io.Closer and waits for the upload to complete
func (w *s3WriteCloser) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	
	if !w.started {
		// Nothing was written, create empty upload
		w.pipeR, w.pipeW = io.Pipe()
		w.started = true
		go w.streamUpload()
	}

	// Close the write end of the pipe
	if w.pipeW != nil {
		w.pipeW.Close()
	}

	// Wait for upload to complete
	return <-w.done
}

// New creates a new S3 storage backend provider function
func New(client S3Client, bucket string, opts ...Option) func() storage.Backend {
	return func() storage.Backend {
		backend, err := newBackend(client, bucket, opts...)
		if err != nil {
			panic(err)
		}
		return backend
	}
}
