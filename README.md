# S3 Storage Backend

This package provides AWS S3 compatible storage backend for HybridBuffer that stores data in S3 buckets.

## Features

- **AWS S3 compatible** (works with AWS S3, MinIO, DigitalOcean Spaces, etc.)
- **True streaming uploads** with real-time data transfer using io.Pipe
- **Asynchronous uploads** for better performance
- **Configurable timeouts** and key prefixes
- **Automatic object cleanup** when removed
- **Unique object keys** with timestamp and random suffix
- **Clean API** with private backend constructor

## Installation

```bash
go get schneider.vip/hybridbuffer/storage/s3
```

## Usage

### Basic Setup

```go
import (
    "context"
    
    "schneider.vip/hybridbuffer"
    "schneider.vip/hybridbuffer/storage/s3"
    
    "github.com/aws/aws-sdk-go-v2/config"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Load AWS config
cfg, err := config.LoadDefaultConfig(context.TODO())
if err != nil {
    panic(err)
}

// Create S3 client
client := awss3.NewFromConfig(cfg)

// Create storage factory - returns func() StorageBackend
storage := s3.New(client, "my-bucket")

// Use with HybridBuffer
buf := hybridbuffer.New(
    hybridbuffer.WithStorage(storage),
)
defer buf.Close()
```

### With Custom Options

```go
storage := s3.New(client, "my-bucket",
    s3.WithKeyPrefix("myapp/temp"),
    s3.WithTimeout(60 * time.Second),
)
```

### MinIO Example

```go
import (
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/credentials"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// MinIO configuration
cfg := aws.Config{
    Region: "us-east-1",
    Credentials: credentials.NewStaticCredentialsProvider(
        "minioadmin",     // access key
        "minioadmin",     // secret key
        "",               // session token
    ),
    EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(
        func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{
                URL: "http://localhost:9000",
            }, nil
        }),
}

client := awss3.NewFromConfig(cfg)
storage := s3.New(client, "test-bucket")
```

## Configuration Options

### WithKeyPrefix(prefix string)
Sets the S3 key prefix for objects. Default is "hybridbuffer".

```go
storage := s3.New(client, "bucket",
    s3.WithKeyPrefix("myapp/buffers"),
)
```

Objects will be created like: `myapp/buffers/1640995200-a1b2c3d4.bin`

### WithTimeout(duration time.Duration)
Sets the timeout for S3 operations. Default is 30 seconds.

```go
storage := s3.New(client, "bucket",
    s3.WithTimeout(2 * time.Minute),
)
```

## Object Naming

Objects are created with unique names following the pattern:
```
{prefix}/{timestamp}-{random}.bin
```

- **prefix**: Configurable prefix (default: "hybridbuffer")
- **timestamp**: Unix timestamp when object was created
- **random**: 8 random hex characters for uniqueness
- **extension**: Always ".bin"

Example: `hybridbuffer/1640995200-a1b2c3d4.bin`

## Error Handling

The S3 backend handles various error conditions:

- **Invalid credentials**: Authentication errors from AWS
- **Bucket not found**: Missing bucket or permission issues
- **Network timeouts**: Configurable timeout for operations
- **Upload failures**: Retry logic handled by AWS SDK

## Performance Considerations

- **Streaming uploads**: Data is streamed directly to S3 using io.Pipe (no buffering)
- **Asynchronous processing**: Upload starts immediately on first write
- **Memory efficient**: No intermediate buffering - constant memory usage
- **Single upload**: Uses PutObject for simplicity (no multipart)
- **Network latency**: Performance depends on network connection to S3
- **Background uploads**: Writer operations don't block on S3 upload

## Security

- **IAM permissions**: Requires appropriate S3 permissions
- **Encryption**: Use S3 server-side encryption or encryption middleware
- **Access control**: Configure bucket policies appropriately
- **Temporary objects**: Objects are cleaned up but ensure proper error handling

## Required AWS Permissions

Minimum IAM permissions needed:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::your-bucket/*"
        }
    ]
}
```

## Implementation Details

### Streaming Architecture

The S3 storage backend uses a streaming architecture for optimal performance:

1. **Writer Creation**: Creates an `io.Pipe` for streaming data
2. **Asynchronous Upload**: Starts S3 upload in background goroutine on first write
3. **Streaming Data**: Data flows directly from writer to S3 without buffering
4. **Error Handling**: Upload errors are captured and returned on `Close()`

### API Design

- **Factory Pattern**: `New()` returns a factory function `func() *Backend`
- **Clean API**: Simple constructor with functional options
- **Thread Safety**: Each backend instance is used by single HybridBuffer
- **Resource Management**: Automatic cleanup on `Close()` or `Remove()`

### Error Handling

The backend handles errors gracefully:
- Upload errors are captured in background goroutine
- Errors are returned synchronously on `Close()`
- Pipe closing ensures no deadlocks on error conditions
- Double-close protection prevents resource leaks

## Dependencies

- `github.com/aws/aws-sdk-go-v2/service/s3` - AWS S3 SDK
- Standard library packages for streaming and utilities