package storage

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hako/durafmt"
	"github.com/huacnlee/gobackup/logger"
)

// S3 - Amazon S3 storage
//
// type: s3
// bucket: gobackup-test
// region: us-east-1
// path: backups
// access_key_id: your-access-key-id
// secret_access_key: your-secret-access-key
// max_retries: 5
// timeout: 300
type S3 struct {
	Base
	Service string

	bucket string
	path   string
	client *s3manager.Uploader
}

func (s S3) providerName() string {
	switch s.Service {
	case "s3":
		return "AWS S3"
	case "b2":
		return "Backblaze B2"
	case "us3":
		return "UCloud US3"
	case "cos":
		return "QCloud COS"
	case "kodo":
		return "Qiniu Kodo"
	case "r2":
		return "Cloudflare R2"
	case "spaces":
		return "DigitalOcean Spaces"
	}

	return "AWS S3"
}

func (s S3) defaultRegion() string {
	switch s.Service {
	case "s3":
		return "us-east-1"
	case "b2":
		return "us-east-001"
	case "us3":
		return "s3-cn-bj"
	case "cos":
		return "ap-nanjing"
	case "kodo":
		return "cn-east-1"
	case "r2":
		return "us-east-1"
	case "spaces":
		return "nyc1"
	}

	return "us-east-1"
}

func (s S3) defaultEndpoint() *string {
	switch s.Service {
	case "b2":
		return aws.String(fmt.Sprintf("%s.backblazeb2.com", s.viper.GetString("region")))
	case "us3":
		return aws.String(fmt.Sprintf("%s.ufileos.com", s.viper.GetString("region")))
	case "cos":
		return aws.String(fmt.Sprintf("cos.%s.myqcloud.com", s.viper.GetString("region")))
	case "kodo":
		return aws.String(fmt.Sprintf("s3-%s.qiniucs.com", s.viper.GetString("region")))
	case "r2":
		return aws.String(fmt.Sprintf("%s.r2.cloudflarestorage.com", s.viper.GetString("region")))
	case "spaces":
		return aws.String(fmt.Sprintf("%s.digitaloceanspaces.com", s.viper.GetString("region")))
	}

	return aws.String("")
}

func (s *S3) init() {
	s.viper.SetDefault("region", s.defaultRegion())
	s.viper.SetDefault("endpoint", s.defaultEndpoint())
	s.viper.SetDefault("max_retries", 3)
	s.viper.SetDefault("timeout", "300")
}

func (s *S3) open() (err error) {
	s.init()

	cfg := aws.NewConfig()
	endpoint := s.viper.GetString("endpoint")

	if len(endpoint) > 0 {
		cfg.Endpoint = aws.String(endpoint)
		cfg.S3ForcePathStyle = aws.Bool(true)
	}

	cfg.Credentials = credentials.NewStaticCredentials(
		s.viper.GetString("access_key_id"),
		s.viper.GetString("secret_access_key"),
		s.viper.GetString("token"),
	)

	cfg.Region = aws.String(s.viper.GetString("region"))
	cfg.MaxRetries = aws.Int(s.viper.GetInt("max_retries"))

	s.bucket = s.viper.GetString("bucket")
	s.path = s.viper.GetString("path")

	timeout := s.viper.GetInt("timeout")
	uploadTimeoutDuration := time.Duration(timeout) * time.Second

	httpClient := &http.Client{Timeout: uploadTimeoutDuration}
	cfg.HTTPClient = httpClient

	sess := session.Must(session.NewSession(cfg))
	s.client = s3manager.NewUploader(sess)

	return
}

func (s *S3) close() {
}

func (s *S3) upload(fileKey string) (err error) {
	logger := logger.Tag(s.providerName())

	f, err := os.Open(s.archivePath)
	if err != nil {
		return fmt.Errorf("failed to open file %q, %v", s.archivePath, err)
	}

	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to get size of file %q, %v", s.archivePath, err)
	}

	remotePath := path.Join(s.path, fileKey)

	input := &s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(remotePath),
		Body:   f,
	}

	logger.Info(fmt.Sprintf("-> Uploading (%d MiB)...", info.Size()/(1024*1024)))

	start := time.Now()

	result, err := s.client.Upload(input, func(uploader *s3manager.Uploader) {
		// set the part size as low as possible to avoid timeouts and aborts
		// also set concurrency to 1 for the same reason
		var partSize int64 = 5242880 // 5MiB
		maxParts := math.Ceil(float64(info.Size() / partSize))

		// 10000 parts is the limit for AWS S3. If the resulting number of parts would exceed that limit, increase the
		// part size as much as needed but as little possible
		if maxParts > 10000 {
			partSize = int64(math.Ceil(float64(info.Size()) / 10000))
		}

		uploader.Concurrency = 1
		uploader.LeavePartsOnError = false
		uploader.PartSize = partSize
	})

	if err != nil {
		return fmt.Errorf("failed to upload file, %v", err)
	}

	t := time.Now()
	elapsed := t.Sub(start)

	logger.Info("=>", result.Location)
	if s.Service == "s3" {
		logger.Info("=>", fmt.Sprintf("s3://%s/%s", s.bucket, remotePath))
	}
	rate := math.Ceil(float64(info.Size()) / (elapsed.Seconds() * 1024 * 1024))

	logger.Info(fmt.Sprintf("Duration %v, rate %.1f MiB/s", durafmt.Parse(elapsed).LimitFirstN(2).String(), rate))

	return nil
}

func (s *S3) delete(fileKey string) (err error) {
	remotePath := path.Join(s.path, fileKey)
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(remotePath),
	}
	_, err = s.client.S3.DeleteObject(input)
	return
}
