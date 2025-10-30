package util

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"k8s.io/klog/v2"
)

func EnsurePrerequisites(ctx context.Context, cfg aws.Config, vpcId string) error {
	_, err := ensureVPCFlowLog(ctx, cfg, vpcId)
	if err != nil {
		klog.Error(err)
		return err
	}
	err = ensurePodMetadataBucket(ctx, cfg, vpcId)
	if err != nil {
		klog.Error(err)
		return err
	}
	err = ensureAnthenaResultBucket(ctx, cfg, vpcId)
	if err != nil {
		klog.Error(err)
		return err
	}
	return nil
}

func PutDirInS3Bucket(ctx context.Context, cfg aws.Config, bucket string, key string) error {
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = os.Getenv("AWS_REGION")
	})

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	_, err := client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create dir, %w", err)
	}

	klog.Infof("Successfully created dir %q in bucket %q", key, bucket)
	return nil
}

func PutCSVToS3(ctx context.Context, cfg aws.Config, bucket string, key string, localFilePath string) error {
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = os.Getenv("AWS_REGION")
	})

	var file *os.File = nil
	if len(localFilePath) > 0 {
		file, err := os.Open(localFilePath)
		if err != nil {
			return fmt.Errorf("failed to open file, %w", err)
		}
		defer file.Close()
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        file,
		ContentType: aws.String("text/csv"), // 指定文件类型
	}

	_, err := client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to upload file, %w", err)
	}

	klog.Infof("Successfully uploaded %q to bucket %q", key, bucket)
	return nil
}

func ensureS3Bucket(ctx context.Context, s3Client *s3.Client, bucketName string, objectExpireDays *int32) error {
	_, err := s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) ||
			strings.Contains(err.Error(), "NotFound") ||
			strings.Contains(err.Error(), "404") ||
			strings.Contains(err.Error(), "NoSuchBucket") {
		} else {
			// Some other error (like AccessDenied)
			return fmt.Errorf("cannot check bucket: %w", err)
		}

		createInput := &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		}
		_, err = s3Client.CreateBucket(ctx, createInput)
		if err != nil {
			return fmt.Errorf("failed to create bucket %q: %w", bucketName, err)
		}

		klog.Infof("Created bucket: %q", bucketName)
	} else {
		klog.Infof("Bucket %q already exists.", bucketName)

		if objectExpireDays != nil {
			_, err = s3Client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
				Bucket: aws.String(bucketName),
				LifecycleConfiguration: &types.BucketLifecycleConfiguration{
					Rules: []types.LifecycleRule{
						{
							ID:     aws.String("ExpireObjectsAfter1Day"),
							Status: types.ExpirationStatusEnabled,
							Expiration: &types.LifecycleExpiration{
								Days: objectExpireDays,
							},
						},
					},
				},
			})
			if err != nil {
				return fmt.Errorf("failed to set lifecycle bucket %q: %w", bucketName, err)
			}
		}
		_, err = s3Client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
			Bucket: aws.String(bucketName),
			ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
				Rules: []types.ServerSideEncryptionRule{
					{
						ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
							SSEAlgorithm: types.ServerSideEncryptionAes256,
						},
					},
				},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to set encryption bucket %q: %w", bucketName, err)
		}

		_, err = s3Client.PutPublicAccessBlock(ctx, &s3.PutPublicAccessBlockInput{
			Bucket: aws.String(bucketName),
			PublicAccessBlockConfiguration: &types.PublicAccessBlockConfiguration{
				BlockPublicAcls:       aws.Bool(true),
				IgnorePublicAcls:      aws.Bool(true),
				BlockPublicPolicy:     aws.Bool(true),
				RestrictPublicBuckets: aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to block public access bucket %q: %w", bucketName, err)
		}

		policy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Sid": "EnforceSSL",
			"Effect": "Deny",
			"Principal": "*",
			"Action": "s3:*",
			"Resource": [
				"arn:aws:s3:::%s",
				"arn:aws:s3:::%s/*"
			],
			"Condition": {
				"Bool": {"aws:SecureTransport": "false"}
			}
		}]
	}`, bucketName, bucketName)

		_, err = s3Client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
			Bucket: aws.String(bucketName),
			Policy: aws.String(policy),
		})
		if err != nil {
			return fmt.Errorf("failed to enforce ssl bucket %q: %w", bucketName, err)
		}
	}

	return nil
}

func ensureAnthenaResultBucket(ctx context.Context, cfg aws.Config, vpcId string) error {
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = os.Getenv("AWS_REGION")
	})
	err := ensureS3Bucket(ctx, client, AnthenaResultBucketName, nil)
	if err != nil {
		return fmt.Errorf("failed to create s3 bucket %q: %w", AnthenaResultBucketName, err)
	}
	return nil
}

func ensurePodMetadataBucket(ctx context.Context, cfg aws.Config, vpcId string) error {
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = os.Getenv("AWS_REGION")
	})
	err := ensureS3Bucket(ctx, client, EKSPodMetadataBucketName, nil)
	if err != nil {
		return fmt.Errorf("failed to create s3 bucket %q: %w", EKSPodMetadataBucketName, err)
	}
	return nil
}

func ensureVPCFlowLog(ctx context.Context, cfg aws.Config, vpcId string) (string, error) {
	client := ec2.NewFromConfig(cfg)
	existing, err := client.DescribeFlowLogs(ctx, &ec2.DescribeFlowLogsInput{
		Filter: []ec2types.Filter{
			{Name: aws.String("resource-id"), Values: []string{vpcId}},
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to describe VPC %q flow logs: %w", vpcId, err)
	}
	if len(existing.FlowLogs) > 0 {
		fl := existing.FlowLogs[0]
		klog.Infof("Flow log already exists for VPC %q: %q", vpcId, *fl.FlowLogId)
		return *fl.FlowLogId, nil
	}

	err = ensureS3Bucket(ctx, s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = os.Getenv("AWS_REGION")
	}), VPCFlowLogBucketName, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create s3 bucket %q: %w", VPCFlowLogBucketName, err)
	}

	err = PutDirInS3Bucket(ctx, cfg, VPCFlowLogBucketName, vpcId+"/", "")
	if err != nil {
		return "", fmt.Errorf("failed to create dir in s3 bucket %q: %w", VPCFlowLogBucketName, err)
	}

	out, err := client.CreateFlowLogs(ctx, &ec2.CreateFlowLogsInput{
		ResourceIds:        []string{vpcId},
		ResourceType:       "VPC",
		TrafficType:        "ALL",
		LogDestinationType: "s3",
		LogDestination:     aws.String("arn:aws:s3:::" + VPCFlowLogBucketName + "/" + vpcId + "/"),
		LogFormat:          aws.String(FlowLogsFormat),
		DestinationOptions: &ec2types.DestinationOptionsRequest{
			FileFormat:               ec2types.DestinationFileFormatParquet,
			HiveCompatiblePartitions: aws.Bool(false),
			PerHourPartition:         aws.Bool(true),
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to create VPC %q flow log: %w", vpcId, err)
	}
	if len(out.FlowLogIds) == 0 {
		return "", fmt.Errorf("no FlowLogId returned after creation")
	}

	klog.Infof("Created new flow log for VPC %q: %q", vpcId, out.FlowLogIds[0])
	return out.FlowLogIds[0], nil
}
