package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"

	"github.com/aws/smithy-go/logging"
	log "github.com/sirupsen/logrus"
	"k8s.io/klog/v2"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"

	"github.com/MingZhang-YBPS/aws-inter-az-traffic-analyzer/internal/util"
)

func main() {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(os.Getenv("AWS_REGION")),
		config.WithLogger(logging.LoggerFunc(func(classification logging.Classification, format string, v ...interface{}) {
			// your custom logging
			log.WithField("process", "analyzer-main").Debug(v...)
		})),
	)
	if err != nil {
		klog.Fatalf("failed to load AWS config: %v", err)
	}

	// athenaClient := athena.NewFromConfig(cfg)
	glueClient := glue.NewFromConfig(cfg)
	//outputLocation := "s3://" + util.AthenaResultBucketName
	database := os.Getenv("JOB_NAME")
	podMetaTable := "podmeta"
	flowTable := "flow"
	resultTable := "result"

	_, err = glueClient.CreateDatabase(ctx, &glue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{
			Name: aws.String(database),
		},
	})
	if err != nil {
		var exists *gluetypes.AlreadyExistsException
		if errors.As(err, &exists) {
			klog.Infof("Database %s already exists — safe to ignore.", database)
		} else {
			klog.Fatal(err)
		}
	} else {
		klog.Infof("Database %s created successfully.", database)
	}

	_, err = glueClient.CreateTable(ctx, &glue.CreateTableInput{
		DatabaseName: aws.String(database),
		TableInput: &gluetypes.TableInput{
			Name: aws.String(podMetaTable),
			StorageDescriptor: &gluetypes.StorageDescriptor{
				Columns: []gluetypes.Column{
					{Name: aws.String("uid"), Type: aws.String("string")},
					{Name: aws.String("name"), Type: aws.String("string")},
					{Name: aws.String("ip"), Type: aws.String("string")},
					{Name: aws.String("app"), Type: aws.String("string")},
					{Name: aws.String("creation_time"), Type: aws.String("string")},
					{Name: aws.String("node"), Type: aws.String("string")},
					{Name: aws.String("az"), Type: aws.String("string")},
				},
				Location:     aws.String(fmt.Sprintf("s3://%s/%s/", util.EKSPodMetadataBucketName, os.Getenv("CLUSTER"))),
				InputFormat:  aws.String("org.apache.hadoop.mapred.TextInputFormat"),
				OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
				SerdeInfo: &gluetypes.SerDeInfo{
					SerializationLibrary: aws.String("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
					Parameters: map[string]string{
						"field.delim":            ",", // CSV 分隔符
						"skip.header.line.count": "1", // 跳过第一行表头（可选）
					},
				},
			},
			TableType: aws.String("EXTERNAL_TABLE"),
			Parameters: map[string]string{
				"classification":  "csv",
				"compressionType": "none",
				"typeOfData":      "file",
			},
		},
	})
	if err != nil {
		var exists *gluetypes.AlreadyExistsException
		if errors.As(err, &exists) {
			klog.Infof("Table %s already exists.", podMetaTable)
		} else {
			klog.Fatal(err)
		}
	} else {
		klog.Infof("Table %s created.", podMetaTable)
	}

	_, err = glueClient.CreateTable(ctx, &glue.CreateTableInput{
		DatabaseName: aws.String(database),
		TableInput: &gluetypes.TableInput{
			Name: aws.String(flowTable),
			StorageDescriptor: &gluetypes.StorageDescriptor{
				Columns: []gluetypes.Column{
					{Name: aws.String("az_id"), Type: aws.String("string")},
					{Name: aws.String("flow_direction"), Type: aws.String("string")},
					{Name: aws.String("pkt_srcaddr"), Type: aws.String("string")},
					{Name: aws.String("pkt_dstaddr"), Type: aws.String("string")},
					{Name: aws.String("start"), Type: aws.String("bigint")},
					{Name: aws.String("bytes"), Type: aws.String("bigint")},
				},
				Location:     aws.String(fmt.Sprintf("s3://%s/%s/", util.VPCFlowLogBucketName, os.Getenv("VPC_ID"))),
				InputFormat:  aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
				OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
				SerdeInfo: &gluetypes.SerDeInfo{
					SerializationLibrary: aws.String("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
				},
			},
			TableType: aws.String("EXTERNAL_TABLE"),
			Parameters: map[string]string{
				"classification": "parquet",
				"typeOfData":     "file",
			},
		},
	})
	if err != nil {
		var exists *gluetypes.AlreadyExistsException
		if errors.As(err, &exists) {
			klog.Infof("Table %s already exists.", flowTable)
		} else {
			klog.Fatal(err)
		}
	} else {
		klog.Infof("Table %s created.", flowTable)
	}

	_, err = glueClient.CreateTable(ctx, &glue.CreateTableInput{
		DatabaseName: aws.String(database),
		TableInput: &gluetypes.TableInput{
			Name: aws.String(resultTable),
			StorageDescriptor: &gluetypes.StorageDescriptor{
				Columns: []gluetypes.Column{
					{Name: aws.String("timestamp"), Type: aws.String("timestamp")},
					{Name: aws.String("cross_az_traffic"), Type: aws.String("string")},
					{Name: aws.String("bytes_transfered"), Type: aws.String("bigint")},
				},
				Location:     aws.String(fmt.Sprintf("s3://%s/%s/", util.AthenaResultBucketName, os.Getenv("JOB_NAME"))),
				InputFormat:  aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
				OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
				SerdeInfo: &gluetypes.SerDeInfo{
					SerializationLibrary: aws.String("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
				},
			},
			TableType: aws.String("EXTERNAL_TABLE"),
			Parameters: map[string]string{
				"classification": "parquet",
				"typeOfData":     "file",
			},
		},
	})
	if err != nil {
		var exists *gluetypes.AlreadyExistsException
		if errors.As(err, &exists) {
			klog.Infof("Table %s already exists.", resultTable)
		} else {
			klog.Fatal(err)
		}
	} else {
		klog.Infof("Table %s created.", resultTable)
	}
}

func runQuery(ctx context.Context, client *athena.Client, query string, outputLocation string) string {
	start, err := client.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: aws.String(outputLocation),
		},
	})
	if err != nil {
		klog.Fatalf("failed to start query: %v", err)
	}

	queryID := aws.ToString(start.QueryExecutionId)
	klog.Infof("Started query: %s", queryID)

	for {
		statusResp, err := client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			klog.Fatalf("failed to get query status: %v", err)
		}

		state := string(statusResp.QueryExecution.Status.State)
		if state == "SUCCEEDED" {
			klog.Infof("Query succeeded")
			break
		} else if state == "FAILED" || state == "CANCELLED" {
			klog.Fatalf("query failed or cancelled: %v", statusResp.QueryExecution.Status.StateChangeReason)
		}

		time.Sleep(2 * time.Second)
	}

	// TODO: write query result location to job annotation

	return queryID
}
