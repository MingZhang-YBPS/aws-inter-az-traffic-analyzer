package main

import (
	"context"
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

	client := athena.NewFromConfig(cfg)
	outputLocation := "s3://" + util.AthenaResultBucketName
	database := os.Getenv("JOB_NAME")

	createDB := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, database)
	runQuery(ctx, client, createDB, outputLocation)

	createTable := fmt.Sprintf(`
CREATE EXTERNAL TABLE IF NOT EXISTS %s.podmeta (
  uid STRING,
  name STRING,
  ip STRING,
  app STRING,
  creation_time STRING,
  node STRING,
  az STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = ','
)
STORED AS TEXTFILE
LOCATION 's3://%s/%s/';

CREATE EXTERNAL TABLE IF NOT EXISTS %s.flow (
  az_id STRING,
  flow_direction STRING,
  pkt_srcaddr STRING,
  pkt_dstaddr STRING,
  start BIGINT,
  bytes BIGINT
)
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://%s/%s/';


	`,
		database, util.AthenaResultBucketName, os.Getenv("CLUSTER"),
		database, util.VPCFlowLogBucketName, os.Getenv("VPC_ID"))
	runQuery(ctx, client, createTable, outputLocation)

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

	return queryID
}
