package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/smithy-go/logging"

	"github.com/MingZhang-YBPS/aws-inter-az-traffic-analyzer/internal/util"
)

var podMetaTable = "podmeta"
var flowTable = "flow"
var resultTable = "result"

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

	glueClient := glue.NewFromConfig(cfg)
	s3client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = os.Getenv("AWS_REGION")
	})
	database := os.Getenv("JOB_NAME")

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
					SerializationLibrary: aws.String("org.apache.hadoop.hive.serde2.OpenCSVSerde"),
					Parameters: map[string]string{
						"separatorChar":          ",", // CSV 分隔符
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
					{Name: aws.String("end"), Type: aws.String("bigint")},
					{Name: aws.String("bytes"), Type: aws.String("bigint")},
				},
				Location:     aws.String(fmt.Sprintf("s3://%s/%s/", util.VPCFlowLogBucketName, os.Getenv("VPC_ID"))),
				InputFormat:  aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
				OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
				SerdeInfo: &gluetypes.SerDeInfo{
					SerializationLibrary: aws.String("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
					Parameters: map[string]string{
						"serialization.format": "1",
					},
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
	/*
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
						Parameters: map[string]string{
							"serialization.format": "1",
						},
					},
				},
				TableType: aws.String("EXTERNAL_TABLE"),
				Parameters: map[string]string{
					"classification": "parquet",
					"typeOfData":     "file",
				},
			},
		}) */
	_, err = glueClient.CreateTable(ctx, &glue.CreateTableInput{
		DatabaseName: aws.String(database),
		TableInput: &gluetypes.TableInput{
			Name: aws.String(resultTable),
			StorageDescriptor: &gluetypes.StorageDescriptor{
				Columns: []gluetypes.Column{
					// {Name: aws.String("timestamp"), Type: aws.String("timestamp")},
					{Name: aws.String("cross_az_traffic"), Type: aws.String("string")},
					{Name: aws.String("bytes_transfered"), Type: aws.String("string")},
				},
				Location:     aws.String(fmt.Sprintf("s3://%s/%s/", util.AthenaResultBucketName, os.Getenv("JOB_NAME"))),
				InputFormat:  aws.String("org.apache.hadoop.mapred.TextInputFormat"),
				OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"),
				SerdeInfo: &gluetypes.SerDeInfo{
					SerializationLibrary: aws.String("org.apache.hadoop.hive.serde2.OpenCSVSerde"),
					Parameters: map[string]string{
						"separatorChar":          ",", // CSV 分隔符
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
			klog.Infof("Table %s already exists.", resultTable)
		} else {
			klog.Fatal(err)
		}
	} else {
		klog.Infof("Table %s created.", resultTable)
	}

	// TODO: write query result location to job annotation
	athenaClient := athena.NewFromConfig(cfg)
	outputLocation := "s3://" + util.AthenaResultBucketName + "/" + os.Getenv("JOB_NAME") + "/"
	query := `
INSERT INTO result
WITH 
egress_flow_summary AS (
  SELECT
    pkt_srcaddr,
    pkt_dstaddr,
    az_id AS srcazid,
    SUM(bytes) AS total_bytes
  FROM flow
  WHERE flow_direction = 'egress'
    AND from_unixtime("start") >= from_iso8601_timestamp('%s')
    AND from_unixtime("end")   <= from_iso8601_timestamp('%s')
  GROUP BY pkt_srcaddr, pkt_dstaddr, az_id
),
podmeta_unique AS (
  SELECT ip, app, az
  FROM (
    SELECT uid, ip, app, az,
           ROW_NUMBER() OVER (PARTITION BY uid) AS rn
    FROM podmeta
  )
  WHERE rn = 1
)

SELECT
  CONCAT(srcpod.app, ' -> ', dstpod.app) AS cross_az_traffic,
  CAST(SUM(f.total_bytes) AS VARCHAR) AS bytes_transfered
FROM egress_flow_summary f
JOIN podmeta_unique srcpod ON f.pkt_srcaddr = srcpod.ip
JOIN podmeta_unique dstpod ON f.pkt_dstaddr = dstpod.ip
WHERE srcpod.app != '<none>'
  AND dstpod.app != '<none>'
  AND srcpod.az != dstpod.az
GROUP BY CONCAT(srcpod.app, ' -> ', dstpod.app)
ORDER BY SUM(f.total_bytes) DESC;

`
	/*	query := `
		INSERT INTO result
		WITH
		ip_addresses_and_az_mapping AS (
		SELECT DISTINCT pkt_srcaddr as ipaddress, az_id
		FROM flow
		WHERE flow_direction = 'egress'
		),
		egress_flows_of_pods_with_status AS (
		SELECT
		podmeta.name as srcpodname,
		podmeta.app as srcpodapp,
		pkt_srcaddr as srcaddr,
		pkt_dstaddr as dstaddr,
		flow.az_id as srcazid,
		bytes,
		start
		FROM flow
		INNER JOIN podmeta ON flow.pkt_srcaddr = podmeta.ip
		WHERE flow_direction = 'egress'
		),

		cross_az_traffic_by_pod as (
		SELECT
		srcaddr,
		srcpodname,
		srcpodapp,
		dstaddr,
		podmeta.name as dstpodname,
		podmeta.app as dstpodapp,
		srcazid,
		ip_addresses_and_az_mapping.az_id as dstazid,
		bytes,
		start
		FROM egress_flows_of_pods_with_status
		INNER JOIN podmeta ON dstaddr = podmeta.ip
		LEFT JOIN ip_addresses_and_az_mapping ON dstaddr = ipaddress
		WHERE ip_addresses_and_az_mapping.az_id != srcazid
		)

		SELECT date_trunc('MINUTE', from_unixtime(start)) AS time, CONCAT(srcpodapp, ' -> ', dstpodapp) as inter_az_traffic, sum(bytes) as total_bytes
		FROM cross_az_traffic_by_pod
		WHERE srcpodapp!='<none>' AND dstpodapp!='<none>'
		GROUP BY date_trunc('MINUTE', from_unixtime(start)), CONCAT(srcpodapp, ' -> ', dstpodapp)
		ORDER BY time, total_bytes DESC
		`
	*/

	klog.Infof("startFrom %s, endTo %s", os.Getenv("START_FROM"), os.Getenv("END_TO"))
	_, resultLocation := runQuery(ctx, athenaClient, database,
		fmt.Sprintf(query, os.Getenv("START_FROM"), os.Getenv("END_TO")), outputLocation)
	if len(resultLocation) > 0 {
		config, err := rest.InClusterConfig()
		if err != nil {
			klog.Fatalf("Error loading in-cluster config: %v", err)
		}

		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			klog.Fatalf("Failed to create clientset: %v", err)
		}

		job, err := clientset.BatchV1().Jobs(os.Getenv("MY_POD_NAMESPACE")).Get(ctx, os.Getenv("JOB_NAME"), metav1.GetOptions{})
		if err != nil {
			klog.Fatalf("Failed to get job: %v", err)
		}
		if job.Annotations == nil {
			job.Annotations = make(map[string]string)
		}
		// 最终地址其实还在manifests.csv的文件内容里面
		job.Annotations[util.AnalyzerReportLocationAnnotation] = getActualLocFromManifestCSV(
			ctx,
			s3client,
			resultLocation+"-manifest.csv")
		_, err = clientset.BatchV1().Jobs(os.Getenv("MY_POD_NAMESPACE")).Update(ctx, job, metav1.UpdateOptions{})
		if err != nil {
			klog.Fatalf("Failed to update job: %v", err)
		}
	}
}

func getActualLocFromManifestCSV(ctx context.Context, client *s3.Client, manifestLoc string) string {
	bucket, key, err := parseS3URI(manifestLoc)
	if err != nil {
		klog.Errorf("解析S3 URI失败: %v", err)
		return ""
	}
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		klog.Errorf("获取对象失败: %v", err)
		return ""
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		klog.Errorf("读取内容失败: %v", err)
		return ""
	}

	return string(body)
}

func parseS3URI(s3uri string) (bucket string, key string, err error) {
	if !strings.HasPrefix(s3uri, "s3://") {
		return "", "", fmt.Errorf("无效的S3 URI: %s", s3uri)
	}

	u, err := url.Parse(s3uri)
	if err != nil {
		return "", "", fmt.Errorf("解析S3 URI失败: %w", err)
	}

	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")
	return bucket, key, nil
}

func runQuery(ctx context.Context, client *athena.Client, database string, query string, outputLocation string) (queryID string, resultLocation string) {
	start, err := client.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: aws.String(outputLocation),
		},
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: aws.String(database), // Athena 中的数据库
		},
	})
	if err != nil {
		klog.Fatalf("failed to start query: %v", err)
	}

	queryID = aws.ToString(start.QueryExecutionId)
	klog.Infof("Started query: %s", queryID)

	// for loop till job is cancelled by k8s ActiveDeadlineSeconds
	for {
		statusResp, err := client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			klog.Fatalf("failed to get query status: %v", err)
		}

		if statusResp.QueryExecution != nil &&
			statusResp.QueryExecution.Status != nil {
			state := string(statusResp.QueryExecution.Status.State)
			if state == "SUCCEEDED" {
				klog.Infof("Query succeeded")
				if statusResp.QueryExecution.ResultConfiguration != nil &&
					statusResp.QueryExecution.ResultConfiguration.OutputLocation != nil {
					resultLocation = *statusResp.QueryExecution.ResultConfiguration.OutputLocation
				}
				break
			} else if state == "FAILED" || state == "CANCELLED" {
				klog.Fatalf("query failed or cancelled: %+v", statusResp.QueryExecution.Status.StateChangeReason)
			}
		}

		time.Sleep(2 * time.Second)
	}

	return
}
