package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/aws/smithy-go/logging"
	log "github.com/sirupsen/logrus"
	"os"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/MingZhang-YBPS/aws-inter-az-traffic-analyzer/internal/util"
)

func main() {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(os.Getenv("AWS_REGION")),
		config.WithLogger(logging.LoggerFunc(func(classification logging.Classification, format string, v ...interface{}) {
			// your custom logging
			log.WithField("process", "extractor-main").Debug(v...)
		})),
	)
	if err != nil {
		klog.Fatalf("failed to load AWS config: %v", err)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Error loading in-cluster config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create clientset: %v", err)
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Fatalf("Failed to list nodes: %v", err)
	}

	pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Fatalf("Failed to list pods: %v", err)
	}

	podsInfo := getPodInfo(pods, getNodeAZMapping(nodes))

	cvsFileName := ""
	if cvsFileName, err = createPodMetadataCSV(podsInfo); err != nil {
		klog.Fatalf("Failed to create pod metadata CSV: %v", err)
	}

	if err = util.PutCSVToS3(ctx, cfg,
		util.EKSPodMetadataBucketName,
		getCSVFileS3Key(cvsFileName),
		getCSVFilePath(cvsFileName)); err != nil {
		klog.Errorf("Failed to upload pod metadata CSV: %v", err)
	}
}

func getCSVFileS3Key(fileName string) string {
	return os.Getenv("CLUSTER") + "/" + fileName
}

func getCSVFilePath(fileName string) string {
	return "/tmp/" + fileName
}

func createPodMetadataCSV(podsInfo [][]string) (string, error) {
	fileName := fmt.Sprintf("pods_metadata-%s.csv", time.Now().Format("20060102150405"))
	file, err := os.Create(getCSVFilePath(fileName))
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"uid", "name", "ip", "app", "creation_time", "node", "az"}
	if err = writer.Write(header); err != nil {
		return "", err
	}

	for _, podInfo := range podsInfo {
		values := make([]string, 0, len(podInfo))
		for _, v := range podInfo {
			values = append(values, v)
		}

		if err = writer.Write(values); err != nil {
			return "", err
		}
	}
	klog.Infof("Created pod metadata csv file: %q", fileName)
	return fileName, nil
}

func getNodeAZMapping(nodes *v1.NodeList) map[string]string {
	mapping := make(map[string]string)
	for _, node := range nodes.Items {
		mapping[node.Name] = util.GetNodeAZ(&node)
	}
	return mapping
}

func getPodInfo(pods *v1.PodList, nodeAZMapping map[string]string) [][]string {
	var podsInfo [][]string
	for _, pod := range pods.Items {
		// 排除所有使用主机IP的pod
		if pod.Status.PodIP != pod.Status.HostIP {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == v1.PodReady && cond.Status == v1.ConditionTrue {
					podInfo := []string{
						string(pod.UID),
						pod.Name,
						pod.Status.PodIP,
						util.GetPodAppName(&pod),
						pod.CreationTimestamp.Format(time.RFC3339),
						pod.Spec.NodeName,
						nodeAZMapping[pod.Spec.NodeName],
					}
					podsInfo = append(podsInfo, podInfo)
				}
			}
		}
	}
	return podsInfo
}
