package main

import (
	"context"
	"encoding/csv"
	"fmt"
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

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		klog.Fatalf("failed to load AWS config: %v", err)
		os.Exit(1)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Error loading in-cluster config: %v", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create clientset: %v", err)
		os.Exit(1)
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Fatalf("Failed to list nodes: %v", err)
		os.Exit(1)
	}

	pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Fatalf("Failed to list pods: %v", err)
		os.Exit(1)
	}

	podsInfo := getPodInfo(pods, getNodeAZMapping(nodes))

	cvsFileName := ""
	if cvsFileName, err = createPodMetadataCSV(podsInfo); err != nil {
		klog.Errorf("Failed to create pod metadata CSV: %v", err)
		os.Exit(1)
	}

	if err = util.UploadFileToS3(ctx, cfg,
		util.EKSPodMetadataBucketName,
		os.Getenv("vpcId")+"/"+cvsFileName,
		getCSVFilePath(cvsFileName)); err != nil {
		klog.Errorf("Failed to upload pod metadata CSV: %v", err)
	}
}

func getCSVFilePath(fileName string) string {
	return "/tmp/" + fileName
}

func createPodMetadataCSV(podsInfo []map[string]string) (string, error) {
	fileName := fmt.Sprintf("pods_metadata-%s.csv", time.Now().Format("20060102150405"))
	file, err := os.Create(getCSVFilePath(fileName))
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"name", "ip", "app", "creation_time", "node", "az"}
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

func getPodInfo(pods *v1.PodList, nodeAZMapping map[string]string) []map[string]string {
	podsInfo := make([]map[string]string, 0)
	for _, pod := range pods.Items {
		for _, cond := range pod.Status.Conditions {
			if cond.Type == v1.PodReady && cond.Status == v1.ConditionTrue {
				podsInfo = append(podsInfo, map[string]string{
					"name":          pod.Name,
					"ip":            pod.Status.PodIP,
					"app":           util.GetPodAppName(&pod),
					"creation_time": pod.CreationTimestamp.Format(time.RFC3339),
					"node":          pod.Spec.NodeName,
					"az":            nodeAZMapping[pod.Spec.NodeName],
				})
			}
		}
	}
	return podsInfo
}
