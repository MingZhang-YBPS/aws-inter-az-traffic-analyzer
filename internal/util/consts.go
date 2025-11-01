package util

import "os"

var VPCFlowLogBucketName = "vpc-flow-logs-bucket-" + os.Getenv("MY_ACCOUNT")
var EKSPodMetadataBucketName = "pod-state-bucket-" + os.Getenv("MY_ACCOUNT")
var AthenaResultBucketName = "inter-az-traffic-results-" + os.Getenv("MY_ACCOUNT")

const FlowLogsFormat = "${az-id} ${flow-direction} ${pkt-srcaddr} ${pkt-dstaddr} ${start} ${bytes}"
const NodeAZLabel = "topology.kubernetes.io/zone"

const AnalyzerJobLabel = "analyzer"
const AnalyzerReportLocationAnnotation = "interaztraffic.report.k8s.aws/location"
