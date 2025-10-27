package util

const VPCFlowLogBucketName = "vpc-flow-logs-bucket"
const EKSPodMetadataBucketName = "pod-state-bucket"
const AnthenaResultBucketName = "inter-az-traffic-athena-results"
const FlowLogsFormat = "${az-id} ${flow-direction} ${pkt-srcaddr} ${pkt-dstaddr} ${start} ${bytes}"
const NodeAZLabel = "topology.kubernetes.io/zone"
