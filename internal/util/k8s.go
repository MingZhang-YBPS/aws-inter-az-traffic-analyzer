package util

import v1 "k8s.io/api/core/v1"

func GetPodAppName(pod *v1.Pod) string {
	if pod == nil {
		return ""
	}

	for _, key := range []string{"app.kubernetes.io/name", "app"} {
		if v, ok := pod.Labels[key]; ok && v != "" {
			return v
		}
	}

	return pod.Name
}

func GetNodeAZ(node *v1.Node) string {
	if node == nil {
		return ""
	}

	if len(node.Labels[NodeAZLabel]) > 0 {
		return node.Labels[NodeAZLabel]
	}
	return "<none>"
}
