/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	ctx "context"
	"fmt"
	apiv1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"strconv"
)

//// GetNodeGroupSizeMap return a map of node group id and its target size
//func GetNodeGroupSizeMap(cloudProvider cloudprovider.CloudProvider) map[string]int {
//	nodeGroupSize := make(map[string]int)
//	for _, nodeGroup := range cloudProvider.NodeGroups() {
//		size, err := nodeGroup.TargetSize()
//		if err != nil {
//			klog.Errorf("Error while checking node group size %s: %v", nodeGroup.Id(), err)
//			continue
//		}
//		nodeGroupSize[nodeGroup.Id()] = size
//	}
//	return nodeGroupSize
//}

// Get min size group
func GetMinSizeNodeGroup(kubeclient kube_client.Interface) int {
	var minSizeNodeGroup int
	configmaps, err := kubeclient.CoreV1().ConfigMaps("kube-system").Get(ctx.Background(), "autoscaling-configmap", metav1.GetOptions{})
	if err != nil {
		fmt.Println("cannot get information from autoscaling configmap")
		klog.Fatalf("Failed to get information of autoscaling configmap: %v", err)
	}
	for k, v := range configmaps.Data {
		if k == "min_node_group_size" {
			value, err := strconv.Atoi(v)
			if err != nil {
				klog.Fatalf("Failed to convert string to integer: %v", err)
			}
			minSizeNodeGroup = value
		}
	}
	return minSizeNodeGroup
}

// Get max size group
func GetMaxSizeNodeGroup(kubeclient kube_client.Interface) int {
	var maxSizeNodeGroup int
	configmaps, err := kubeclient.CoreV1().ConfigMaps("kube-system").Get(ctx.Background(), "autoscaling-configmap", metav1.GetOptions{})
	if err != nil {
		fmt.Println("cannot get information from autoscaling configmap")
		klog.Fatalf("Failed to get information of autoscaling configmap: %v", err)
	}
	for k, v := range configmaps.Data {
		if k == "max_node_group_size" {
			value, err := strconv.Atoi(v)
			if err != nil {
				klog.Fatalf("Failed to convert string to integer: %v", err)
			}
			maxSizeNodeGroup = value
		}
	}
	return maxSizeNodeGroup
}

// Get access token of FPTCloud
func GetAccessToken(kubeclient kube_client.Interface) string {
	var accessToken string
	secret, err := kubeclient.CoreV1().Secrets("kube-system").Get(ctx.Background(), "fke-secret", metav1.GetOptions{})
	if err != nil {
		fmt.Println("cannot get information from fke secret")
		klog.Fatalf("Failed to get information of fke secret: %v", err)
	}
	for k, v := range secret.Data {
		if k == "access_token" {
			accessToken = string(v)
		}
	}
	return accessToken
}

// Get vpc_id of customer
func GetVPCId(kubeclient kube_client.Interface) string {
	var vpcID string
	secret, err := kubeclient.CoreV1().Secrets("kube-system").Get(ctx.Background(), "fke-secret", metav1.GetOptions{})
	if err != nil {
		fmt.Println("cannot get information from fke secret")
		klog.Fatalf("Failed to get information of fke secret: %v", err)
	}
	for k, v := range secret.Data {
		if k == "vpc_id" {
			vpcID = string(v)
		}
	}
	return vpcID
}

// FilterOutNodes filters out nodesToFilterOut from nodes
func FilterOutNodes(nodes []*apiv1.Node, nodesToFilterOut []*apiv1.Node) []*apiv1.Node {
	var filtered []*apiv1.Node
	for _, node := range nodes {
		found := false
		for _, nodeToFilter := range nodesToFilterOut {
			if nodeToFilter.Name == node.Name {
				found = true
			}
		}
		if !found {
			filtered = append(filtered, node)
		}
	}

	return filtered
}

// PodSpecSemanticallyEqual returns true if two pod specs are similar after dropping
// the fields we don't care about
// Due to the generated suffixes, a strict DeepEquals check will fail and generate
// an equivalence group per pod which is undesirable.
// Projected volumes do not impact scheduling so we should ignore them
func PodSpecSemanticallyEqual(p1 apiv1.PodSpec, p2 apiv1.PodSpec) bool {
	p1Spec := sanitizeProjectedVolumesAndMounts(p1)
	p2Spec := sanitizeProjectedVolumesAndMounts(p2)
	return apiequality.Semantic.DeepEqual(p1Spec, p2Spec)
}

// sanitizeProjectedVolumesAndMounts returns a pod spec with projected volumes
// and their mounts removed
func sanitizeProjectedVolumesAndMounts(podSpec apiv1.PodSpec) apiv1.PodSpec {
	projectedVolumeNames := map[string]bool{}
	var volumes []apiv1.Volume
	for _, v := range podSpec.Volumes {
		if v.Projected == nil {
			volumes = append(volumes, v)
		} else {
			projectedVolumeNames[v.Name] = true
		}
	}
	podSpec.Volumes = volumes

	for i := range podSpec.Containers {
		var volumeMounts []apiv1.VolumeMount
		for _, mount := range podSpec.Containers[i].VolumeMounts {
			if ok := projectedVolumeNames[mount.Name]; !ok {
				volumeMounts = append(volumeMounts, mount)
			}
		}
		podSpec.Containers[i].VolumeMounts = volumeMounts
	}
	return podSpec
}

// Get cluster_id of K8S cluster
func GetClusterID(kubeclient kube_client.Interface) string {
	var clusterID string
	secret, err := kubeclient.CoreV1().Secrets("kube-system").Get(ctx.Background(), "fke-secret", metav1.GetOptions{})
	if err != nil {
		fmt.Println("cannot get information from fke secret")
		klog.Fatalf("Failed to get information of fke secret: %v", err)
	}
	for k, v := range secret.Data {
		if k == "cluster_id" {
			clusterID = string(v)
		}
	}
	return clusterID
}
