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

package nodes

import (
	apiv1 "k8s.io/api/core/v1"
	kube_client "k8s.io/client-go/kubernetes"

	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

// ScaleDownNodeProcessor contains methods to get harbor and scale down candidate nodes
type ScaleDownNodeProcessor interface {
	// GetPodDestinationCandidates returns nodes that potentially could act as destinations for pods
	// that would become unscheduled after a scale down.
	GetPodDestinationCandidates(*context.AutoscalingContext, []*apiv1.Node) ([]*apiv1.Node, errors.AutoscalerError)
	// GetScaleDownCandidates returns nodes that potentially could be scaled down.
	GetScaleDownCandidates(*context.AutoscalingContext, []*apiv1.Node, kube_client.Interface) ([]*apiv1.Node, errors.AutoscalerError)
	// CleanUp is called at CA termination
	CleanUp()
}

// ScaleDownSetProcessor contains a method to select nodes for deletion
type ScaleDownSetProcessor interface {
	// GetNodesToRemove selects up to maxCount nodes for deletion
	GetNodesToRemove(ctx *context.AutoscalingContext, candidates []simulator.NodeToBeRemoved, maxCount int) []simulator.NodeToBeRemoved
	// CleanUp is called at CA termination
	CleanUp()
}
