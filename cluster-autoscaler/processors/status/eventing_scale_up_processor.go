/*
Copyright 2018 The Kubernetes Authors.

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

package status

import (
	ctx "context"
	"fmt"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_client "k8s.io/client-go/kubernetes"
	"strings"

	"k8s.io/autoscaler/cluster-autoscaler/context"
)

// EventingScaleUpStatusProcessor processes the state of the cluster after
// a scale-up by emitting relevant events for pods depending on their post
// scale-up status.
type EventingScaleUpStatusProcessor struct{}

// Process processes the state of the cluster after a scale-up by emitting
// relevant events for pods depending on their post scale-up status.
func (p *EventingScaleUpStatusProcessor) Process(context *context.AutoscalingContext, status *ScaleUpStatus, kubeclient kube_client.Interface) {
	//fmt.Println("test test")
	//fmt.Println("PodsRemainUnschedulable are: ")
	//for _, pod := range status.PodsRemainUnschedulable {
	//	fmt.Println(pod.Pod.Name)
	//}
	for _, pod := range status.PodsRemainUnschedulable {
		events, _ := kubeclient.CoreV1().Events(pod.Pod.Namespace).List(ctx.TODO(), metav1.ListOptions{FieldSelector: "involvedObject.name=" + pod.Pod.Name, TypeMeta: metav1.TypeMeta{Kind: "Pod"}})
		//fmt.Println("first event of ", pod.Pod.Name, " is: ", events.Items[0].Message)

		if strings.Contains(events.Items[0].Message, "Insufficient") == false {
			context.Recorder.Event(pod.Pod, apiv1.EventTypeNormal, "NotTriggerScaleUp",
				fmt.Sprintf("pod didn't trigger scale-up"))
		} else {
			context.Recorder.Event(pod.Pod, apiv1.EventTypeNormal, "TriggerScaleUp",
				fmt.Sprintf("pod trigger scale-up"))
		}
	}
}

// CleanUp cleans up the processor's internal structures.
func (p *EventingScaleUpStatusProcessor) CleanUp() {
}

// ReasonsMessage aggregates reasons from NoScaleUpInfos.
func ReasonsMessage(noScaleUpInfo NoScaleUpInfo) string {
	messages := []string{}
	aggregated := map[string]int{}
	for _, reasons := range noScaleUpInfo.RejectedNodeGroups {
		//if nodeGroup, present := consideredNodeGroups[nodeGroupId]; !present || !nodeGroup.Exist() {
		//	continue
		//}

		for _, reason := range reasons.Reasons() {
			aggregated[reason]++
		}
	}

	for _, reasons := range noScaleUpInfo.SkippedNodeGroups {
		//if nodeGroup, present := consideredNodeGroups[nodeGroupId]; !present || !nodeGroup.Exist() {
		//	continue
		//}

		for _, reason := range reasons.Reasons() {
			aggregated[reason]++
		}
	}

	for msg, count := range aggregated {
		messages = append(messages, fmt.Sprintf("%d %s", count, msg))
	}
	return strings.Join(messages, ", ")
}
