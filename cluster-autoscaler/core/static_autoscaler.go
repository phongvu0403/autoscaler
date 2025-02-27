/*
Copyright 2016 The Kubernetes Authors.

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

package core

import (
	ctx "context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"log"
	"strconv"
	"strings"
	"time"

	kube_client "k8s.io/client-go/kubernetes"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/utils"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	core_utils "k8s.io/autoscaler/cluster-autoscaler/core/utils"
	"k8s.io/autoscaler/cluster-autoscaler/debuggingsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/metrics"
	ca_processors "k8s.io/autoscaler/cluster-autoscaler/processors"
	"k8s.io/autoscaler/cluster-autoscaler/processors/status"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/utils/deletetaint"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/autoscaler/cluster-autoscaler/utils/tpu"

	klog "k8s.io/klog/v2"
)

const (
	// How old the oldest unschedulable pod should be before starting scale up.
	unschedulablePodTimeBuffer = 2 * time.Second
	// How old the oldest unschedulable pod with GPU should be before starting scale up.
	// The idea is that nodes with GPU are very expensive and we're ready to sacrifice
	// a bit more latency to wait for more pods and make a more informed scale-up decision.
	unschedulablePodWithGpuTimeBuffer = 30 * time.Second

	// NodeUpcomingAnnotation is an annotation CA adds to nodes which are upcoming.
	NodeUpcomingAnnotation = "cluster-autoscaler.k8s.io/upcoming-node"
)

// StaticAutoscaler is an autoscaler which has all the core functionality of a CA but without the reconfiguration feature
type StaticAutoscaler struct {
	// AutoscalingContext consists of validated settings and options for this autoscaler
	*context.AutoscalingContext
	// ClusterState for maintaining the state of cluster nodes.
	clusterStateRegistry    *clusterstate.ClusterStateRegistry
	lastScaleUpTime         time.Time
	lastScaleDownDeleteTime time.Time
	lastScaleDownFailTime   time.Time
	scaleDown               *ScaleDown
	processors              *ca_processors.AutoscalingProcessors
	processorCallbacks      *staticAutoscalerProcessorCallbacks
	initialized             bool
	ignoredTaints           taints.TaintKeySet
}

type staticAutoscalerProcessorCallbacks struct {
	disableScaleDownForLoop bool
	extraValues             map[string]interface{}
	scaleDown               *ScaleDown
}

func (callbacks *staticAutoscalerProcessorCallbacks) ResetUnneededNodes() {
	callbacks.scaleDown.CleanUpUnneededNodes()
}

func newStaticAutoscalerProcessorCallbacks() *staticAutoscalerProcessorCallbacks {
	callbacks := &staticAutoscalerProcessorCallbacks{}
	callbacks.reset()
	return callbacks
}

func (callbacks *staticAutoscalerProcessorCallbacks) DisableScaleDownForLoop() {
	callbacks.disableScaleDownForLoop = true
}

func (callbacks *staticAutoscalerProcessorCallbacks) SetExtraValue(key string, value interface{}) {
	callbacks.extraValues[key] = value
}

func (callbacks *staticAutoscalerProcessorCallbacks) GetExtraValue(key string) (value interface{}, found bool) {
	value, found = callbacks.extraValues[key]
	return
}

func (callbacks *staticAutoscalerProcessorCallbacks) reset() {
	callbacks.disableScaleDownForLoop = false
	callbacks.extraValues = make(map[string]interface{})
}

// NewStaticAutoscaler creates an instance of Autoscaler filled with provided parameters
func NewStaticAutoscaler(
	opts config.AutoscalingOptions,
	predicateChecker simulator.PredicateChecker,
	clusterSnapshot simulator.ClusterSnapshot,
	autoscalingKubeClients *context.AutoscalingKubeClients,
	processors *ca_processors.AutoscalingProcessors,
	// cloudProvider cloudprovider.CloudProvider,
	expanderStrategy expander.Strategy,
	estimatorBuilder estimator.EstimatorBuilder,
	//backoff backoff.Backoff,
	debuggingSnapshotter debuggingsnapshot.DebuggingSnapshotter) *StaticAutoscaler {

	processorCallbacks := newStaticAutoscalerProcessorCallbacks()
	autoscalingContext := context.NewAutoscalingContext(
		opts,
		predicateChecker,
		clusterSnapshot,
		autoscalingKubeClients,
		// cloudProvider,
		expanderStrategy,
		estimatorBuilder,
		processorCallbacks,
		debuggingSnapshotter)

	clusterStateConfig := clusterstate.ClusterStateRegistryConfig{
		MaxTotalUnreadyPercentage: opts.MaxTotalUnreadyPercentage,
		OkTotalUnreadyCount:       opts.OkTotalUnreadyCount,
		MaxNodeProvisionTime:      opts.MaxNodeProvisionTime,
	}

	ignoredTaints := make(taints.TaintKeySet)
	for _, taintKey := range opts.IgnoredTaints {
		klog.V(4).Infof("Ignoring taint %s on all NodeGroups", taintKey)
		ignoredTaints[taintKey] = true
	}

	clusterStateRegistry := clusterstate.NewClusterStateRegistry(clusterStateConfig, autoscalingContext.LogRecorder)

	scaleDown := NewScaleDown(autoscalingContext, processors, clusterStateRegistry)
	processorCallbacks.scaleDown = scaleDown

	// Set the initial scale times to be less than the start time so as to
	// not start in cooldown mode.
	initialScaleTime := time.Now().Add(-time.Hour)
	return &StaticAutoscaler{
		AutoscalingContext:      autoscalingContext,
		lastScaleUpTime:         initialScaleTime,
		lastScaleDownDeleteTime: initialScaleTime,
		lastScaleDownFailTime:   initialScaleTime,
		scaleDown:               scaleDown,
		processors:              processors,
		processorCallbacks:      processorCallbacks,
		clusterStateRegistry:    clusterStateRegistry,
		ignoredTaints:           ignoredTaints,
	}
}

// Start starts components running in background.
func (a *StaticAutoscaler) Start() error {
	a.clusterStateRegistry.Start()
	return nil
}

// cleanUpIfRequired removes ToBeDeleted taints added by a previous run of CA
// the taints are removed only once per runtime
func (a *StaticAutoscaler) cleanUpIfRequired() {
	if a.initialized {
		return
	}

	// CA can die at any time. Removing taints that might have been left from the previous run.
	if readyNodes, err := a.ReadyNodeLister().List(); err != nil {
		klog.Errorf("Failed to list ready nodes, not cleaning up taints: %v", err)
	} else {
		deletetaint.CleanAllToBeDeleted(readyNodes,
			a.AutoscalingContext.ClientSet, a.Recorder, a.CordonNodeBeforeTerminate)
		if a.AutoscalingContext.AutoscalingOptions.MaxBulkSoftTaintCount == 0 {
			// Clean old taints if soft taints handling is disabled
			deletetaint.CleanAllDeletionCandidates(readyNodes,
				a.AutoscalingContext.ClientSet, a.Recorder)
		}
	}
	a.initialized = true
}

func (a *StaticAutoscaler) initializeClusterSnapshot(nodes []*apiv1.Node, scheduledPods []*apiv1.Pod) errors.AutoscalerError {
	a.ClusterSnapshot.Clear()

	knownNodes := make(map[string]bool)
	for _, node := range nodes {
		if err := a.ClusterSnapshot.AddNode(node); err != nil {
			klog.Errorf("Failed to add node %s to cluster snapshot: %v", node.Name, err)
			return errors.ToAutoscalerError(errors.InternalError, err)
		}
		knownNodes[node.Name] = true
	}
	for _, pod := range scheduledPods {
		if knownNodes[pod.Spec.NodeName] {
			if err := a.ClusterSnapshot.AddPod(pod, pod.Spec.NodeName); err != nil {
				klog.Errorf("Failed to add pod %s scheduled to node %s to cluster snapshot: %v", pod.Name, pod.Spec.NodeName, err)
				return errors.ToAutoscalerError(errors.InternalError, err)
			}
		}
	}
	return nil
}

// RunOnce iterates over node groups and scales them up/down if necessary
func (a *StaticAutoscaler) RunOnce(currentTime time.Time, kubeclient kube_client.Interface, vpcID string,
	accessToken string, idCluster string, clusterIDPortal string, env string) errors.AutoscalerError {
	a.cleanUpIfRequired()
	a.processorCallbacks.reset()
	a.clusterStateRegistry.PeriodicCleanup()
	a.DebuggingSnapshotter.StartDataCollection()
	defer a.DebuggingSnapshotter.Flush()

	unschedulablePodLister := a.UnschedulablePodLister()

	//fmt.Println("unschedulablePodLister is")
	//fmt.Println(unschedulablePodLister.List())

	scheduledPodLister := a.ScheduledPodLister()
	//fmt.Println("scheduledPodLister is")
	//fmt.Println(scheduledPodLister.List())
	pdbLister := a.PodDisruptionBudgetLister()

	//fmt.Println("pdbLister is")
	//fmt.Println(pdbLister.List())

	scaleDown := a.scaleDown

	//fmt.Println("unneededNodesList is")
	//for _, node := range scaleDown.unneededNodesList {
	//	fmt.Println(node.Name)
	//}

	autoscalingContext := a.AutoscalingContext

	klog.V(4).Info("Starting main loop")

	stateUpdateStart := time.Now()

	//// Get nodes and pods currently living on cluster
	allNodes, readyNodes, typedErr := a.obtainNodeLists()

	domainAPI := core_utils.GetDomainApiConformEnv(env)

	workerNodeNameList := make([]string, 0, len(allNodes))
	for _, node := range allNodes {
		if strings.Contains(node.Name, "worker") {
			workerNodeNameList = append(workerNodeNameList, node.Name)
		}
	}
	numberWorkerNode := len(workerNodeNameList)
	var workerNameToRemove string

	if numberWorkerNode < core_utils.GetMinSizeNodeGroup(kubeclient) {
		workerCountNeedToScaledUp := core_utils.GetMinSizeNodeGroup(kubeclient) - numberWorkerNode
		klog.V(1).Infof("Current worker nodes are less than min node group")
		klog.V(1).Infof("Scaling up %v node", workerCountNeedToScaledUp)
		//fmt.Println("current worker nodes are less than min node group")
		//fmt.Println("scaling up ", workerCountNeedToScaledUp, " node")
		core_utils.PerformScaleUp(domainAPI, vpcID, accessToken, workerCountNeedToScaledUp, idCluster, clusterIDPortal)
		for {
			time.Sleep(30 * time.Second)
			isSucceededStatus := core_utils.CheckStatusCluster(domainAPI, vpcID, accessToken, clusterIDPortal)
			//fmt.Println("status cluster is SCALING")
			klog.V(1).Infof("Status of cluster is SCALING")
			if isSucceededStatus == true {
				//fmt.Println("status cluster is SUCCEEDED")
				klog.V(1).Infof("Status of cluster is SUCCEEDED")
				break
			}
			isErrorStatus := core_utils.CheckErrorStatusCluster(domainAPI, vpcID, accessToken, clusterIDPortal)
			if isErrorStatus == true {
				core_utils.PerformScaleUp(domainAPI, vpcID, accessToken, workerCountNeedToScaledUp, idCluster, clusterIDPortal)
				for {
					time.Sleep(30 * time.Second)
					if core_utils.CheckStatusCluster(domainAPI, vpcID, accessToken, clusterIDPortal) == true {
						break
					}
				}
				break
			}
		}
	} else if numberWorkerNode > core_utils.GetMaxSizeNodeGroup(kubeclient) {
		for _, nodeName := range workerNodeNameList {
			if strings.HasSuffix(nodeName, "worker"+strconv.Itoa(len(workerNodeNameList))) {
				workerNameToRemove = nodeName
			}
		}
		workerCountNeedToScaledDown := numberWorkerNode - core_utils.GetMaxSizeNodeGroup(kubeclient)
		klog.V(1).Infof("Current worker nodes are greater than max node group")
		klog.V(1).Infof("Scaling down %v node", workerCountNeedToScaledDown)
		//fmt.Println("current worker nodes are greater than max node group")
		//fmt.Println("scaling down ", workerCountNeedToScaledDown, " node")
		klog.V(1).Infof("Scaling down node %s", workerNameToRemove)
		if !checkWorkerNodeCanBeRemove(kubeclient, workerNameToRemove) {
			klog.V(1).Infof("Cannot perform scale down action")
			return nil
		}
		core_utils.PerformScaleDown(domainAPI, vpcID, accessToken, workerCountNeedToScaledDown, idCluster, clusterIDPortal)
		for {
			time.Sleep(30 * time.Second)
			isSucceededStatus := core_utils.CheckStatusCluster(domainAPI, vpcID, accessToken, clusterIDPortal)
			//fmt.Println("status cluster is SCALING")
			klog.V(1).Infof("Status of cluster is SCALING")
			if isSucceededStatus == true {
				//fmt.Println("status cluster is SUCCEEDED")
				klog.V(1).Infof("Status of cluster is SUCCEEDED")
				break
			}
			isErrorStatus := core_utils.CheckErrorStatusCluster(domainAPI, vpcID, accessToken, clusterIDPortal)
			if isErrorStatus == true {
				core_utils.PerformScaleDown(domainAPI, vpcID, accessToken, workerCountNeedToScaledDown, idCluster, clusterIDPortal)
				for {
					time.Sleep(30 * time.Second)
					if core_utils.CheckStatusCluster(domainAPI, vpcID, accessToken, clusterIDPortal) == true {
						break
					}
				}
				break
			}
		}
	}

	allNodes, readyNodes, typedErr = a.obtainNodeLists()

	//fmt.Println("allNodes are")
	//for _, node := range allNodes {
	//	fmt.Println(node.Name)
	//}
	//fmt.Println("readyNodes are")
	//for _, node := range readyNodes {
	//	fmt.Println(node.Name)
	//}
	if typedErr != nil {
		klog.Errorf("Failed to get node list: %v", typedErr)
		return typedErr
	}
	originalScheduledPods, err := scheduledPodLister.List()
	//fmt.Println()
	//fmt.Println("originalScheduledPods are")
	//for _, pod := range originalScheduledPods {
	//	fmt.Println(pod.Name)
	//}
	fmt.Println()
	if err != nil {
		klog.Errorf("Failed to list scheduled pods: %v", err)
		return errors.ToAutoscalerError(errors.ApiCallError, err)
	}

	if abortLoop, err := a.processors.ActionableClusterProcessor.ShouldAbort(
		a.AutoscalingContext, allNodes, readyNodes, currentTime); abortLoop {
		return err
	}

	// Update cluster resource usage metrics
	coresTotal, memoryTotal := calculateCoresMemoryTotal(allNodes, currentTime)
	//fmt.Println("coresTotal is: ", coresTotal)
	//fmt.Println("memoryTotal is: ", memoryTotal)
	metrics.UpdateClusterCPUCurrentCores(coresTotal)
	metrics.UpdateClusterMemoryCurrentBytes(memoryTotal)

	daemonsets, err := a.ListerRegistry.DaemonSetLister().List(labels.Everything())
	fmt.Println()
	//fmt.Println("daemonsets are:")
	//for _, ds := range daemonsets {
	//	fmt.Println(ds.Name)
	//}
	if err != nil {
		klog.Errorf("Failed to get daemonset list: %v", err)
		return errors.ToAutoscalerError(errors.ApiCallError, err)
	}

	// Call CloudProvider.Refresh before any other calls to cloud provider.
	//refreshStart := time.Now()
	//err = a.AutoscalingContext.CloudProvider.Refresh()
	//metrics.UpdateDurationFromStart(metrics.CloudProviderRefresh, refreshStart)
	//if err != nil {
	//	klog.Errorf("Failed to refresh cloud provider config: %v", err)
	//	return errors.ToAutoscalerError(errors.CloudProviderError, err)
	//}

	//Update node groups min/max after cloud provider refresh
	//for _, nodeGroup := range a.AutoscalingContext.CloudProvider.NodeGroups() {
	//	metrics.UpdateNodeGroupMin(nodeGroup.Id(), nodeGroup.MinSize())
	//	metrics.UpdateNodeGroupMax(nodeGroup.Id(), nodeGroup.MaxSize())
	//}

	nonExpendableScheduledPods := core_utils.FilterOutExpendablePods(originalScheduledPods, a.ExpendablePodsPriorityCutoff)
	fmt.Println()
	//fmt.Println("nonExpendableScheduledPods are")
	//for _, pod := range nonExpendableScheduledPods {
	//	fmt.Println(pod.Name)
	//}
	// Initialize cluster state to ClusterSnapshot
	if typedErr := a.initializeClusterSnapshot(allNodes, nonExpendableScheduledPods); typedErr != nil {
		return typedErr.AddPrefix("Initialize ClusterSnapshot")
	}

	//nodeInfosForGroups, autoscalerError := a.processors.TemplateNodeInfoProvider.Process(autoscalingContext, readyNodes, daemonsets, a.ignoredTaints, currentTime)
	//if autoscalerError != nil {
	//	klog.Errorf("Failed to get node infos for groups: %v", autoscalerError)
	//	return autoscalerError.AddPrefix("failed to build node infos for node groups: ")
	//}

	//a.DebuggingSnapshotter.SetTemplateNodes(nodeInfosForGroups)

	//nodeInfosForGroups, err = a.processors.NodeInfoProcessor.Process(autoscalingContext, nodeInfosForGroups)
	//if err != nil {
	//	klog.Errorf("Failed to process nodeInfos: %v", err)
	//	return errors.ToAutoscalerError(errors.InternalError, err)
	//}

	if typedErr := a.updateClusterState(allNodes, currentTime); typedErr != nil {
		klog.Errorf("Failed to update cluster state: %v", typedErr)
		return typedErr
	}
	metrics.UpdateDurationFromStart(metrics.UpdateState, stateUpdateStart)

	scaleUpStatus := &status.ScaleUpStatus{Result: status.ScaleUpNotTried}
	scaleUpStatusProcessorAlreadyCalled := false
	scaleDownStatus := &status.ScaleDownStatus{Result: status.ScaleDownNotTried}
	scaleDownStatusProcessorAlreadyCalled := false

	defer func() {
		// Update status information when the loop is done (regardless of reason)
		if autoscalingContext.WriteStatusConfigMap {
			status := a.clusterStateRegistry.GetStatus(currentTime)
			utils.WriteStatusConfigMap(autoscalingContext.ClientSet, autoscalingContext.ConfigNamespace,
				status.GetReadableString(), a.AutoscalingContext.LogRecorder, a.AutoscalingContext.StatusConfigMapName)
		}

		// This deferred processor execution allows the processors to handle a situation when a scale-(up|down)
		// wasn't even attempted because e.g. the iteration exited earlier.
		if !scaleUpStatusProcessorAlreadyCalled && a.processors != nil && a.processors.ScaleUpStatusProcessor != nil {
			a.processors.ScaleUpStatusProcessor.Process(a.AutoscalingContext, scaleUpStatus, kubeclient)
		}
		if !scaleDownStatusProcessorAlreadyCalled && a.processors != nil && a.processors.ScaleDownStatusProcessor != nil {
			scaleDownStatus.SetUnremovableNodesInfo(scaleDown.unremovableNodeReasons, scaleDown.nodeUtilizationMap)
			a.processors.ScaleDownStatusProcessor.Process(a.AutoscalingContext, scaleDownStatus)
		}

		err := a.processors.AutoscalingStatusProcessor.Process(a.AutoscalingContext, a.clusterStateRegistry, currentTime)
		if err != nil {
			klog.Errorf("AutoscalingStatusProcessor error: %v.", err)
		}
	}()

	//// Check if there are any nodes that failed to register in Kubernetes
	//// master.
	//unregisteredNodes := a.clusterStateRegistry.GetUnregisteredNodes()
	//if len(unregisteredNodes) > 0 {
	//	klog.V(1).Infof("%d unregistered nodes present", len(unregisteredNodes))
	//	removedAny, err := removeOldUnregisteredNodes(unregisteredNodes, autoscalingContext,
	//		a.clusterStateRegistry, currentTime, autoscalingContext.LogRecorder)
	//	// There was a problem with removing unregistered nodes. Retry in the next loop.
	//	if err != nil {
	//		klog.Warningf("Failed to remove unregistered nodes: %v", err)
	//	}
	//	if removedAny {
	//		klog.V(0).Infof("Some unregistered nodes were removed, skipping iteration")
	//		return nil
	//	}
	//}

	if !a.clusterStateRegistry.IsClusterHealthy() {
		klog.Warning("Cluster is not ready for autoscaling")
		scaleDown.CleanUpUnneededNodes()
		autoscalingContext.LogRecorder.Eventf(apiv1.EventTypeWarning, "ClusterUnhealthy", "Cluster is unhealthy")
		return nil
	}

	//if a.deleteCreatedNodesWithErrors() {
	//	klog.V(0).Infof("Some nodes that failed to create were removed, skipping iteration")
	//	return nil
	//}

	//// Check if there has been a constant difference between the number of nodes in k8s and
	//// the number of nodes on the cloud provider side.
	//// TODO: andrewskim - add protection for ready AWS nodes.
	//fixedSomething, err := fixNodeGroupSize(autoscalingContext, a.clusterStateRegistry, currentTime)
	//if err != nil {
	//	klog.Errorf("Failed to fix node group sizes: %v", err)
	//	return errors.ToAutoscalerError(errors.CloudProviderError, err)
	//}
	//if fixedSomething {
	//	klog.V(0).Infof("Some node group target size was fixed, skipping the iteration")
	//	return nil
	//}

	metrics.UpdateLastTime(metrics.Autoscaling, time.Now())

	unschedulablePods, err := unschedulablePodLister.List()
	//fmt.Println("unschedulablePods are: ")
	//for _, pod := range unschedulablePods {
	//	fmt.Println(pod.Name)
	//}
	if err != nil {
		klog.Errorf("Failed to list unscheduled pods: %v", err)
		return errors.ToAutoscalerError(errors.ApiCallError, err)
	}
	metrics.UpdateUnschedulablePodsCount(len(unschedulablePods))

	unschedulablePods = tpu.ClearTPURequests(unschedulablePods)

	// todo: move split and append below to separate PodListProcessor
	// Some unschedulable pods can be waiting for lower priority pods preemption so they have nominated node to run.
	// Such pods don't require scale up but should be considered during scale down.
	unschedulablePods, unschedulableWaitingForLowerPriorityPreemption := core_utils.FilterOutExpendableAndSplit(unschedulablePods, allNodes, a.ExpendablePodsPriorityCutoff)

	// modify the snapshot simulating scheduling of pods waiting for preemption.
	// this is not strictly correct as we are not simulating preemption itself but it matches
	// CA logic from before migration to scheduler framework. So let's keep it for now
	for _, p := range unschedulableWaitingForLowerPriorityPreemption {
		if err := a.ClusterSnapshot.AddPod(p, p.Status.NominatedNodeName); err != nil {
			klog.Errorf("Failed to update snapshot with pod %s waiting for preemption", err)
			return errors.ToAutoscalerError(errors.InternalError, err)
		}
	}

	//// add upcoming nodes to ClusterSnapshot
	//upcomingNodes := getUpcomingNodeInfos(a.clusterStateRegistry, nodeInfosForGroups)
	//for _, upcomingNode := range upcomingNodes {
	//	var pods []*apiv1.Pod
	//	for _, podInfo := range upcomingNode.Pods {
	//		pods = append(pods, podInfo.Pod)
	//	}
	//	err = a.ClusterSnapshot.AddNodeWithPods(upcomingNode.Node(), pods)
	//	if err != nil {
	//		klog.Errorf("Failed to add upcoming node %s to cluster snapshot: %v", upcomingNode.Node().Name, err)
	//		return errors.ToAutoscalerError(errors.InternalError, err)
	//	}
	//}

	l, err := a.ClusterSnapshot.NodeInfos().List()
	//fmt.Println()
	//fmt.Println("Nodes in Cluster Snapshot are: ")
	//for _, list := range l {
	//	fmt.Println(list.Node().Name)
	//}
	if err != nil {
		klog.Errorf("Unable to fetch ClusterNode List for Debugging Snapshot, %v", err)
	} else {
		a.AutoscalingContext.DebuggingSnapshotter.SetClusterNodes(l)
	}

	unschedulablePodsToHelp, _ := a.processors.PodListProcessor.Process(a.AutoscalingContext, unschedulablePods)
	//fmt.Println()
	//fmt.Println("unschedulablePodsToHelp are: ")
	//for _, pod := range unschedulablePodsToHelp {
	//	fmt.Println(pod.Name)
	//}

	// finally, filter out pods that are too "young" to safely be considered for a scale-up (delay is configurable)
	unschedulablePodsToHelp = a.filterOutYoungPods(unschedulablePodsToHelp, currentTime)
	//fmt.Println()
	//fmt.Println("filter out unschedulablePodsToHelp are: ")
	//for _, pod := range unschedulablePodsToHelp {
	//	fmt.Println(pod.Name)
	//}
	//fmt.Println()
	//fmt.Println("Max node total is: ", core_utils.GetMaxSizeNodeGroup(kubeclient))
	//fmt.Println("Min node total is: ", core_utils.GetMinSizeNodeGroup(kubeclient))
	//fmt.Println("Access Token FPTCloud is: ", core_utils.GetAccessToken(kubeclient))
	//fmt.Println("VPC ID of customer is: ", core_utils.GetVPCId(kubeclient))
	if len(unschedulablePodsToHelp) == 0 {
		scaleUpStatus.Result = status.ScaleUpNotNeeded
		klog.V(1).Info("No unschedulable pods")
		klog.V(1).Info("No need Scale up")

		//fmt.Println("No need Scale up")

	} else if allPodsAreNew(unschedulablePodsToHelp, currentTime) {
		// The assumption here is that these pods have been created very recently and probably there
		// is more pods to come. In theory we could check the newest pod time but then if pod were created
		// slowly but at the pace of 1 every 2 seconds then no scale up would be triggered for long time.
		// We also want to skip a real scale down (just like if the pods were handled).
		a.processorCallbacks.DisableScaleDownForLoop()
		scaleUpStatus.Result = status.ScaleUpInCooldown
		klog.V(1).Info("Unschedulable pods are very new, waiting one iteration for more")

		//fmt.Println()
		//fmt.Println("Unschedulable pods are very new, waiting one iteration for more")

	} else {
		scaleUpStart := time.Now()
		//fmt.Println("Start to scale up")
		klog.V(1).Info("Start to scale up")
		metrics.UpdateLastTime(metrics.ScaleUp, scaleUpStart)

		scaleUpStatus, typedErr = ScaleUp(autoscalingContext, a.processors, a.clusterStateRegistry, unschedulablePodsToHelp, readyNodes, daemonsets, a.ignoredTaints, kubeclient, accessToken, vpcID, idCluster, clusterIDPortal, env)

		metrics.UpdateDurationFromStart(metrics.ScaleUp, scaleUpStart)

		if a.processors != nil && a.processors.ScaleUpStatusProcessor != nil {
			a.processors.ScaleUpStatusProcessor.Process(autoscalingContext, scaleUpStatus, kubeclient)
			scaleUpStatusProcessorAlreadyCalled = true
		}

		if typedErr != nil {
			klog.Errorf("Failed to scale up: %v", typedErr)
			return typedErr
		}
		if scaleUpStatus.Result == status.ScaleUpSuccessful {
			a.lastScaleUpTime = currentTime
			// No scale down in this iteration.
			scaleDownStatus.Result = status.ScaleDownInCooldown
			return nil
		}
	}

	//fmt.Println()
	//fmt.Println("ScaleDownEnabled is: ", a.ScaleDownEnabled)

	if a.ScaleDownEnabled {
		pdbs, err := pdbLister.List()

		// fmt.Println()
		// fmt.Println("PDBs are: ")

		// for _, pdb := range pdbs {
		// 	fmt.Println(pdb.Name)
		// }
		if err != nil {
			scaleDownStatus.Result = status.ScaleDownError
			klog.Errorf("Failed to list pod disruption budgets: %v", err)
			return errors.ToAutoscalerError(errors.ApiCallError, err)
		}

		unneededStart := time.Now()

		klog.V(4).Infof("Calculating unneeded nodes")

		scaleDown.CleanUp(currentTime)

		var scaleDownCandidates []*apiv1.Node
		var podDestinations []*apiv1.Node

		// podDestinations and scaleDownCandidates are initialized based on allNodes variable, which contains only
		// registered nodes in cluster.
		// It does not include any upcoming nodes which can be part of clusterSnapshot. As an alternative to using
		// allNodes here, we could use nodes from clusterSnapshot and explicitly filter out upcoming nodes here but it
		// is of little (if any) benefit.

		//fmt.Println()
		//fmt.Println("ScaleDownNodeProcessor is: ")
		//fmt.Println(a.processors.ScaleDownNodeProcessor)

		if a.processors == nil || a.processors.ScaleDownNodeProcessor == nil {

			//fmt.Println()
			//fmt.Println("scaleDownCandidates are allNodes")

			scaleDownCandidates = allNodes
			podDestinations = allNodes
		} else {
			var err errors.AutoscalerError

			//fmt.Println()
			//fmt.Println("GetScaleDownCandidates")

			scaleDownCandidates, err = a.processors.ScaleDownNodeProcessor.GetScaleDownCandidates(
				autoscalingContext, allNodes, kubeclient)
			//fmt.Println()
			//fmt.Println("ScaleDownCandidates are:")
			//for _, node := range scaleDownCandidates {
			//	fmt.Println(node.Name)
			//}
			if err != nil {
				klog.Error(err)
				return err
			}
			podDestinations, err = a.processors.ScaleDownNodeProcessor.GetPodDestinationCandidates(autoscalingContext, allNodes)

			//fmt.Println()
			//fmt.Println("podDestinations are:")
			//for _, node := range podDestinations {
			//	fmt.Println(node.Name)
			//}

			if err != nil {
				klog.Error(err)
				return err
			}
		}

		// We use scheduledPods (not originalScheduledPods) here, so artificial scheduled pods introduced by processors
		// (e.g unscheduled pods with nominated node name) can block scaledown of given node.
		if typedErr := scaleDown.UpdateUnneededNodes(podDestinations, scaleDownCandidates, currentTime, pdbs, kubeclient); typedErr != nil {
			scaleDownStatus.Result = status.ScaleDownError
			klog.Errorf("Failed to scale down: %v", typedErr)
			return typedErr
		}

		metrics.UpdateDurationFromStart(metrics.FindUnneeded, unneededStart)

		if klog.V(4).Enabled() {
			//for key, val := range scaleDown.unneededNodes {
			//	klog.Infof("%s is unneeded since %s duration %s", key, val.String(), currentTime.Sub(val).String())
			//}
		}

		scaleDownInCooldown := a.processorCallbacks.disableScaleDownForLoop ||
			a.lastScaleUpTime.Add(a.ScaleDownDelayAfterAdd).After(currentTime) ||
			a.lastScaleDownFailTime.Add(a.ScaleDownDelayAfterFailure).After(currentTime) ||
			a.lastScaleDownDeleteTime.Add(a.ScaleDownDelayAfterDelete).After(currentTime)

		//fmt.Println()
		//fmt.Println("scaleDownInCooldown is: ", scaleDownInCooldown)
		//fmt.Println("lastScaleUpTime is: ", a.lastScaleUpTime)
		//fmt.Println("lastScaleDownFailTime is: ", a.lastScaleDownFailTime)
		//fmt.Println("lastScaleDownDeleteTime is: ", a.lastScaleDownDeleteTime)

		// In dry run only utilization is updated
		calculateUnneededOnly := scaleDownInCooldown || scaleDown.nodeDeletionTracker.IsNonEmptyNodeDeleteInProgress()

		klog.V(4).Infof("Scale down status: unneededOnly=%v lastScaleUpTime=%s "+
			"lastScaleDownDeleteTime=%v lastScaleDownFailTime=%s scaleDownForbidden=%v "+
			"isDeleteInProgress=%v scaleDownInCooldown=%v",
			calculateUnneededOnly, a.lastScaleUpTime,
			a.lastScaleDownDeleteTime, a.lastScaleDownFailTime, a.processorCallbacks.disableScaleDownForLoop,
			scaleDown.nodeDeletionTracker.IsNonEmptyNodeDeleteInProgress(), scaleDownInCooldown)
		metrics.UpdateScaleDownInCooldown(scaleDownInCooldown)

		if scaleDownInCooldown {
			scaleDownStatus.Result = status.ScaleDownInCooldown
		} else if scaleDown.nodeDeletionTracker.IsNonEmptyNodeDeleteInProgress() {
			scaleDownStatus.Result = status.ScaleDownInProgress
		} else {
			klog.V(4).Infof("Starting scale down")

			//fmt.Println()
			//fmt.Println("starting scale down")

			//// We want to delete unneeded Node Groups only if there was no recent scale up,
			//// and there is no current delete in progress and there was no recent errors.
			//removedNodeGroups, err := a.processors.NodeGroupManager.RemoveUnneededNodeGroups(autoscalingContext)
			//if err != nil {
			//	klog.Errorf("Error while removing unneeded node groups: %v", err)
			//}

			scaleDownStart := time.Now()
			metrics.UpdateLastTime(metrics.ScaleDown, scaleDownStart)
			scaleDownStatus, typedErr := scaleDown.TryToScaleDown(currentTime, pdbs, kubeclient, accessToken, vpcID, idCluster, clusterIDPortal, env)
			metrics.UpdateDurationFromStart(metrics.ScaleDown, scaleDownStart)
			metrics.UpdateUnremovableNodesCount(scaleDown.getUnremovableNodesCount())

			//scaleDownStatus.RemovedNodeGroups = removedNodeGroups

			// fmt.Println()
			// fmt.Println("scale down status is: ", scaleDownStatus.Result)

			if scaleDownStatus.Result == status.ScaleDownNodeDeleteStarted {
				a.lastScaleDownDeleteTime = currentTime
				//a.clusterStateRegistry.Recalculate()
			}

			if (scaleDownStatus.Result == status.ScaleDownNoNodeDeleted ||
				scaleDownStatus.Result == status.ScaleDownNoUnneeded) &&
				a.AutoscalingContext.AutoscalingOptions.MaxBulkSoftTaintCount != 0 {
				scaleDown.SoftTaintUnneededNodes(allNodes)
			}

			if a.processors != nil && a.processors.ScaleDownStatusProcessor != nil {
				scaleDownStatus.SetUnremovableNodesInfo(scaleDown.unremovableNodeReasons, scaleDown.nodeUtilizationMap)
				a.processors.ScaleDownStatusProcessor.Process(autoscalingContext, scaleDownStatus)
				scaleDownStatusProcessorAlreadyCalled = true
			}

			if typedErr != nil {
				klog.Errorf("Failed to scale down: %v", typedErr)
				a.lastScaleDownFailTime = currentTime
				return typedErr
			}
		}
	}
	return nil
}

// Sets the target size of node groups to the current number of nodes in them
// if the difference was constant for a prolonged time. Returns true if managed
// to fix something.
//func fixNodeGroupSize(context *context.AutoscalingContext, clusterStateRegistry *clusterstate.ClusterStateRegistry, currentTime time.Time) (bool, error) {
//	fixed := false
//	for _, nodeGroup := range context.CloudProvider.NodeGroups() {
//		incorrectSize := clusterStateRegistry.GetIncorrectNodeGroupSize(nodeGroup.Id())
//		if incorrectSize == nil {
//			continue
//		}
//		if incorrectSize.FirstObserved.Add(context.MaxNodeProvisionTime).Before(currentTime) {
//			delta := incorrectSize.CurrentSize - incorrectSize.ExpectedSize
//			if delta < 0 {
//				klog.V(0).Infof("Decreasing size of %s, expected=%d current=%d delta=%d", nodeGroup.Id(),
//					incorrectSize.ExpectedSize,
//					incorrectSize.CurrentSize,
//					delta)
//				if err := nodeGroup.DecreaseTargetSize(delta); err != nil {
//					return fixed, fmt.Errorf("failed to decrease %s: %v", nodeGroup.Id(), err)
//				}
//				fixed = true
//			}
//		}
//	}
//	return fixed, nil
//}

// Removes unregistered nodes if needed. Returns true if anything was removed and error if such occurred.
//func removeOldUnregisteredNodes(unregisteredNodes []clusterstate.UnregisteredNode, context *context.AutoscalingContext,
//	csr *clusterstate.ClusterStateRegistry, currentTime time.Time, logRecorder *utils.LogEventRecorder) (bool, error) {
//	removedAny := false
//	for _, unregisteredNode := range unregisteredNodes {
//		if unregisteredNode.UnregisteredSince.Add(context.MaxNodeProvisionTime).Before(currentTime) {
//			klog.V(0).Infof("Removing unregistered node %v", unregisteredNode.Node.Name)
//			nodeGroup, err := context.CloudProvider.NodeGroupForNode(unregisteredNode.Node)
//			if err != nil {
//				klog.Warningf("Failed to get node group for %s: %v", unregisteredNode.Node.Name, err)
//				return removedAny, err
//			}
//			if nodeGroup == nil || reflect.ValueOf(nodeGroup).IsNil() {
//				klog.Warningf("No node group for node %s, skipping", unregisteredNode.Node.Name)
//				continue
//			}
//			size, err := nodeGroup.TargetSize()
//			if err != nil {
//				klog.Warningf("Failed to get node group size; unregisteredNode=%v; nodeGroup=%v; err=%v", unregisteredNode.Node.Name, nodeGroup.Id(), err)
//				continue
//			}
//			if nodeGroup.MinSize() >= size {
//				klog.Warningf("Failed to remove node %s: node group min size reached, skipping unregistered node removal", unregisteredNode.Node.Name)
//				continue
//			}
//			err = nodeGroup.DeleteNodes([]*apiv1.Node{unregisteredNode.Node})
//			csr.InvalidateNodeInstancesCacheEntry(nodeGroup)
//			if err != nil {
//				klog.Warningf("Failed to remove node %s: %v", unregisteredNode.Node.Name, err)
//				logRecorder.Eventf(apiv1.EventTypeWarning, "DeleteUnregisteredFailed",
//					"Failed to remove node %s: %v", unregisteredNode.Node.Name, err)
//				return removedAny, err
//			}
//			logRecorder.Eventf(apiv1.EventTypeNormal, "DeleteUnregistered",
//				"Removed unregistered node %v", unregisteredNode.Node.Name)
//			metrics.RegisterOldUnregisteredNodesRemoved(1)
//			removedAny = true
//		}
//	}
//	return removedAny, nil
//}

//func (a *StaticAutoscaler) deleteCreatedNodesWithErrors() bool {
//	// We always schedule deleting of incoming errornous nodes
//	// TODO[lukaszos] Consider adding logic to not retry delete every loop iteration
//	nodes := a.clusterStateRegistry.GetCreatedNodesWithErrors()
//
//	nodeGroups := a.nodeGroupsById()
//	nodesToBeDeletedByNodeGroupId := make(map[string][]*apiv1.Node)
//
//	for _, node := range nodes {
//		nodeGroup, err := a.CloudProvider.NodeGroupForNode(node)
//		if err != nil {
//			id := "<nil>"
//			if node != nil {
//				id = node.Spec.ProviderID
//			}
//			klog.Warningf("Cannot determine nodeGroup for node %v; %v", id, err)
//			continue
//		}
//		nodesToBeDeletedByNodeGroupId[nodeGroup.Id()] = append(nodesToBeDeletedByNodeGroupId[nodeGroup.Id()], node)
//	}
//
//	deletedAny := false
//
//	for nodeGroupId, nodesToBeDeleted := range nodesToBeDeletedByNodeGroupId {
//		var err error
//		klog.V(1).Infof("Deleting %v from %v node group because of create errors", len(nodesToBeDeleted), nodeGroupId)
//
//		nodeGroup := nodeGroups[nodeGroupId]
//		if nodeGroup == nil {
//			err = fmt.Errorf("node group %s not found", nodeGroupId)
//		} else {
//			err = nodeGroup.DeleteNodes(nodesToBeDeleted)
//		}
//
//		if err != nil {
//			klog.Warningf("Error while trying to delete nodes from %v: %v", nodeGroupId, err)
//		}
//
//		deletedAny = deletedAny || err == nil
//		a.clusterStateRegistry.InvalidateNodeInstancesCacheEntry(nodeGroup)
//	}
//
//	return deletedAny
//}

//func (a *StaticAutoscaler) nodeGroupsById() map[string]cloudprovider.NodeGroup {
//	nodeGroups := make(map[string]cloudprovider.NodeGroup)
//	for _, nodeGroup := range a.CloudProvider.NodeGroups() {
//		nodeGroups[nodeGroup.Id()] = nodeGroup
//	}
//	return nodeGroups
//}

// don't consider pods newer than newPodScaleUpDelay seconds old as unschedulable
func (a *StaticAutoscaler) filterOutYoungPods(allUnschedulablePods []*apiv1.Pod, currentTime time.Time) []*apiv1.Pod {
	var oldUnschedulablePods []*apiv1.Pod
	newPodScaleUpDelay := a.AutoscalingOptions.NewPodScaleUpDelay
	for _, pod := range allUnschedulablePods {
		podAge := currentTime.Sub(pod.CreationTimestamp.Time)
		if podAge > newPodScaleUpDelay {
			oldUnschedulablePods = append(oldUnschedulablePods, pod)
		} else {
			klog.V(3).Infof("Pod %s is %.3f seconds old, too new to consider unschedulable", pod.Name, podAge.Seconds())

		}
	}
	return oldUnschedulablePods
}

// ExitCleanUp performs all necessary clean-ups when the autoscaler's exiting.
func (a *StaticAutoscaler) ExitCleanUp() {
	a.processors.CleanUp()
	a.DebuggingSnapshotter.Cleanup()

	if !a.AutoscalingContext.WriteStatusConfigMap {
		return
	}
	utils.DeleteStatusConfigMap(a.AutoscalingContext.ClientSet, a.AutoscalingContext.ConfigNamespace, a.AutoscalingContext.StatusConfigMapName)

	a.clusterStateRegistry.Stop()
}

func (a *StaticAutoscaler) obtainNodeLists() ([]*apiv1.Node, []*apiv1.Node, errors.AutoscalerError) {
	allNodes, err := a.AllNodeLister().List()
	if err != nil {
		klog.Errorf("Failed to list all nodes: %v", err)
		return nil, nil, errors.ToAutoscalerError(errors.ApiCallError, err)
	}
	readyNodes, err := a.ReadyNodeLister().List()
	if err != nil {
		klog.Errorf("Failed to list ready nodes: %v", err)
		return nil, nil, errors.ToAutoscalerError(errors.ApiCallError, err)
	}

	// Handle GPU case - allocatable GPU may be equal to 0 up to 15 minutes after
	// node registers as ready. See https://github.com/kubernetes/kubernetes/issues/54959
	// Treat those nodes as unready until GPU actually becomes available and let
	// our normal handling for booting up nodes deal with this.
	// TODO: Remove this call when we handle dynamically provisioned resources.

	//allNodes, readyNodes = a.processors.CustomResourcesProcessor.FilterOutNodesWithUnreadyResources(a.AutoscalingContext, allNodes, readyNodes)
	//allNodes, readyNodes = taints.FilterOutNodesWithIgnoredTaints(a.ignoredTaints, allNodes, readyNodes)
	return allNodes, readyNodes, nil
}

func (a *StaticAutoscaler) updateClusterState(allNodes []*apiv1.Node, currentTime time.Time) errors.AutoscalerError {
	//err := a.clusterStateRegistry.UpdateNodes(allNodes, nodeInfosForGroups, currentTime)
	//if err != nil {
	//	klog.Errorf("Failed to update node registry: %v", err)
	//	a.scaleDown.CleanUpUnneededNodes()
	//	return errors.ToAutoscalerError(errors.CloudProviderError, err)
	//}
	core_utils.UpdateClusterStateMetrics(a.clusterStateRegistry)

	return nil
}

func allPodsAreNew(pods []*apiv1.Pod, currentTime time.Time) bool {
	if core_utils.GetOldestCreateTime(pods).Add(unschedulablePodTimeBuffer).After(currentTime) {
		return true
	}
	//found, oldest := core_utils.GetOldestCreateTimeWithGpu(pods)
	//return found && oldest.Add(unschedulablePodWithGpuTimeBuffer).After(currentTime)
	return false
}

//func getUpcomingNodeInfos(registry *clusterstate.ClusterStateRegistry, nodeInfos map[string]*schedulerframework.NodeInfo) []*schedulerframework.NodeInfo {
//	upcomingNodes := make([]*schedulerframework.NodeInfo, 0)
//	for nodeGroup, numberOfNodes := range registry.GetUpcomingNodes() {
//		nodeTemplate, found := nodeInfos[nodeGroup]
//		if !found {
//			klog.Warningf("Couldn't find template for node group %s", nodeGroup)
//			continue
//		}
//
//		if nodeTemplate.Node().Annotations == nil {
//			nodeTemplate.Node().Annotations = make(map[string]string)
//		}
//		nodeTemplate.Node().Annotations[NodeUpcomingAnnotation] = "true"
//
//		for i := 0; i < numberOfNodes; i++ {
//			// Ensure new nodes have different names because nodeName
//			// will be used as a map key. Also deep copy pods (daemonsets &
//			// any pods added by cloud provider on template).
//			upcomingNodes = append(upcomingNodes, scheduler_utils.DeepCopyTemplateNode(nodeTemplate, fmt.Sprintf("upcoming-%d", i)))
//		}
//	}
//	return upcomingNodes
//}

func calculateCoresMemoryTotal(nodes []*apiv1.Node, timestamp time.Time) (int64, int64) {
	// this function is essentially similar to the calculateScaleDownCoresMemoryTotal
	// we want to check all nodes, aside from those deleting, to sum the cluster resource usage.
	var coresTotal, memoryTotal int64
	for _, node := range nodes {
		if isNodeBeingDeleted(node, timestamp) {
			// Nodes being deleted do not count towards total cluster resources
			continue
		}
		cores, memory := core_utils.GetNodeCoresAndMemory(node)

		coresTotal += cores
		memoryTotal += memory
	}

	return coresTotal, memoryTotal
}

func checkWorkerNodeCanBeRemove(kubeclient kube_client.Interface, workerNodeName string) bool {
	var canBeRemove bool = true
	pods, err := kubeclient.CoreV1().Pods("").List(ctx.Background(), metav1.ListOptions{})
	if err != nil {
		log.Fatal(err)
	}
	for _, pod := range pods.Items {
		if pod.Spec.NodeName == workerNodeName && pod.OwnerReferences[0].Kind != "DaemonSet" {
			replicaset, _ := kubeclient.AppsV1().ReplicaSets(pod.Namespace).Get(ctx.Background(),
				pod.OwnerReferences[0].Name, metav1.GetOptions{})
			//if err != nil {
			//	log.Fatal(err)
			//}
			if replicaset.Status.Replicas == 1 {
				klog.V(1).Infof("If you want to scale down, you should evict pod %s in namespace %s "+
					"because your replicaset %s has only one replica", pod.Name, pod.Namespace,
					replicaset.Name)
				canBeRemove = false
			}
			for _, volume := range pod.Spec.Volumes {
				if volume.EmptyDir != nil {
					klog.V(1).Infof("If you want to scale down, you should evict pod %s"+
						" in namespace %s because pod has local storage", pod.Name, pod.Namespace)
					canBeRemove = false
				}
			}
		}
	}
	return canBeRemove
}
