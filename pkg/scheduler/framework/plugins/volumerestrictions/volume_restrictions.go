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

package volumerestrictions

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// VolumeRestrictions is a plugin that checks volume restrictions.
type VolumeRestrictions struct {
	pvcLister    corelisters.PersistentVolumeClaimLister
	sharedLister framework.SharedLister
}

var _ framework.PreFilterPlugin = &VolumeRestrictions{}
var _ framework.FilterPlugin = &VolumeRestrictions{}
var _ framework.EnqueueExtensions = &VolumeRestrictions{}
var _ framework.StateData = &preFilterState{}

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = names.VolumeRestrictions
	// preFilterStateKey is the key in CycleState to VolumeRestrictions pre-computed data for Filtering.
	// Using the name of the plugin will likely help us avoid collisions with other plugins.
	preFilterStateKey = "PreFilter" + Name

	// ErrReasonDiskConflict is used for NoDiskConflict predicate error.
	ErrReasonDiskConflict = "node(s) had no available disk"
	// ErrReasonReadWriteOncePodConflict is used when a pod is found using the same PVC with the ReadWriteOncePod access mode.
	ErrReasonReadWriteOncePodConflict = "node has pod using PersistentVolumeClaim with the same name and ReadWriteOncePod access mode"
)

// preFilterState computed at PreFilter and used at Filter.
type preFilterState struct {
	// Names of the pod's volumes using the ReadWriteOncePod access mode.
	readWriteOncePodPVCs sets.Set[string]
	// The number of references to these ReadWriteOncePod volumes by scheduled pods.
	conflictingPVCRefCount int
}

func (s *preFilterState) updateWithPod(podInfo *framework.PodInfo, multiplier int) {
	s.conflictingPVCRefCount += multiplier * s.conflictingPVCRefCountForPod(podInfo)
}

func (s *preFilterState) conflictingPVCRefCountForPod(podInfo *framework.PodInfo) int {
	conflicts := 0
	for _, volume := range podInfo.Pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		if s.readWriteOncePodPVCs.Has(volume.PersistentVolumeClaim.ClaimName) {
			conflicts += 1
		}
	}
	return conflicts
}

// Clone the prefilter state.
func (s *preFilterState) Clone() framework.StateData {
	if s == nil {
		return nil
	}
	return &preFilterState{
		readWriteOncePodPVCs:   s.readWriteOncePodPVCs,
		conflictingPVCRefCount: s.conflictingPVCRefCount,
	}
}

// Name returns name of the plugin. It is used in logs, etc.
func (pl *VolumeRestrictions) Name() string {
	return Name
}

func isVolumeConflict(volume *v1.Volume, pod *v1.Pod) bool {
	for _, existingVolume := range pod.Spec.Volumes {
		// Same GCE disk mounted by multiple pods conflicts unless all pods mount it read-only.
		if volume.GCEPersistentDisk != nil && existingVolume.GCEPersistentDisk != nil {
			disk, existingDisk := volume.GCEPersistentDisk, existingVolume.GCEPersistentDisk
			if disk.PDName == existingDisk.PDName && !(disk.ReadOnly && existingDisk.ReadOnly) {
				return true
			}
		}

		if volume.AWSElasticBlockStore != nil && existingVolume.AWSElasticBlockStore != nil {
			if volume.AWSElasticBlockStore.VolumeID == existingVolume.AWSElasticBlockStore.VolumeID {
				return true
			}
		}

		if volume.ISCSI != nil && existingVolume.ISCSI != nil {
			iqn := volume.ISCSI.IQN
			eiqn := existingVolume.ISCSI.IQN
			// two ISCSI volumes are same, if they share the same iqn. As iscsi volumes are of type
			// RWO or ROX, we could permit only one RW mount. Same iscsi volume mounted by multiple Pods
			// conflict unless all other pods mount as read only.
			if iqn == eiqn && !(volume.ISCSI.ReadOnly && existingVolume.ISCSI.ReadOnly) {
				return true
			}
		}

		if volume.RBD != nil && existingVolume.RBD != nil {
			mon, pool, image := volume.RBD.CephMonitors, volume.RBD.RBDPool, volume.RBD.RBDImage
			emon, epool, eimage := existingVolume.RBD.CephMonitors, existingVolume.RBD.RBDPool, existingVolume.RBD.RBDImage
			// two RBDs images are the same if they share the same Ceph monitor, are in the same RADOS Pool, and have the same image name
			// only one read-write mount is permitted for the same RBD image.
			// same RBD image mounted by multiple Pods conflicts unless all Pods mount the image read-only
			if haveOverlap(mon, emon) && pool == epool && image == eimage && !(volume.RBD.ReadOnly && existingVolume.RBD.ReadOnly) {
				return true
			}
		}
	}

	return false
}

// haveOverlap searches two arrays and returns true if they have at least one common element; returns false otherwise.
func haveOverlap(a1, a2 []string) bool {
	if len(a1) > len(a2) {
		a1, a2 = a2, a1
	}
	m := sets.New(a1...)
	for _, val := range a2 {
		if _, ok := m[val]; ok {
			return true
		}
	}

	return false
}

// return true if there are conflict checking targets.
func needsRestrictionsCheck(v v1.Volume) bool {
	return v.GCEPersistentDisk != nil || v.AWSElasticBlockStore != nil || v.RBD != nil || v.ISCSI != nil
}

// PreFilter computes and stores cycleState containing details for enforcing ReadWriteOncePod.
func (pl *VolumeRestrictions) PreFilter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	needsCheck := false
	for i := range pod.Spec.Volumes {
		if needsRestrictionsCheck(pod.Spec.Volumes[i]) {
			needsCheck = true
			break
		}
	}

	pvcs, err := pl.readWriteOncePodPVCsForPod(pod, false)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
		}
		return nil, framework.AsStatus(err)
	}

	s, err := pl.calPreFilterState(ctx, pod, pvcs)
	if err != nil {
		return nil, framework.AsStatus(err)
	}

	if !needsCheck && s.conflictingPVCRefCount == 0 {
		return nil, framework.NewStatus(framework.Skip)
	}
	cycleState.Write(preFilterStateKey, s)
	return nil, nil
}

// AddPod from pre-computed data in cycleState.
func (pl *VolumeRestrictions) AddPod(ctx context.Context, cycleState *framework.CycleState, podToSchedule *v1.Pod, podInfoToAdd *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	state, err := getPreFilterState(cycleState)
	if err != nil {
		return framework.AsStatus(err)
	}
	state.updateWithPod(podInfoToAdd, 1)
	return nil
}

// RemovePod from pre-computed data in cycleState.
func (pl *VolumeRestrictions) RemovePod(ctx context.Context, cycleState *framework.CycleState, podToSchedule *v1.Pod, podInfoToRemove *framework.PodInfo, nodeInfo *framework.NodeInfo) *framework.Status {
	state, err := getPreFilterState(cycleState)
	if err != nil {
		return framework.AsStatus(err)
	}
	state.updateWithPod(podInfoToRemove, -1)
	return nil
}

func getPreFilterState(cycleState *framework.CycleState) (*preFilterState, error) {
	c, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// preFilterState doesn't exist, likely PreFilter wasn't invoked.
		return nil, fmt.Errorf("cannot read %q from cycleState", preFilterStateKey)
	}

	s, ok := c.(*preFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v convert to volumerestrictions.state error", c)
	}
	return s, nil
}

// calPreFilterState computes preFilterState describing which PVCs use ReadWriteOncePod
// and which pods in the cluster are in conflict.
func (pl *VolumeRestrictions) calPreFilterState(ctx context.Context, pod *v1.Pod, pvcs sets.Set[string]) (*preFilterState, error) {
	conflictingPVCRefCount := 0
	for pvc := range pvcs {
		key := framework.GetNamespacedName(pod.Namespace, pvc)
		if pl.sharedLister.StorageInfos().IsPVCUsedByPods(key) {
			// There can only be at most one pod using the ReadWriteOncePod PVC.
			conflictingPVCRefCount += 1
		}
	}
	return &preFilterState{
		readWriteOncePodPVCs:   pvcs,
		conflictingPVCRefCount: conflictingPVCRefCount,
	}, nil
}

// readWriteOncePodPVCsForPod returns the name of ReadWriteOncePod PVCs in a given Pod.
// If ignoreNotFoundError is true, it tries to check all PVCs, ignoring not found errors.
func (pl *VolumeRestrictions) readWriteOncePodPVCsForPod(pod *v1.Pod, ignoreNotFoundError bool) (sets.Set[string], error) {
	pvcs := sets.New[string]()
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		pvc, err := pl.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(volume.PersistentVolumeClaim.ClaimName)
		if err != nil {
			if ignoreNotFoundError && apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}

		if !v1helper.ContainsAccessMode(pvc.Spec.AccessModes, v1.ReadWriteOncePod) {
			continue
		}
		pvcs.Insert(pvc.Name)
	}
	return pvcs, nil
}

// Checks if scheduling the pod onto this node would cause any conflicts with
// existing volumes.
func satisfyVolumeConflicts(pod *v1.Pod, nodeInfo *framework.NodeInfo) bool {
	for i := range pod.Spec.Volumes {
		v := pod.Spec.Volumes[i]
		if !needsRestrictionsCheck(v) {
			continue
		}
		for _, ev := range nodeInfo.Pods {
			if isVolumeConflict(&v, ev.Pod) {
				return false
			}
		}
	}
	return true
}

// Checks if scheduling the pod would cause any ReadWriteOncePod PVC access mode conflicts.
func satisfyReadWriteOncePod(ctx context.Context, state *preFilterState) *framework.Status {
	if state == nil {
		return nil
	}
	if state.conflictingPVCRefCount > 0 {
		return framework.NewStatus(framework.Unschedulable, ErrReasonReadWriteOncePodConflict)
	}
	return nil
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (pl *VolumeRestrictions) PreFilterExtensions() framework.PreFilterExtensions {
	return pl
}

// Filter invoked at the filter extension point.
// It evaluates if a pod can fit due to the volumes it requests, and those that
// are already mounted. If there is already a volume mounted on that node, another pod that uses the same volume
// can't be scheduled there.
// This is GCE, Amazon EBS, ISCSI and Ceph RBD specific for now:
// - GCE PD allows multiple mounts as long as they're all read-only
// - AWS EBS forbids any two pods mounting the same volume ID
// - Ceph RBD forbids if any two pods share at least same monitor, and match pool and image, and the image is read-only
// - ISCSI forbids if any two pods share at least same IQN and ISCSI volume is read-only
// If the pod uses PVCs with the ReadWriteOncePod access mode, it evaluates if
// these PVCs are already in-use and if preemption will help.
func (pl *VolumeRestrictions) Filter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if !satisfyVolumeConflicts(pod, nodeInfo) {
		return framework.NewStatus(framework.Unschedulable, ErrReasonDiskConflict)
	}
	state, err := getPreFilterState(cycleState)
	if err != nil {
		return framework.AsStatus(err)
	}
	return satisfyReadWriteOncePod(ctx, state)
}

// EventsToRegister returns the possible events that may make a Pod
// failed by this plugin schedulable.
func (pl *VolumeRestrictions) EventsToRegister() []framework.ClusterEventWithHint {
	return []framework.ClusterEventWithHint{
		// Pods may fail to schedule because of volumes conflicting with other pods on same node.
		// Once running pods are deleted and volumes have been released, the unschedulable pod will be schedulable.
		// Due to immutable fields `spec.volumes`, pod update events are ignored.
		{Event: framework.ClusterEvent{Resource: framework.Pod, ActionType: framework.Delete}, QueueingHintFn: pl.isSchedulableAfterPodDeleted},
		// A new Node may make a pod schedulable.
		// We intentionally don't set QueueingHint since all Node/Add events could make Pods schedulable.
		{Event: framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Add}},
		// Pods may fail to schedule because the PVC it uses has not yet been created.
		// This PVC is required to exist to check its access modes.
		{Event: framework.ClusterEvent{Resource: framework.PersistentVolumeClaim, ActionType: framework.Add | framework.Update}, QueueingHintFn: pl.isSchedulableAfterPersistentVolumeClaimChange},
	}
}

// isSchedulableAfterPodDeleted is invoked whenever a pod deleted,
// It checks whether the deleted pod will conflict with volumes of other pods on the same node
func (pl *VolumeRestrictions) isSchedulableAfterPodDeleted(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	deletedPod, _, err := util.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return framework.Queue, fmt.Errorf("unexpected objects in isSchedulableAfterPodDeleted: %w", err)
	}

	if deletedPod.Namespace != pod.Namespace {
		return framework.QueueSkip, nil
	}

	newPodPvcs, err := pl.readWriteOncePodPVCsForPod(pod, false)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(5).Info("no PVC for the Pod is found, this Pod won't be schedulable until PVC is created", "pod", klog.KObj(pod), "err", err)
			return framework.QueueSkip, nil
		}
		return framework.Queue, err
	}

	// deletedPod may contain multiple PVCs and maybe one or more PVCs have been deleted (while some still exist).
	// We can always ignore a deleted PVC associated with this deleted Pod because:
	// - if a new Pod has this PVC, this Pod won't be schedulable until the PVC with the same name is recreated.
	// - if a new Pod doesn't have this PVC, this Pod is completely not related to that deleted PVC.
	//
	// But, a complex scenario is that when the deleted Pod has more than one PVCs, and PVC-1 is deleted, but PVC-x isn't deleted.
	// In this case, as the above describes, PVC1 can be ignored anyway.
	// But we still need to check PVC-x, whether the deletion of deletedPod could make the pod schedulable.
	deletedPodPvcs, err := pl.readWriteOncePodPVCsForPod(deletedPod, true)
	if err != nil {
		return framework.Queue, err
	}

	// If oldPod and the current pod are in conflict because of readWriteOncePodPVC,
	// the current pod may be scheduled in the next scheduling cycle, so we return Queue
	for pvc := range deletedPodPvcs {
		if newPodPvcs.Has(pvc) {
			return framework.Queue, nil
		}
	}

	nodeInfo := framework.NewNodeInfo(deletedPod)
	if !satisfyVolumeConflicts(pod, nodeInfo) {
		return framework.Queue, nil
	}

	return framework.QueueSkip, nil
}

// isSchedulableAfterPersistentVolumeClaimChange is invoked whenever a PersistentVolumeClaim added or changed, It checks whether
// that change made a previously unschedulable pod schedulable.
func (pl *VolumeRestrictions) isSchedulableAfterPersistentVolumeClaimChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	oldPersistentVolumeClaim, newPersistentVolumeClaim, err := util.As[*v1.PersistentVolumeClaim](oldObj, newObj)
	if err != nil {
		return framework.Queue, fmt.Errorf("unexpected objects in isSchedulableAfterPersistentVolumeClaimChange: %w", err)
	}

	if oldPersistentVolumeClaim != nil || newPersistentVolumeClaim.Namespace != pod.Namespace {
		return framework.QueueSkip, nil
	}

	pvcs := sets.New[string]()

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		pvc, err := pl.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(volume.PersistentVolumeClaim.ClaimName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				logger.V(5).Info("The PVC for the Pod is not created, and this Pod won't be schedulable until the PVC is created",
					"pod", klog.KObj(pod), "pvc", volume.PersistentVolumeClaim.ClaimName, "err", err)
				return framework.QueueSkip, nil
			}
			return framework.Queue, err
		}

		pvcs.Insert(pvc.Name)
	}

	// We're only interested in PVC which the Pod requests.
	if oldPersistentVolumeClaim == nil && pvcs.Has(newPersistentVolumeClaim.Name) {
		return framework.Queue, nil
	}
	return framework.QueueSkip, nil
}

// New initializes a new plugin and returns it.
func New(_ context.Context, _ runtime.Object, handle framework.Handle, fts feature.Features) (framework.Plugin, error) {
	informerFactory := handle.SharedInformerFactory()
	pvcLister := informerFactory.Core().V1().PersistentVolumeClaims().Lister()
	sharedLister := handle.SnapshotSharedLister()

	return &VolumeRestrictions{
		pvcLister:    pvcLister,
		sharedLister: sharedLister,
	}, nil
}
